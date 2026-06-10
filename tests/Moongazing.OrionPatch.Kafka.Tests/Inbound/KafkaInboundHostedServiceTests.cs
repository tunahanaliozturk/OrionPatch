using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Kafka.Inbound;
using Moq;
using Xunit;

namespace Moongazing.OrionPatch.Kafka.Tests.Inbound;

public sealed class KafkaInboundHostedServiceTests
{
    private sealed class StubInbox : IInbox
    {
        public HashSet<Guid> Accepted { get; } = new();
        public HashSet<Guid> RolledBack { get; } = new();
        public ValueTask<bool> TryAcceptAsync(Guid id, CancellationToken ct)
            => ValueTask.FromResult(Accepted.Add(id));
        public ValueTask RollbackAsync(Guid id, CancellationToken ct)
        {
            Accepted.Remove(id);
            RolledBack.Add(id);
            return default;
        }
    }

    private sealed class RecordingHandler : IKafkaInboundHandler
    {
        public List<InboundKafkaMessage> Received { get; } = new();
        public Func<InboundKafkaMessage, Task>? Behaviour { get; set; }
        public async Task HandleAsync(InboundKafkaMessage message, CancellationToken ct)
        {
            Received.Add(message);
            if (Behaviour is not null)
            {
                await Behaviour(message);
            }
        }
    }

    private sealed class ScriptedConsumerFactory : IKafkaConsumerFactory
    {
        private readonly Queue<ConsumeResult<string, byte[]>?> script;
        public Mock<IConsumer<string, byte[]>> Consumer { get; }
        public List<ConsumeResult<string, byte[]>> Committed { get; } = new();

        public ScriptedConsumerFactory(IEnumerable<ConsumeResult<string, byte[]>?> script)
        {
            this.script = new Queue<ConsumeResult<string, byte[]>?>(script);
            Consumer = new Mock<IConsumer<string, byte[]>>();
            Consumer.Setup(c => c.Consume(It.IsAny<TimeSpan>()))
                .Returns(() => this.script.Count > 0 ? this.script.Dequeue() : null);
            Consumer.Setup(c => c.Commit(It.IsAny<ConsumeResult<string, byte[]>>()))
                .Callback<ConsumeResult<string, byte[]>>(r => Committed.Add(r));
        }

        public IConsumer<string, byte[]> CreateConsumer() => Consumer.Object;
    }

    private static ConsumeResult<string, byte[]> Record(Guid envelopeId, string messageType = "Demo.Event",
        long offset = 0, byte[]? body = null)
    {
        var headers = new Headers
        {
            { "orionpatch-envelope-id", Encoding.UTF8.GetBytes(envelopeId.ToString("N")) },
            { "orionpatch-message-type", Encoding.UTF8.GetBytes(messageType) },
        };
        return new ConsumeResult<string, byte[]>
        {
            Topic = "orders",
            Partition = new Partition(0),
            Offset = new Offset(offset),
            Message = new Message<string, byte[]>
            {
                Key = envelopeId.ToString("N"),
                Value = body ?? Encoding.UTF8.GetBytes("{\"a\":1}"),
                Headers = headers,
            },
        };
    }

    private static (KafkaInboundHostedService svc, ScriptedConsumerFactory factory,
                    StubInbox inbox, RecordingHandler handler, ServiceProvider sp)
        BuildSut(params ConsumeResult<string, byte[]>?[] script)
    {
        var factory = new ScriptedConsumerFactory(script);
        var inbox = new StubInbox();
        var handler = new RecordingHandler();

        var collection = new ServiceCollection();
        collection.AddSingleton<IInbox>(inbox);
        collection.AddSingleton<IKafkaInboundHandler>(handler);
        var sp = collection.BuildServiceProvider();

        var options = Options.Create(new KafkaInboxOptions
        {
            BootstrapServers = "x",
            GroupId = "g",
            Topics = new[] { "orders" },
            PollTimeout = TimeSpan.FromMilliseconds(50),
        });
        var svc = new KafkaInboundHostedService(
            factory, options, sp.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<KafkaInboundHostedService>.Instance);
        return (svc, factory, inbox, handler, sp);
    }

    private static async Task RunUntilDrainedAsync(
        KafkaInboundHostedService svc,
        ScriptedConsumerFactory factory,
        Func<bool>? until = null)
    {
        // Default exit condition: at least one commit has fired. Pass a custom predicate
        // for tests that expect the no-commit (handler-failure) path.
        until ??= () => factory.Committed.Count >= 1;
        await svc.StartAsync(CancellationToken.None);
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline && !until())
        {
            await Task.Delay(20);
        }
        await svc.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Dispatches_accepted_message_to_handler_and_commits()
    {
        var envelopeId = Guid.NewGuid();
        var (svc, factory, inbox, handler, sp) = BuildSut(Record(envelopeId));
        try
        {
            await RunUntilDrainedAsync(svc, factory);

            Assert.Contains(envelopeId, inbox.Accepted);
            Assert.Single(handler.Received, m => m.EnvelopeId == envelopeId);
            Assert.Single(factory.Committed);
        }
        finally
        {
            await sp.DisposeAsync();
        }
    }

    [Fact]
    public async Task Skips_duplicate_envelope_id_but_commits()
    {
        var envelopeId = Guid.NewGuid();
        var (svc, factory, inbox, handler, sp) = BuildSut(Record(envelopeId, offset: 1));
        inbox.Accepted.Add(envelopeId); // pre-seeded duplicate
        try
        {
            await RunUntilDrainedAsync(svc, factory);

            Assert.Empty(handler.Received);
            Assert.Single(factory.Committed);
        }
        finally
        {
            await sp.DisposeAsync();
        }
    }

    [Fact]
    public async Task Rolls_back_inbox_and_does_not_commit_on_handler_failure()
    {
        var envelopeId = Guid.NewGuid();
        var (svc, factory, inbox, handler, sp) = BuildSut(Record(envelopeId, offset: 2));
        handler.Behaviour = _ => throw new InvalidOperationException("handler boom");
        try
        {
            // Wait until the rollback has been observed - the failure path does NOT
            // commit, so commit-count would never increase.
            await RunUntilDrainedAsync(svc, factory, until: () => inbox.RolledBack.Contains(envelopeId));

            Assert.Contains(envelopeId, inbox.RolledBack);
            Assert.DoesNotContain(envelopeId, inbox.Accepted);
            Assert.Empty(factory.Committed);
        }
        finally
        {
            await sp.DisposeAsync();
        }
    }

    [Fact]
    public async Task Drops_and_commits_record_without_orionpatch_envelope_id_header()
    {
        var bareRecord = new ConsumeResult<string, byte[]>
        {
            Topic = "orders",
            Partition = new Partition(0),
            Offset = new Offset(7),
            Message = new Message<string, byte[]>
            {
                Key = "no-headers",
                Value = Array.Empty<byte>(),
                Headers = new Headers(),
            },
        };
        var (svc, factory, inbox, handler, sp) = BuildSut(bareRecord);
        try
        {
            await RunUntilDrainedAsync(svc, factory);

            Assert.Empty(inbox.Accepted);
            Assert.Empty(handler.Received);
            Assert.Single(factory.Committed);
        }
        finally
        {
            await sp.DisposeAsync();
        }
    }

    [Fact]
    public void DefaultKafkaConsumerFactory_rejects_empty_BootstrapServers()
    {
        var opts = Options.Create(new KafkaInboxOptions { BootstrapServers = string.Empty, GroupId = "g" });
        Assert.Throws<InvalidOperationException>(() => new DefaultKafkaConsumerFactory(opts));
    }

    [Fact]
    public void DefaultKafkaConsumerFactory_rejects_empty_GroupId()
    {
        var opts = Options.Create(new KafkaInboxOptions { BootstrapServers = "x", GroupId = string.Empty });
        Assert.Throws<InvalidOperationException>(() => new DefaultKafkaConsumerFactory(opts));
    }

    [Fact]
    public void AddOrionPatchKafkaInbox_registers_hosted_service_and_handler()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IInbox, StubInbox>();
        services.AddOrionPatchKafkaInbox<RecordingHandler>(o =>
        {
            o.BootstrapServers = "x";
            o.GroupId = "g";
            o.Topics = new[] { "orders" };
        });

        using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<Microsoft.Extensions.Hosting.IHostedService>()
            .OfType<KafkaInboundHostedService>()
            .Single();
        Assert.NotNull(hosted);
        using var scope = sp.CreateScope();
        var handler = scope.ServiceProvider.GetRequiredService<IKafkaInboundHandler>();
        Assert.IsType<RecordingHandler>(handler);
    }
}
