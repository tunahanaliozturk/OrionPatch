using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Moongazing.OrionPatch.Kafka;
using Moongazing.OrionPatch.Models;
using Moq;
using Xunit;

namespace Moongazing.OrionPatch.Kafka.Tests;

public sealed class KafkaOutboxSinkTests
{
    private static OutboxEnvelope Env(
        Guid? id = null,
        string messageType = "Demo.Event",
        string payload = "{\"a\":1}",
        string? correlationId = null,
        IReadOnlyDictionary<string, string>? headers = null) =>
        new(
            Id: id ?? Guid.NewGuid(),
            MessageType: messageType,
            Payload: payload,
            Headers: headers,
            CorrelationId: correlationId,
            OccurredAtUtc: new DateTime(2026, 6, 10, 12, 0, 0, DateTimeKind.Utc),
            AttemptNumber: 1);

    private sealed class CapturingProducerFactory : IKafkaProducerFactory
    {
        public Mock<IProducer<string, byte[]>> Producer { get; }
        public List<(string Topic, Message<string, byte[]> Message)> Sent { get; } = new();

        public CapturingProducerFactory()
        {
            Producer = new Mock<IProducer<string, byte[]>>();
            Producer.Setup(p => p.ProduceAsync(
                    It.IsAny<string>(),
                    It.IsAny<Message<string, byte[]>>(),
                    It.IsAny<CancellationToken>()))
                .Returns<string, Message<string, byte[]>, CancellationToken>((topic, msg, _) =>
                {
                    Sent.Add((topic, msg));
                    var result = new DeliveryResult<string, byte[]>
                    {
                        Topic = topic,
                        Partition = new Partition(0),
                        Offset = new Offset(Sent.Count - 1),
                        Message = msg,
                    };
                    return Task.FromResult(result);
                });
        }

        public IProducer<string, byte[]> GetProducer() => Producer.Object;
    }

    private static IOptions<KafkaOutboxSinkOptions> Opts(Action<KafkaOutboxSinkOptions>? configure = null)
    {
        var o = new KafkaOutboxSinkOptions();
        configure?.Invoke(o);
        return Options.Create(o);
    }

    [Fact]
    public async Task SendAsync_produces_to_configured_topic_with_envelope_id_as_key()
    {
        var factory = new CapturingProducerFactory();
        var sut = new KafkaOutboxSink(factory, Opts(o => o.Topic = "orders"));
        var envelopeId = Guid.NewGuid();

        await sut.SendAsync(Env(id: envelopeId));

        Assert.Single(factory.Sent);
        Assert.Equal("orders", factory.Sent[0].Topic);
        Assert.Equal(envelopeId.ToString("N"), factory.Sent[0].Message.Key);
    }

    [Fact]
    public async Task SendAsync_uses_TopicSelector_when_supplied()
    {
        var factory = new CapturingProducerFactory();
        var sut = new KafkaOutboxSink(factory,
            Opts(o => o.TopicSelector = e => $"events.{e.MessageType}"));

        await sut.SendAsync(Env(messageType: "OrderPlaced"));

        Assert.Equal("events.OrderPlaced", factory.Sent[0].Topic);
    }

    [Fact]
    public async Task SendAsync_uses_KeySelector_for_partition_routing()
    {
        var factory = new CapturingProducerFactory();
        var sut = new KafkaOutboxSink(factory,
            Opts(o => o.KeySelector = e => "tenant-acme"));

        await sut.SendAsync(Env());

        Assert.Equal("tenant-acme", factory.Sent[0].Message.Key);
    }

    [Fact]
    public async Task SendAsync_stamps_orionpatch_envelope_id_header()
    {
        var factory = new CapturingProducerFactory();
        var sut = new KafkaOutboxSink(factory, Opts());
        var envelopeId = Guid.NewGuid();

        await sut.SendAsync(Env(id: envelopeId));

        var headers = factory.Sent[0].Message.Headers;
        var raw = headers.GetLastBytes("orionpatch-envelope-id");
        Assert.Equal(envelopeId.ToString("N"), Encoding.UTF8.GetString(raw));
    }

    [Fact]
    public async Task SendAsync_stamps_orionpatch_message_type_header()
    {
        var factory = new CapturingProducerFactory();
        var sut = new KafkaOutboxSink(factory, Opts());

        await sut.SendAsync(Env(messageType: "Foo"));

        var raw = factory.Sent[0].Message.Headers.GetLastBytes("orionpatch-message-type");
        Assert.Equal("Foo", Encoding.UTF8.GetString(raw));
    }

    [Fact]
    public async Task SendAsync_stamps_correlation_id_header_when_present()
    {
        var factory = new CapturingProducerFactory();
        var sut = new KafkaOutboxSink(factory, Opts());

        await sut.SendAsync(Env(correlationId: "corr-42"));

        var raw = factory.Sent[0].Message.Headers.GetLastBytes("orionpatch-correlation-id");
        Assert.Equal("corr-42", Encoding.UTF8.GetString(raw));
    }

    [Fact]
    public async Task SendAsync_omits_correlation_id_header_when_absent()
    {
        var factory = new CapturingProducerFactory();
        var sut = new KafkaOutboxSink(factory, Opts());

        await sut.SendAsync(Env(correlationId: null));

        Assert.DoesNotContain(factory.Sent[0].Message.Headers, h => h.Key == "orionpatch-correlation-id");
    }

    [Fact]
    public async Task SendAsync_propagates_caller_supplied_headers_but_reserved_keys_win()
    {
        var factory = new CapturingProducerFactory();
        var sut = new KafkaOutboxSink(factory, Opts());
        var envelopeId = Guid.NewGuid();
        var headers = new Dictionary<string, string>
        {
            ["traceparent"] = "00-aaa-bbb-01",
            ["tenant"] = "acme",
            ["orionpatch-envelope-id"] = "hijack-attempt",
        };

        await sut.SendAsync(Env(id: envelopeId, headers: headers));

        var msgHeaders = factory.Sent[0].Message.Headers;
        Assert.Equal("00-aaa-bbb-01", Encoding.UTF8.GetString(msgHeaders.GetLastBytes("traceparent")));
        Assert.Equal("acme", Encoding.UTF8.GetString(msgHeaders.GetLastBytes("tenant")));
        Assert.Equal(envelopeId.ToString("N"), Encoding.UTF8.GetString(msgHeaders.GetLastBytes("orionpatch-envelope-id")));
    }

    [Fact]
    public async Task SendAsync_serialises_payload_as_UTF8_bytes()
    {
        var factory = new CapturingProducerFactory();
        var sut = new KafkaOutboxSink(factory, Opts());

        await sut.SendAsync(Env(payload: "{\"k\":\"v\"}"));

        var body = Encoding.UTF8.GetString(factory.Sent[0].Message.Value);
        Assert.Equal("{\"k\":\"v\"}", body);
    }

    [Fact]
    public void AddOrionPatchKafkaSink_invokes_configure_delegate_exactly_once()
    {
        var invocations = 0;
        var services = new ServiceCollection();
        services.AddOrionPatchKafkaSink(o =>
        {
            invocations++;
            o.BootstrapServers = $"broker-{invocations}";
        });

        Assert.Equal(1, invocations);
    }

    [Fact]
    public void AddOrionPatchKafkaSink_registers_sink_as_singleton_IOutboxSink()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IKafkaProducerFactory>(_ => new CapturingProducerFactory());
        services.AddOrionPatchKafkaSink(o => o.Topic = "test");

        using var sp = services.BuildServiceProvider();
        var sink = sp.GetRequiredService<Moongazing.OrionPatch.Abstractions.IOutboxSink>();

        Assert.IsType<KafkaOutboxSink>(sink);
    }
}
