using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Moongazing.OrionPatch.AzureServiceBus;
using Moongazing.OrionPatch.Models;
using Moq;
using Xunit;

namespace Moongazing.OrionPatch.AzureServiceBus.Tests;

public sealed class AzureServiceBusOutboxSinkTests
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

    private sealed class CapturingSenderFactory : IServiceBusSenderFactory
    {
        public List<string> RequestedPaths { get; } = new();
        public List<ServiceBusMessage> Sent { get; } = new();
        public Mock<ServiceBusSender>? SenderMock { get; private set; }

        public ServiceBusSender CreateSender(string entityPath)
        {
            RequestedPaths.Add(entityPath);
            var mock = new Mock<ServiceBusSender>();
            mock.Setup(s => s.SendMessageAsync(It.IsAny<ServiceBusMessage>(), It.IsAny<CancellationToken>()))
                .Returns<ServiceBusMessage, CancellationToken>((m, _) =>
                {
                    Sent.Add(m);
                    return Task.CompletedTask;
                });
            SenderMock = mock;
            return mock.Object;
        }
    }

    private static IOptions<AzureServiceBusOutboxSinkOptions> Opts(Action<AzureServiceBusOutboxSinkOptions>? configure = null)
    {
        var o = new AzureServiceBusOutboxSinkOptions();
        configure?.Invoke(o);
        return Options.Create(o);
    }

    [Fact]
    public async Task SendAsync_publishes_to_configured_entity_with_envelope_message_id()
    {
        var factory = new CapturingSenderFactory();
        var sut = new AzureServiceBusOutboxSink(factory, Opts(o => o.EntityPath = "orders"));
        var envelopeId = Guid.NewGuid();

        await sut.SendAsync(Env(id: envelopeId, messageType: "OrderPlaced"));

        Assert.Single(factory.RequestedPaths);
        Assert.Equal("orders", factory.RequestedPaths[0]);
        Assert.Single(factory.Sent);
        Assert.Equal(envelopeId.ToString("N"), factory.Sent[0].MessageId);
        Assert.Equal("OrderPlaced", factory.Sent[0].Subject);
    }

    [Fact]
    public async Task SendAsync_stamps_application_properties_for_envelope_id_and_message_type()
    {
        var factory = new CapturingSenderFactory();
        var sut = new AzureServiceBusOutboxSink(factory, Opts());
        var envelopeId = Guid.NewGuid();

        await sut.SendAsync(Env(id: envelopeId, messageType: "Foo"));

        var props = factory.Sent[0].ApplicationProperties;
        Assert.Equal(envelopeId.ToString("N"), props["orionpatch-envelope-id"]);
        Assert.Equal("Foo", props["orionpatch-message-type"]);
    }

    [Fact]
    public async Task SendAsync_propagates_caller_supplied_headers_but_reserved_keys_win()
    {
        var factory = new CapturingSenderFactory();
        var sut = new AzureServiceBusOutboxSink(factory, Opts());
        var envelopeId = Guid.NewGuid();
        var headers = new Dictionary<string, string>
        {
            ["traceparent"] = "00-aaa-bbb-01",
            ["tenant"] = "acme",
            ["orionpatch-envelope-id"] = "hijack-attempt",
        };

        await sut.SendAsync(Env(id: envelopeId, headers: headers));

        var props = factory.Sent[0].ApplicationProperties;
        Assert.Equal("00-aaa-bbb-01", props["traceparent"]);
        Assert.Equal("acme", props["tenant"]);
        Assert.Equal(envelopeId.ToString("N"), props["orionpatch-envelope-id"]);
    }

    [Fact]
    public async Task SendAsync_stamps_correlation_id_when_present()
    {
        var factory = new CapturingSenderFactory();
        var sut = new AzureServiceBusOutboxSink(factory, Opts());

        await sut.SendAsync(Env(correlationId: "corr-42"));

        Assert.Equal("corr-42", factory.Sent[0].CorrelationId);
        Assert.Equal("corr-42", factory.Sent[0].ApplicationProperties["orionpatch-correlation-id"]);
    }

    [Fact]
    public async Task SendAsync_uses_SubjectSelector_for_message_subject()
    {
        var factory = new CapturingSenderFactory();
        var sut = new AzureServiceBusOutboxSink(factory,
            Opts(o => o.SubjectSelector = e => $"v1.{e.MessageType}"));

        await sut.SendAsync(Env(messageType: "Order.Created"));

        Assert.Equal("v1.Order.Created", factory.Sent[0].Subject);
    }

    [Fact]
    public async Task SendAsync_sets_content_type_from_options()
    {
        var factory = new CapturingSenderFactory();
        var sut = new AzureServiceBusOutboxSink(factory,
            Opts(o => o.ContentType = "application/cloudevents+json"));

        await sut.SendAsync(Env());

        Assert.Equal("application/cloudevents+json", factory.Sent[0].ContentType);
    }

    [Fact]
    public async Task SendAsync_payload_round_trips_through_message_body()
    {
        var factory = new CapturingSenderFactory();
        var sut = new AzureServiceBusOutboxSink(factory, Opts());

        await sut.SendAsync(Env(payload: "{\"k\":\"v\"}"));

        var body = System.Text.Encoding.UTF8.GetString(factory.Sent[0].Body.ToArray());
        Assert.Equal("{\"k\":\"v\"}", body);
    }

    [Fact]
    public async Task SendAsync_send_timeout_cancels_inflight_send_when_sdk_hangs()
    {
        // Sender mock returns a task that never completes; the sink's CancelAfter must
        // surface as TaskCanceledException / OperationCanceledException.
        var factory = new HangingSenderFactory();
        var sut = new AzureServiceBusOutboxSink(factory,
            Opts(o => o.SendTimeout = TimeSpan.FromMilliseconds(50)));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => sut.SendAsync(Env()));
    }

    private sealed class HangingSenderFactory : IServiceBusSenderFactory
    {
        public ServiceBusSender CreateSender(string entityPath)
        {
            var mock = new Mock<ServiceBusSender>();
            mock.Setup(s => s.SendMessageAsync(It.IsAny<ServiceBusMessage>(), It.IsAny<CancellationToken>()))
                .Returns<ServiceBusMessage, CancellationToken>(async (_, ct) =>
                {
                    await Task.Delay(Timeout.Infinite, ct);
                });
            return mock.Object;
        }
    }

    [Fact]
    public async Task AddOrionPatchAzureServiceBusSink_registers_sink_as_singleton_IOutboxSink()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new ServiceBusClient("Endpoint=sb://test.example.com/;SharedAccessKeyName=k;SharedAccessKey=Zm9v"));
        services.AddOrionPatchAzureServiceBusSink(o => o.EntityPath = "test-q");

        // ServiceBusClient implements IAsyncDisposable but NOT IDisposable; the synchronous
        // 'using' block would throw at teardown. Build the provider as an async-disposable
        // and dispose it asynchronously.
        await using var sp = services.BuildServiceProvider();
        var sink = sp.GetRequiredService<Moongazing.OrionPatch.Abstractions.IOutboxSink>();

        Assert.IsType<AzureServiceBusOutboxSink>(sink);
    }

    [Fact]
    public void AddOrionPatchAzureServiceBusSink_invokes_configure_delegate_exactly_once()
    {
        var invocations = 0;
        var services = new ServiceCollection();
        services.AddSingleton(new ServiceBusClient("Endpoint=sb://test.example.com/;SharedAccessKeyName=k;SharedAccessKey=Zm9v"));

        services.AddOrionPatchAzureServiceBusSink(o =>
        {
            invocations++;
            o.EntityPath = $"q-{invocations}";
        });

        Assert.Equal(1, invocations);
    }

    [Fact]
    public void DefaultServiceBusSenderFactory_caches_senders_per_entity_path()
    {
        var client = new ServiceBusClient("Endpoint=sb://test.example.com/;SharedAccessKeyName=k;SharedAccessKey=Zm9v");
        var factory = new DefaultServiceBusSenderFactory(client);

        var s1 = factory.CreateSender("q");
        var s2 = factory.CreateSender("q");
        var s3 = factory.CreateSender("other");

        Assert.Same(s1, s2);
        Assert.NotSame(s1, s3);
    }
}
