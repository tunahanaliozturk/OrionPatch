using Microsoft.Extensions.Options;
using Moongazing.OrionPatch.Models;
using Moongazing.OrionPatch.RabbitMQ;
using Moq;
using RabbitMQ.Client;
using Xunit;

namespace Moongazing.OrionPatch.RabbitMQ.Tests;

public sealed class RabbitMqOutboxSinkTests
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
            OccurredAtUtc: new DateTime(2026, 6, 9, 12, 0, 0, DateTimeKind.Utc),
            AttemptNumber: 1);

    private static (Mock<IConnection> conn, Mock<IModel> model, Mock<IBasicProperties> props) NewMocks(
        bool confirmsAck = true)
    {
        var props = new Mock<IBasicProperties>();
        props.SetupAllProperties();

        var model = new Mock<IModel>();
        model.Setup(m => m.IsOpen).Returns(true);
        model.Setup(m => m.CreateBasicProperties()).Returns(props.Object);
        model.Setup(m => m.WaitForConfirms(It.IsAny<TimeSpan>())).Returns(confirmsAck);

        var conn = new Mock<IConnection>();
        conn.Setup(c => c.CreateModel()).Returns(model.Object);
        return (conn, model, props);
    }

    private static IOptions<RabbitMqOutboxSinkOptions> Opts(Action<RabbitMqOutboxSinkOptions>? configure = null)
    {
        var o = new RabbitMqOutboxSinkOptions();
        configure?.Invoke(o);
        return Options.Create(o);
    }

    [Fact]
    public async Task SendAsync_publishes_to_configured_exchange_and_routing_key()
    {
        var (conn, model, _) = NewMocks();
        using var sut = new RabbitMqOutboxSink(conn.Object, Opts(o =>
        {
            o.ExchangeName = "demo";
            o.RoutingKeySelector = e => $"r.{e.MessageType}";
        }));

        await sut.SendAsync(Env(messageType: "Order.Created"), CancellationToken.None);

        model.Verify(m => m.BasicPublish(
            "demo",
            "r.Order.Created",
            true, // mandatory:true forces a BasicReturn for unroutable messages
            It.IsAny<IBasicProperties>(),
            It.IsAny<ReadOnlyMemory<byte>>()),
            Times.Once);
    }

    [Fact]
    public async Task SendAsync_serialises_concurrent_calls_on_the_shared_channel()
    {
        // RabbitMQ.Client requires single-publisher per channel. Concurrent calls to
        // SendAsync against the singleton sink must NOT enter PublishCore in parallel.
        var props = new Mock<IBasicProperties>();
        props.SetupAllProperties();

        var inFlight = 0;
        var maxObserved = 0;
        var gate = new object();

        var model = new Mock<IModel>();
        model.Setup(m => m.IsOpen).Returns(true);
        model.Setup(m => m.CreateBasicProperties()).Returns(props.Object);
        model.Setup(m => m.WaitForConfirms(It.IsAny<TimeSpan>())).Returns(true);
        model.Setup(m => m.BasicPublish(
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<bool>(),
            It.IsAny<IBasicProperties>(),
            It.IsAny<ReadOnlyMemory<byte>>()))
            .Callback(() =>
            {
                lock (gate)
                {
                    inFlight++;
                    if (inFlight > maxObserved) { maxObserved = inFlight; }
                }
                System.Threading.Thread.Sleep(20);
                lock (gate) { inFlight--; }
            });

        var conn = new Mock<IConnection>();
        conn.Setup(c => c.CreateModel()).Returns(model.Object);
        using var sut = new RabbitMqOutboxSink(conn.Object, Opts());

        var tasks = Enumerable.Range(0, 8)
            .Select(_ => sut.SendAsync(Env(), CancellationToken.None))
            .ToArray();
        await Task.WhenAll(tasks);

        Assert.Equal(1, maxObserved);
    }

    [Fact]
    public async Task SendAsync_throws_when_BasicReturn_fires_for_in_flight_envelope()
    {
        // mandatory:true means a misconfigured exchange/routing key triggers BasicReturn
        // BEFORE WaitForConfirms returns. The sink MUST throw so the outbox row stays
        // unprocessed; without this, a topology bug would silently drop messages while the
        // dispatcher marks them processed.
        var props = new Mock<IBasicProperties>();
        props.SetupAllProperties();

        EventHandler<global::RabbitMQ.Client.Events.BasicReturnEventArgs>? capturedHandler = null;

        var model = new Mock<IModel>();
        model.Setup(m => m.IsOpen).Returns(true);
        model.Setup(m => m.CreateBasicProperties()).Returns(props.Object);
        model.Setup(m => m.WaitForConfirms(It.IsAny<TimeSpan>())).Returns(true);
        model.SetupAdd(m => m.BasicReturn += It.IsAny<EventHandler<global::RabbitMQ.Client.Events.BasicReturnEventArgs>>())
             .Callback<EventHandler<global::RabbitMQ.Client.Events.BasicReturnEventArgs>>(h => capturedHandler = h);

        var envelope = Env();
        var returnProps = new Mock<IBasicProperties>();
        returnProps.Setup(p => p.MessageId).Returns(envelope.Id.ToString("N"));

        model.Setup(m => m.BasicPublish(
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(),
                It.IsAny<IBasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>()))
            .Callback(() =>
            {
                // Simulate broker BasicReturn during publish (before WaitForConfirms).
                capturedHandler?.Invoke(model.Object,
                    new global::RabbitMQ.Client.Events.BasicReturnEventArgs
                    {
                        BasicProperties = returnProps.Object,
                        ReplyCode = 312,
                        ReplyText = "NO_ROUTE",
                    });
            });

        var conn = new Mock<IConnection>();
        conn.Setup(c => c.CreateModel()).Returns(model.Object);
        using var sut = new RabbitMqOutboxSink(conn.Object, Opts());

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => sut.SendAsync(envelope, CancellationToken.None));
        Assert.Contains("unroutable", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("NO_ROUTE", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SendAsync_stamps_envelope_id_and_message_type_headers()
    {
        var (conn, model, props) = NewMocks();
        var envelopeId = Guid.NewGuid();

        using var sut = new RabbitMqOutboxSink(conn.Object, Opts());
        await sut.SendAsync(Env(id: envelopeId, messageType: "Foo"), CancellationToken.None);

        var lastHeaders = props.Object.Headers!;
        Assert.Equal(envelopeId.ToString("N"), lastHeaders["orionpatch-envelope-id"]);
        Assert.Equal("Foo", lastHeaders["orionpatch-message-type"]);
    }

    [Fact]
    public async Task SendAsync_propagates_caller_supplied_headers_but_orionpatch_keys_win()
    {
        var (conn, _, props) = NewMocks();
        var envelopeId = Guid.NewGuid();
        var headers = new Dictionary<string, string>
        {
            ["traceparent"] = "00-aaa-bbb-01",
            ["tenant"] = "acme",
            ["orionpatch-envelope-id"] = "hijack-attempt", // MUST be ignored
        };

        using var sut = new RabbitMqOutboxSink(conn.Object, Opts());
        await sut.SendAsync(Env(id: envelopeId, headers: headers), CancellationToken.None);

        var actual = props.Object.Headers!;
        Assert.Equal("00-aaa-bbb-01", actual["traceparent"]);
        Assert.Equal("acme", actual["tenant"]);
        Assert.Equal(envelopeId.ToString("N"), actual["orionpatch-envelope-id"]);
    }

    [Fact]
    public async Task SendAsync_sets_persistent_delivery_mode_by_default()
    {
        var (conn, _, props) = NewMocks();
        using var sut = new RabbitMqOutboxSink(conn.Object, Opts());

        await sut.SendAsync(Env(), CancellationToken.None);

        Assert.Equal((byte)2, props.Object.DeliveryMode);
    }

    [Fact]
    public async Task SendAsync_skips_publisher_confirms_when_disabled()
    {
        var (conn, model, _) = NewMocks();
        using var sut = new RabbitMqOutboxSink(conn.Object, Opts(o => o.UsePublisherConfirms = false));

        await sut.SendAsync(Env(), CancellationToken.None);

        model.Verify(m => m.WaitForConfirms(It.IsAny<TimeSpan>()), Times.Never);
        model.Verify(m => m.ConfirmSelect(), Times.Never);
    }

    [Fact]
    public async Task SendAsync_throws_when_publisher_confirms_timeout()
    {
        var (conn, _, _) = NewMocks(confirmsAck: false);
        using var sut = new RabbitMqOutboxSink(conn.Object, Opts());

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => sut.SendAsync(Env(), CancellationToken.None));
        Assert.Contains("did not acknowledge", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Dispose_disposes_the_channel_and_blocks_further_sends()
    {
        var (conn, model, _) = NewMocks();
        var sut = new RabbitMqOutboxSink(conn.Object, Opts());

        await sut.SendAsync(Env(), CancellationToken.None);
        sut.Dispose();

        model.Verify(m => m.Dispose(), Times.Once);
        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => sut.SendAsync(Env(), CancellationToken.None));
    }

    [Fact]
    public async Task SendAsync_reopens_channel_when_previous_channel_closes()
    {
        // Simulate: first send opens model A; A.IsOpen becomes false; next send opens model B.
        var props = new Mock<IBasicProperties>();
        props.SetupAllProperties();

        var modelA = new Mock<IModel>();
        modelA.Setup(m => m.CreateBasicProperties()).Returns(props.Object);
        modelA.Setup(m => m.WaitForConfirms(It.IsAny<TimeSpan>())).Returns(true);
        var isOpenA = true;
        modelA.Setup(m => m.IsOpen).Returns(() => isOpenA);

        var modelB = new Mock<IModel>();
        modelB.Setup(m => m.IsOpen).Returns(true);
        modelB.Setup(m => m.CreateBasicProperties()).Returns(props.Object);
        modelB.Setup(m => m.WaitForConfirms(It.IsAny<TimeSpan>())).Returns(true);

        var conn = new Mock<IConnection>();
        conn.SetupSequence(c => c.CreateModel())
            .Returns(modelA.Object)
            .Returns(modelB.Object);

        using var sut = new RabbitMqOutboxSink(conn.Object, Opts());

        await sut.SendAsync(Env(), CancellationToken.None);
        isOpenA = false; // simulate channel closure
        await sut.SendAsync(Env(), CancellationToken.None);

        conn.Verify(c => c.CreateModel(), Times.Exactly(2));
    }
}
