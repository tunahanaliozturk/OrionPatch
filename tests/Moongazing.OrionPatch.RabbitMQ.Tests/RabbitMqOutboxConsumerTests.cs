using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Channels;
using Moongazing.OrionPatch.Models;
using Moongazing.OrionPatch.RabbitMQ;
using Moq;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Xunit;

namespace Moongazing.OrionPatch.RabbitMQ.Tests;

public sealed class RabbitMqOutboxConsumerTests
{
    private sealed class RecordingHandler : IOrionPatchMessageHandler
    {
        public List<OutboxEnvelope> Received { get; } = new();
        public Exception? ThrowOnHandle { get; set; }

        public Task HandleAsync(OutboxEnvelope envelope, CancellationToken cancellationToken)
        {
            if (ThrowOnHandle is not null)
            {
                throw ThrowOnHandle;
            }
            Received.Add(envelope);
            return Task.CompletedTask;
        }
    }

    private static (Mock<IConnection> conn, Mock<IModel> model, AsyncEventingBasicConsumer? capturedConsumer) NewMocks()
    {
        var model = new Mock<IModel>();
        model.Setup(m => m.IsOpen).Returns(true);

        var conn = new Mock<IConnection>();
        conn.Setup(c => c.CreateModel()).Returns(model.Object);
        return (conn, model, null);
    }

    private static IOptions<RabbitMqOutboxConsumerOptions> Opts(Action<RabbitMqOutboxConsumerOptions>? configure = null)
    {
        var o = new RabbitMqOutboxConsumerOptions();
        configure?.Invoke(o);
        return Options.Create(o);
    }

    private static (RabbitMqOutboxConsumer consumer, IModel model, AsyncEventingBasicConsumer broker, RecordingHandler handler, InMemoryInbox inbox)
        BuildAndStart(Action<RabbitMqOutboxConsumerOptions>? configure = null)
    {
        var (conn, model, _) = NewMocks();

        AsyncEventingBasicConsumer? captured = null;
        model.Setup(m => m.BasicConsume(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<bool>(),
                It.IsAny<IDictionary<string, object>>(),
                It.IsAny<IBasicConsumer>()))
            .Callback<string, bool, string, bool, bool, IDictionary<string, object>, IBasicConsumer>(
                (_, _, _, _, _, _, c) => captured = (AsyncEventingBasicConsumer)c)
            .Returns("tag");

        var handler = new RecordingHandler();
        var inbox = new InMemoryInbox();

        var services = new ServiceCollection();
        services.AddSingleton<IInbox>(inbox);
        services.AddSingleton<IOrionPatchMessageHandler>(handler);
        var sp = services.BuildServiceProvider();

        var consumer = new RabbitMqOutboxConsumer(conn.Object, sp, Opts(configure));

        consumer.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        return (consumer, model.Object, captured!, handler, inbox);
    }

    private static BasicDeliverEventArgs Delivery(
        ulong deliveryTag,
        Guid envelopeId,
        string payload = "{}",
        string messageType = "Demo.Event",
        IDictionary<string, object>? extraHeaders = null)
    {
        var props = new Mock<IBasicProperties>();
        var headers = new Dictionary<string, object>
        {
            ["orionpatch-envelope-id"] = Encoding.UTF8.GetBytes(envelopeId.ToString("N")),
            ["orionpatch-message-type"] = Encoding.UTF8.GetBytes(messageType),
        };
        if (extraHeaders is not null)
        {
            foreach (var (k, v) in extraHeaders)
            {
                headers[k] = v;
            }
        }
        props.Setup(p => p.Headers).Returns(headers);
        props.Setup(p => p.Type).Returns(messageType);

        return new BasicDeliverEventArgs(
            consumerTag: "tag",
            deliveryTag: deliveryTag,
            redelivered: false,
            exchange: "x",
            routingKey: "rk",
            properties: props.Object,
            body: new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(payload)));
    }

    [Fact]
    public async Task First_delivery_invokes_handler_and_acks()
    {
        var (sut, model, broker, handler, _) = BuildAndStart();
        var id = Guid.NewGuid();
        var modelMock = Mock.Get(model);

        await broker.HandleBasicDeliver("tag", 7, false, "x", "rk", Delivery(7, id).BasicProperties,
            new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes("{\"a\":1}")));

        Assert.Single(handler.Received);
        Assert.Equal(id, handler.Received[0].Id);
        modelMock.Verify(m => m.BasicAck(7, false), Times.Once);

        await sut.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Duplicate_delivery_acks_without_invoking_handler()
    {
        var (sut, model, broker, handler, inbox) = BuildAndStart();
        var id = Guid.NewGuid();
        await inbox.TryAcceptAsync(id, CancellationToken.None);
        var modelMock = Mock.Get(model);

        await broker.HandleBasicDeliver("tag", 8, false, "x", "rk", Delivery(8, id).BasicProperties,
            new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes("{}")));

        Assert.Empty(handler.Received);
        modelMock.Verify(m => m.BasicAck(8, false), Times.Once);

        await sut.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Duplicate_delivery_with_AckDuplicates_false_nacks_without_requeue()
    {
        var (sut, model, broker, handler, inbox) = BuildAndStart(o => o.AckDuplicates = false);
        var id = Guid.NewGuid();
        await inbox.TryAcceptAsync(id, CancellationToken.None);
        var modelMock = Mock.Get(model);

        await broker.HandleBasicDeliver("tag", 9, false, "x", "rk", Delivery(9, id).BasicProperties,
            new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes("{}")));

        Assert.Empty(handler.Received);
        modelMock.Verify(m => m.BasicNack(9, false, false), Times.Once);

        await sut.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Missing_envelope_id_header_nacks_without_requeue()
    {
        var (sut, model, broker, handler, _) = BuildAndStart();
        var props = new Mock<IBasicProperties>();
        props.Setup(p => p.Headers).Returns(new Dictionary<string, object>());
        var modelMock = Mock.Get(model);

        await broker.HandleBasicDeliver("tag", 10, false, "x", "rk", props.Object,
            new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes("{}")));

        Assert.Empty(handler.Received);
        modelMock.Verify(m => m.BasicNack(10, false, false), Times.Once);

        await sut.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Handler_exception_nacks_with_requeue_by_default()
    {
        var (sut, model, broker, handler, _) = BuildAndStart();
        handler.ThrowOnHandle = new InvalidOperationException("boom");
        var modelMock = Mock.Get(model);

        await broker.HandleBasicDeliver("tag", 11, false, "x", "rk", Delivery(11, Guid.NewGuid()).BasicProperties,
            new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes("{}")));

        modelMock.Verify(m => m.BasicNack(11, false, true), Times.Once);

        await sut.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Handler_exception_with_RequeueOnFailure_false_nacks_without_requeue()
    {
        var (sut, model, broker, handler, _) = BuildAndStart(o => o.RequeueOnFailure = false);
        handler.ThrowOnHandle = new InvalidOperationException("boom");
        var modelMock = Mock.Get(model);

        await broker.HandleBasicDeliver("tag", 12, false, "x", "rk", Delivery(12, Guid.NewGuid()).BasicProperties,
            new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes("{}")));

        modelMock.Verify(m => m.BasicNack(12, false, false), Times.Once);

        await sut.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task ExecuteAsync_sets_QoS_prefetch_to_configured_value()
    {
        var (conn, model, _) = NewMocks();
        model.Setup(m => m.BasicConsume(
            It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<string>(),
            It.IsAny<bool>(), It.IsAny<bool>(),
            It.IsAny<IDictionary<string, object>>(), It.IsAny<IBasicConsumer>()))
            .Returns("tag");

        var services = new ServiceCollection();
        services.AddSingleton<IInbox>(new InMemoryInbox());
        services.AddSingleton<IOrionPatchMessageHandler>(new RecordingHandler());
        var sp = services.BuildServiceProvider();

        var sut = new RabbitMqOutboxConsumer(conn.Object, sp, Opts(o => o.PrefetchCount = 32));
        await sut.StartAsync(CancellationToken.None);

        model.Verify(m => m.BasicQos(0, 32, false), Times.Once);

        await sut.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task BuildEnvelope_propagates_caller_supplied_headers_excluding_orionpatch_prefix()
    {
        var (sut, _, broker, handler, _) = BuildAndStart();
        var id = Guid.NewGuid();
        var extra = new Dictionary<string, object>
        {
            ["traceparent"] = Encoding.UTF8.GetBytes("00-aaa-bbb-01"),
            ["tenant"] = Encoding.UTF8.GetBytes("acme"),
        };
        var delivery = Delivery(13, id, extraHeaders: extra);

        await broker.HandleBasicDeliver("tag", 13, false, "x", "rk", delivery.BasicProperties, delivery.Body);

        Assert.Single(handler.Received);
        var headers = handler.Received[0].Headers;
        Assert.NotNull(headers);
        Assert.Equal("00-aaa-bbb-01", headers!["traceparent"]);
        Assert.Equal("acme", headers["tenant"]);
        Assert.False(headers.ContainsKey("orionpatch-envelope-id"));

        await sut.StopAsync(CancellationToken.None);
    }

    [Fact]
    public void AddOrionPatchRabbitMqConsumer_registers_handler_scoped()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IConnection>(new Mock<IConnection>().Object);
        services.AddSingleton<IInbox>(new InMemoryInbox());
        services.AddOrionPatchRabbitMqConsumer<RecordingHandler>(o => o.QueueName = "test-q");

        using var sp = services.BuildServiceProvider();

        using var scope1 = sp.CreateScope();
        var h1 = scope1.ServiceProvider.GetRequiredService<IOrionPatchMessageHandler>();
        using var scope2 = sp.CreateScope();
        var h2 = scope2.ServiceProvider.GetRequiredService<IOrionPatchMessageHandler>();

        Assert.IsType<RecordingHandler>(h1);
        Assert.NotSame(h1, h2);
    }
}
