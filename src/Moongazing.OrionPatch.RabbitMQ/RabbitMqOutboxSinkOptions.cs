namespace Moongazing.OrionPatch.RabbitMQ;

/// <summary>
/// Configuration for <see cref="RabbitMqOutboxSink"/>.
/// </summary>
public sealed class RabbitMqOutboxSinkOptions
{
    /// <summary>
    /// AMQP connection string (e.g. <c>amqp://guest:guest@localhost:5672/</c>). Consumers
    /// using `services.AddSingleton&lt;IConnection&gt;(sp =&gt; ...)` may leave this null
    /// and register the connection themselves; the sink resolves
    /// <c>IConnection</c> from DI when this property is null.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// Target exchange name. Must already exist on the broker (the sink does NOT declare
    /// exchanges; provisioning is an operator concern). Default <c>"orionpatch"</c>.
    /// </summary>
    public string ExchangeName { get; set; } = "orionpatch";

    /// <summary>
    /// Function that maps an <see cref="Models.OutboxEnvelope"/> to a routing key.
    /// Default uses the envelope's <see cref="Models.OutboxEnvelope.MessageType"/>.
    /// </summary>
    public Func<Models.OutboxEnvelope, string> RoutingKeySelector { get; set; }
        = envelope => envelope.MessageType;

    /// <summary>
    /// When true (default), opens a channel with publisher confirms enabled and waits for
    /// broker acknowledgement before returning from <c>SendAsync</c>. When false, the sink
    /// publishes fire-and-forget; the OrionPatch outbox still re-delivers on failure but
    /// the per-message latency is lower. Production deployments should keep this on.
    /// </summary>
    public bool UsePublisherConfirms { get; set; } = true;

    /// <summary>
    /// Maximum time to wait for a publisher-confirm ack. Default 10 seconds. Exceeded =&gt;
    /// <c>SendAsync</c> throws so the outbox row stays unprocessed and gets re-delivered.
    /// </summary>
    public TimeSpan ConfirmTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// When true (default), messages are published with <c>DeliveryMode = Persistent</c>
    /// so they survive broker restart on durable queues. Set false for transient queues
    /// where durability is not required.
    /// </summary>
    public bool PersistentDelivery { get; set; } = true;

    /// <summary>
    /// Optional content type stamped on every outgoing message. Default
    /// <c>"application/json"</c>; matches the OrionPatch envelope payload convention.
    /// </summary>
    public string ContentType { get; set; } = "application/json";
}
