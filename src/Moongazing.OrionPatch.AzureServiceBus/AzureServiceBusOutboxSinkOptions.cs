namespace Moongazing.OrionPatch.AzureServiceBus;

/// <summary>
/// Configuration for <see cref="AzureServiceBusOutboxSink"/>.
/// </summary>
public sealed class AzureServiceBusOutboxSinkOptions
{
    /// <summary>
    /// Azure Service Bus connection string. Consumers who register their own
    /// <c>ServiceBusClient</c> (e.g. via Azure Identity / managed identity) may leave this
    /// null; the sink resolves the client from DI when this property is null.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// Target entity path - a queue name or a topic name. Default <c>"orionpatch"</c>.
    /// The sink does NOT create queues / topics; provisioning is an operator concern.
    /// </summary>
    public string EntityPath { get; set; } = "orionpatch";

    /// <summary>
    /// Function that maps an <see cref="Models.OutboxEnvelope"/> to a subject (Service Bus
    /// <c>Subject</c> / SBMP-legacy <c>Label</c>). Default uses the envelope's
    /// <see cref="Models.OutboxEnvelope.MessageType"/>; routing filters on topics typically
    /// filter on Subject + ApplicationProperties.
    /// </summary>
    public Func<Models.OutboxEnvelope, string> SubjectSelector { get; set; }
        = envelope => envelope.MessageType;

    /// <summary>
    /// Content type stamped on every outgoing message. Default <c>"application/json"</c>
    /// matches the OrionPatch envelope payload convention.
    /// </summary>
    public string ContentType { get; set; } = "application/json";

    /// <summary>
    /// Per-send timeout. Default 30 seconds. Service Bus client has its own retry policy
    /// configured at <see cref="Azure.Messaging.ServiceBus.ServiceBusClient"/> construction
    /// time; this is an additional ceiling so a hung send does not stall the outbox
    /// dispatcher loop.
    /// </summary>
    public TimeSpan SendTimeout { get; set; } = TimeSpan.FromSeconds(30);
}
