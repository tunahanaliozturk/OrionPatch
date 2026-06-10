using Confluent.Kafka;

namespace Moongazing.OrionPatch.Kafka.Inbound;

/// <summary>
/// Sink for messages the inbound service routes to a dead-letter topic after they have
/// failed <see cref="KafkaInboxOptions.MaxDeliveryAttempts"/> times. Consumers register
/// their own implementation backed by the producer-side Confluent.Kafka
/// <see cref="IProducer{TKey, TValue}"/> (typically the same producer the v0.2.7
/// <c>AzureServiceBusOutboxSink</c> / <c>KafkaOutboxSink</c> uses).
/// </summary>
/// <remarks>
/// When DLQ routing is configured but no <see cref="IKafkaInboundDeadLetterProducer"/> is
/// registered, the inbound service falls back to logging the poison record and seeks the
/// partition back so Kafka redelivers indefinitely. That preserves the v0.2.8 behaviour
/// for deployments where DLQ routing is desired but the producer wiring is not yet
/// finished.
/// </remarks>
public interface IKafkaInboundDeadLetterProducer
{
    /// <summary>Produce <paramref name="message"/> to the dead-letter topic.</summary>
    Task ProduceAsync(string topic, Message<string, byte[]> message, CancellationToken cancellationToken);
}
