namespace Moongazing.OrionPatch.Kafka.Inbound;

/// <summary>
/// Handler invoked by <see cref="KafkaInboundHostedService"/> for each accepted Kafka
/// message. Throwing from the handler is treated as a transient failure - the inbox
/// rollback runs and the offset is NOT committed so Kafka redelivers on the next consume.
/// </summary>
public interface IKafkaInboundHandler
{
    /// <summary>Process one message. Throwing triggers redelivery.</summary>
    Task HandleAsync(InboundKafkaMessage message, CancellationToken cancellationToken);
}
