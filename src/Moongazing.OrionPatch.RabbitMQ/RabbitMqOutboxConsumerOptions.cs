namespace Moongazing.OrionPatch.RabbitMQ;

/// <summary>
/// Configuration for <see cref="RabbitMqOutboxConsumer"/>.
/// </summary>
public sealed class RabbitMqOutboxConsumerOptions
{
    /// <summary>
    /// Name of the queue to consume from. Must already exist on the broker (the consumer
    /// does NOT declare queues; provisioning is an operator concern, same as the v0.2.4
    /// publisher contract).
    /// </summary>
    public string QueueName { get; set; } = "orionpatch";

    /// <summary>
    /// QoS prefetch count - how many messages the broker may push to this consumer before
    /// waiting for acks. Default 8; tune up for high-throughput / low-latency handlers,
    /// tune down when handler latency is high so a slow consumer does not hog the queue.
    /// </summary>
    public ushort PrefetchCount { get; set; } = 8;

    /// <summary>
    /// Consumer-tag stamped on the AMQP <c>basic.consume</c> registration. Useful for
    /// operator observability via the RabbitMQ management UI. Default
    /// <c>"orionpatch-consumer"</c>.
    /// </summary>
    public string ConsumerTag { get; set; } = "orionpatch-consumer";

    /// <summary>
    /// When a handler throws an exception, the consumer NACKs the message. When this is
    /// <see langword="true"/> (default) the NACK requeues so the broker re-delivers;
    /// set <see langword="false"/> when paired with a dead-letter exchange so failures are
    /// captured there for operator review instead of looping.
    /// </summary>
    public bool RequeueOnFailure { get; set; } = true;

    /// <summary>
    /// Consume duplicate envelopes (as detected by the <see cref="Abstractions.IInbox"/>)
    /// as silent ACKs. Default <see langword="true"/>; set <see langword="false"/> to
    /// require the broker to remove the duplicate via a NACK without requeue.
    /// </summary>
    public bool AckDuplicates { get; set; } = true;
}
