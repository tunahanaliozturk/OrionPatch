using Confluent.Kafka;

namespace Moongazing.OrionPatch.Kafka.Inbound;

/// <summary>Configuration for <see cref="KafkaInboundHostedService"/>.</summary>
public sealed class KafkaInboxOptions
{
    /// <summary>Bootstrap brokers (comma-separated, e.g. <c>"kafka-1:9092"</c>).</summary>
    public string BootstrapServers { get; set; } = string.Empty;

    /// <summary>Consumer group id. Required.</summary>
    public string GroupId { get; set; } = string.Empty;

    /// <summary>Topics to subscribe to. At least one required.</summary>
    public IReadOnlyList<string> Topics { get; set; } = Array.Empty<string>();

    /// <summary>
    /// What to do when there is no committed offset for the group (first subscription).
    /// Default <see cref="AutoOffsetReset.Earliest"/> so consumers do not silently miss
    /// messages produced before the group was first deployed.
    /// </summary>
    public AutoOffsetReset AutoOffsetReset { get; set; } = AutoOffsetReset.Earliest;

    /// <summary>Per-consume blocking timeout. Default 1 second.</summary>
    public TimeSpan PollTimeout { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Backoff applied between iterations of the consume loop after a transient
    /// <see cref="ConsumeException"/>. Default 1 second; prevents the loop from
    /// hot-spinning under sustained broker / auth failures.
    /// </summary>
    public TimeSpan ConsumeRetryBackoff { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Optional dead-letter topic. When set, the inbound service routes a message to this
    /// topic AFTER the handler has failed <see cref="MaxDeliveryAttempts"/> times instead
    /// of indefinitely redelivering. The routed Kafka record carries the original record's
    /// headers + key + value plus diagnostic <c>orionpatch-dlq-*</c> headers (reason,
    /// original-topic, original-partition, original-offset, attempt count). Leave null to
    /// disable DLQ routing (the v0.2.8 behaviour: redeliver forever).
    /// </summary>
    public string? DeadLetterTopic { get; set; }

    /// <summary>
    /// Maximum number of times the inbound service will re-attempt a record before
    /// routing it to <see cref="DeadLetterTopic"/>. Default 5. The counter is kept in
    /// memory (per envelope id) and resets on consumer restart - DLQ routing is a
    /// best-effort poison-pill protection, not a transactional guarantee.
    /// </summary>
    public int MaxDeliveryAttempts { get; set; } = 5;
}
