using Confluent.Kafka;

namespace Moongazing.OrionPatch.Kafka;

/// <summary>
/// Configuration for <see cref="KafkaOutboxSink"/>.
/// </summary>
public sealed class KafkaOutboxSinkOptions
{
    /// <summary>
    /// Comma-separated Kafka bootstrap broker list (e.g. <c>"kafka-1:9092,kafka-2:9092"</c>).
    /// Required when the consumer uses the auto-wired <see cref="DefaultKafkaProducerFactory"/>;
    /// consumers registering their own factory may leave this empty.
    /// </summary>
    public string BootstrapServers { get; set; } = string.Empty;

    /// <summary>
    /// Target topic. Default <c>"orionpatch"</c>. The sink does NOT create topics;
    /// provisioning is an operator concern.
    /// </summary>
    public string Topic { get; set; } = "orionpatch";

    /// <summary>
    /// Function that maps an <see cref="Models.OutboxEnvelope"/> to a topic name. Default
    /// uses <see cref="Topic"/>; override to route different message types to different
    /// topics in a single sink registration.
    /// </summary>
    public Func<Models.OutboxEnvelope, string>? TopicSelector { get; set; }

    /// <summary>
    /// Function that maps an <see cref="Models.OutboxEnvelope"/> to a Kafka message key.
    /// Default uses the envelope id (Guid N format) so the partition is stable per
    /// envelope. Override to route by aggregate id / tenant when partition ordering by
    /// that axis is meaningful.
    /// </summary>
    public Func<Models.OutboxEnvelope, string> KeySelector { get; set; }
        = envelope => envelope.Id.ToString("N");

    /// <summary>
    /// When true (default), enables Kafka's exactly-once idempotent producer mode so
    /// broker-side retries during transient failures do not duplicate messages. Sets
    /// <c>EnableIdempotence = true</c> on the producer config; requires
    /// <see cref="Acks"/> = <see cref="global::Confluent.Kafka.Acks.All"/>.
    /// </summary>
    public bool EnableIdempotence { get; set; } = true;

    /// <summary>
    /// Acknowledgement requirement. Default <see cref="global::Confluent.Kafka.Acks.All"/>;
    /// required when <see cref="EnableIdempotence"/> is true.
    /// </summary>
    public Acks Acks { get; set; } = Acks.All;

    /// <summary>
    /// Per-send timeout. Default 30 seconds. Confluent.Kafka has its own internal queue
    /// timeout; this is an additional ceiling so a hung produce does not stall the
    /// dispatcher loop.
    /// </summary>
    public TimeSpan SendTimeout { get; set; } = TimeSpan.FromSeconds(30);
}
