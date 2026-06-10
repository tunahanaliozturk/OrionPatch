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
}
