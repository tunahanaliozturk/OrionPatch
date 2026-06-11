namespace Moongazing.OrionPatch.Kafka.Inbound;

using System.Diagnostics.Metrics;

/// <summary>
/// v0.2.13 OpenTelemetry instrumentation for the Kafka inbound consumer. Operators wire
/// these counters into Grafana to visualise per-topic DLQ patterns instead of scraping
/// log messages.
/// </summary>
public static class KafkaInboundDiagnostics
{
    /// <summary>Meter name used by the Kafka inbound hosted service.</summary>
    public const string MeterName = "Moongazing.OrionPatch.Kafka.Inbound";

    private static readonly Meter Meter = new(MeterName, "0.2.13");

    /// <summary>
    /// Number of times the inbound hosted service called
    /// <c>IKafkaAttemptCountStore.SetAsync</c> for a failed delivery (every attempt bump
    /// after the first failure). Tagged with <c>topic</c>. Operators graph the rate to
    /// spot redelivery storms before they reach the DLQ.
    /// </summary>
    internal static readonly Counter<long> AttemptSet = Meter.CreateCounter<long>(
        "orionpatch.kafka.inbound.attempt_set");

    /// <summary>
    /// Number of envelopes routed to the configured dead-letter topic after exhausting
    /// <c>MaxDeliveryAttempts</c>. Tagged with <c>topic</c> (source) and <c>dlq</c>
    /// (destination) so multi-topic deployments can split alarms per route.
    /// </summary>
    internal static readonly Counter<long> DlqRouted = Meter.CreateCounter<long>(
        "orionpatch.kafka.inbound.dlq_routed");

    /// <summary>Record a single attempt-store bump; tagged with the source topic.</summary>
    public static void RecordAttemptSet(string topic)
        => AttemptSet.Add(1, new System.Collections.Generic.KeyValuePair<string, object?>("topic", topic));

    /// <summary>Record a single DLQ route; tagged with the source topic and destination DLQ topic.</summary>
    public static void RecordDlqRouted(string sourceTopic, string dlqTopic)
        => DlqRouted.Add(1,
            new System.Collections.Generic.KeyValuePair<string, object?>("topic", sourceTopic),
            new System.Collections.Generic.KeyValuePair<string, object?>("dlq", dlqTopic));
}
