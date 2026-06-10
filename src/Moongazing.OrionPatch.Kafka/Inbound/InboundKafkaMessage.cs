namespace Moongazing.OrionPatch.Kafka.Inbound;

/// <summary>
/// Materialised view of a Kafka record the inbound consumer surfaces to the registered
/// handler. Carries the OrionPatch envelope metadata extracted from Kafka headers plus
/// the Kafka coordinates so the handler can correlate with broker telemetry.
/// </summary>
/// <param name="EnvelopeId">v0.2.7 outbound stamping reads back as a Guid N format string in <c>orionpatch-envelope-id</c>.</param>
/// <param name="MessageType">Value of <c>orionpatch-message-type</c>.</param>
/// <param name="CorrelationId">Value of <c>orionpatch-correlation-id</c> when present.</param>
/// <param name="Payload">Raw record value (UTF-8 bytes; the handler decides JSON vs binary).</param>
/// <param name="Headers">Caller-supplied headers (non-orionpatch-* keys propagated through).</param>
/// <param name="Topic">Topic the record was consumed from.</param>
/// <param name="Partition">Partition number.</param>
/// <param name="Offset">Offset within the partition.</param>
public sealed record InboundKafkaMessage(
    Guid EnvelopeId,
    string MessageType,
    string? CorrelationId,
    byte[] Payload,
    IReadOnlyDictionary<string, string> Headers,
    string Topic,
    int Partition,
    long Offset);
