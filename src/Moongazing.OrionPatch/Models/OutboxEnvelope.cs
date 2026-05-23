namespace Moongazing.OrionPatch.Models;

/// <summary>
/// The sink-facing view of an outbox message ready for dispatch.
/// Immutable; constructed by the dispatcher just before invoking the sink.
/// </summary>
/// <param name="Id">Unique id of the outbox row this envelope was materialized from.</param>
/// <param name="MessageType">Logical type name; defaults to the CLR FullName of T, or the override from <see cref="OutboxEnqueueOptions.MessageType"/>.</param>
/// <param name="Payload">JSON payload as serialized at enqueue time.</param>
/// <param name="Headers">Optional caller-supplied string headers (correlation, tenant, etc.).</param>
/// <param name="CorrelationId">Optional correlation id; picks up ambient correlation at enqueue time when not overridden.</param>
/// <param name="OccurredAtUtc">
/// When the domain event happened (caller-supplied or enqueue time). Values are treated as UTC by
/// convention; the enqueue boundary (Task 5) will enforce <c>Kind == DateTimeKind.Utc</c>.
/// </param>
/// <param name="AttemptNumber">
/// 1-based attempt counter for this dispatch. Equals the row's <see cref="OutboxRow.AttemptCount"/> + 1
/// at the moment of dispatch. Before the first attempt, the row's AttemptCount is 0 and the envelope's
/// AttemptNumber is 1; on a failed dispatch the row's AttemptCount increments to 1, so the envelope
/// materialized for the retry carries AttemptNumber = 2; and so on.
/// </param>
public sealed record OutboxEnvelope(
    Guid Id,
    string MessageType,
    string Payload,
    IReadOnlyDictionary<string, string>? Headers,
    string? CorrelationId,
    DateTime OccurredAtUtc,
    int AttemptNumber);
