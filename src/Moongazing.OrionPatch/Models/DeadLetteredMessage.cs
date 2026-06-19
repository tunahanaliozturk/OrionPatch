namespace Moongazing.OrionPatch.Models;

/// <summary>
/// A snapshot of an outbox row that exhausted its delivery budget and was routed to the
/// dead-letter store, captured together with the final failure context. Unlike an in-place
/// <see cref="OutboxStatus.DeadLettered"/> row that remains in the hot outbox, a dead-lettered
/// message lives in a dedicated store
/// (<see cref="Moongazing.OrionPatch.Abstractions.IDeadLetterStore"/>) so the active outbox is
/// not polluted with terminal rows.
/// </summary>
/// <remarks>
/// All members are <c>init</c>-only: a dead-lettered message is an immutable record of a past
/// terminal event. The enqueue-time columns are copied verbatim from the originating
/// <see cref="OutboxRow"/> so the message can be re-hydrated, inspected, or manually replayed
/// without consulting the (possibly already-purged) source row.
/// </remarks>
public sealed class DeadLetteredMessage
{
    /// <summary>Identity of the originating <see cref="OutboxRow"/>. Stable across the move so a consumer can correlate or replay.</summary>
    public Guid Id { get; init; }

    /// <summary>Logical message type name, copied from the source row.</summary>
    public string MessageType { get; init; } = default!;

    /// <summary>JSON payload, copied from the source row.</summary>
    public string Payload { get; init; } = default!;

    /// <summary>Optional JSON-serialized header map (string to string), copied from the source row.</summary>
    public string? HeadersJson { get; init; }

    /// <summary>Optional correlation id, copied from the source row.</summary>
    public string? CorrelationId { get; init; }

    /// <summary>When the originating domain event occurred (UTC), copied from the source row.</summary>
    public DateTime OccurredAtUtc { get; init; }

    /// <summary>When the source row was written to the outbox (UTC), copied from the source row.</summary>
    public DateTime EnqueuedAtUtc { get; init; }

    /// <summary>Total number of dispatch attempts the row accumulated before it was abandoned.</summary>
    public int AttemptCount { get; init; }

    /// <summary>Truncated error text from the final dispatch attempt that triggered the dead-letter.</summary>
    public string FinalError { get; init; } = default!;

    /// <summary>UTC instant at which the message was routed into the dead-letter store.</summary>
    public DateTime DeadLetteredAtUtc { get; init; }
}
