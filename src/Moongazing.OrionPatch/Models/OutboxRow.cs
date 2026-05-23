namespace Moongazing.OrionPatch.Models;

/// <summary>
/// Storage-facing view of an outbox row. Used between the storage SPI and the dispatcher.
/// EF Core mapping for this type lives in the OrionPatch.EntityFrameworkCore package
/// so the core library stays EF-free.
/// </summary>
/// <remarks>
/// Mutability convention: identity and enqueue-time columns (<see cref="Id"/>, <see cref="MessageType"/>,
/// <see cref="Payload"/>, <see cref="HeadersJson"/>, <see cref="CorrelationId"/>, <see cref="OccurredAtUtc"/>,
/// <see cref="EnqueuedAtUtc"/>) are <c>init</c>-only because they are immutable for the row's lifetime;
/// lifecycle columns (<see cref="Status"/>, <see cref="AttemptCount"/>, <see cref="ClaimedAtUtc"/>,
/// <see cref="ClaimedBy"/>, <see cref="LastError"/>, <see cref="ProcessedAtUtc"/>, <see cref="NextAttemptAtUtc"/>)
/// are <c>set</c> because the dispatcher mutates them on claim/complete/fail/dead-letter transitions.
/// </remarks>
public sealed class OutboxRow
{
    /// <summary>Primary key.</summary>
    public Guid Id { get; init; }

    /// <summary>Logical message type name.</summary>
    public string MessageType { get; init; } = default!;

    /// <summary>JSON payload.</summary>
    public string Payload { get; init; } = default!;

    /// <summary>Optional JSON-serialized header map (string to string).</summary>
    public string? HeadersJson { get; init; }

    /// <summary>Optional correlation id.</summary>
    public string? CorrelationId { get; init; }

    /// <summary>
    /// When the originating domain event occurred (UTC). Treated as UTC by convention;
    /// the enqueue boundary (Task 5) will enforce <c>Kind == DateTimeKind.Utc</c>.
    /// </summary>
    public DateTime OccurredAtUtc { get; init; }

    /// <summary>When the row was written to the outbox (UTC); defaults to <see cref="OccurredAtUtc"/>.</summary>
    public DateTime EnqueuedAtUtc { get; init; }

    /// <summary>Current lifecycle state.</summary>
    public OutboxStatus Status { get; set; }

    /// <summary>Number of dispatch attempts so far.</summary>
    public int AttemptCount { get; set; }

    /// <summary>Time the current dispatcher claimed this row (UTC); the lease expires at ClaimedAtUtc + LeaseDuration.</summary>
    public DateTime? ClaimedAtUtc { get; set; }

    /// <summary>Identity of the dispatcher holding the current claim.</summary>
    public string? ClaimedBy { get; set; }

    /// <summary>Most recent error text (truncated).</summary>
    public string? LastError { get; set; }

    /// <summary>When the row reached <see cref="OutboxStatus.Processed"/> (UTC).</summary>
    public DateTime? ProcessedAtUtc { get; set; }

    /// <summary>Earliest UTC at which the next dispatch attempt should run (used by the backoff strategy).</summary>
    public DateTime? NextAttemptAtUtc { get; set; }
}
