namespace Moongazing.OrionPatch.EntityFrameworkCore;

using Moongazing.OrionPatch.Models;

/// <summary>
/// EF Core-backed dead-letter row. One row per outbox message that exhausted its delivery
/// budget and was routed out of the hot outbox into the durable dead-letter store
/// (<see cref="Abstractions.IDeadLetterStore"/>). Persists the v0.3.0
/// <see cref="DeadLetteredMessage"/> snapshot across process restarts so a relational backend
/// owns the terminal record rather than leaving a <see cref="OutboxStatus.DeadLettered"/> row in
/// the active outbox.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="Id"/> is the identity of the originating <see cref="OutboxRow"/> and the primary
/// key of the dead-letter table. Reusing the source-row id as the key is what makes routing
/// idempotent: a crash-replayed terminal path that tries to dead-letter the same row a second
/// time hits the primary-key constraint, which the storage treats as the exactly-once no-op.
/// </para>
/// <para>
/// The columns mirror <see cref="DeadLetteredMessage"/> one-to-one. They are mutable
/// (<c>set</c>) only because EF Core materializes entities through property setters; a
/// dead-letter row is an immutable record of a past terminal event and is never updated in
/// place by OrionPatch.
/// </para>
/// </remarks>
public sealed class DeadLetterRow
{
    /// <summary>Identity of the originating <see cref="OutboxRow"/> and primary key of the dead-letter table.</summary>
    public Guid Id { get; set; }

    /// <summary>Logical message type name, copied from the source row.</summary>
    public string MessageType { get; set; } = default!;

    /// <summary>JSON payload, copied from the source row.</summary>
    public string Payload { get; set; } = default!;

    /// <summary>Optional JSON-serialized header map (string to string), copied from the source row.</summary>
    public string? HeadersJson { get; set; }

    /// <summary>Optional correlation id, copied from the source row.</summary>
    public string? CorrelationId { get; set; }

    /// <summary>When the originating domain event occurred (UTC), copied from the source row.</summary>
    public DateTime OccurredAtUtc { get; set; }

    /// <summary>When the source row was written to the outbox (UTC), copied from the source row.</summary>
    public DateTime EnqueuedAtUtc { get; set; }

    /// <summary>Total number of dispatch attempts the row accumulated before it was abandoned.</summary>
    public int AttemptCount { get; set; }

    /// <summary>Truncated error text from the final dispatch attempt that triggered the dead-letter.</summary>
    public string FinalError { get; set; } = default!;

    /// <summary>UTC instant at which the message was routed into the dead-letter store.</summary>
    public DateTime DeadLetteredAtUtc { get; set; }

    /// <summary>Project this persisted row into the storage-facing <see cref="DeadLetteredMessage"/> snapshot.</summary>
    /// <returns>An immutable <see cref="DeadLetteredMessage"/> carrying this row's columns.</returns>
    public DeadLetteredMessage ToMessage() => new()
    {
        Id = Id,
        MessageType = MessageType,
        Payload = Payload,
        HeadersJson = HeadersJson,
        CorrelationId = CorrelationId,
        OccurredAtUtc = OccurredAtUtc,
        EnqueuedAtUtc = EnqueuedAtUtc,
        AttemptCount = AttemptCount,
        FinalError = FinalError,
        DeadLetteredAtUtc = DeadLetteredAtUtc,
    };
}
