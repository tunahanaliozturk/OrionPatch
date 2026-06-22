namespace Moongazing.OrionPatch.EntityFrameworkCore;

using Moongazing.OrionPatch.Models;

/// <summary>
/// EF Core-backed archive row. A snapshot of a successfully dispatched
/// (<see cref="OutboxStatus.Processed"/>) outbox row that crossed the retention window and was
/// reaped out of the hot outbox by <see cref="Abstractions.IOutboxArchivalStore"/> in archive
/// mode. Carries the full <see cref="OutboxRow"/> shape so an archived row can be inspected or
/// re-hydrated without consulting the (now removed) source row.
/// </summary>
/// <remarks>
/// <para>
/// The archive table (<c>OrionPatch_OutboxArchive</c>) is intentionally index-light compared to
/// <c>OrionPatch_Outbox</c>: it is a cold landing area, never on the dispatcher's claim path, so
/// it does not carry the status/lease covering indexes. Only <see cref="ProcessedAtUtc"/> is
/// indexed, for retention sweeps and audit range queries over the archive itself.
/// </para>
/// <para>
/// In purge mode the reaper deletes processed rows outright and never writes this table; the
/// archive then stays empty by design.
/// </para>
/// </remarks>
public sealed class OutboxArchiveRow
{
    /// <summary>Primary key, copied verbatim from the originating <see cref="OutboxRow.Id"/>.</summary>
    public Guid Id { get; set; }

    /// <summary>Logical message type name.</summary>
    public string MessageType { get; set; } = default!;

    /// <summary>JSON payload.</summary>
    public string Payload { get; set; } = default!;

    /// <summary>Optional JSON-serialized header map (string to string).</summary>
    public string? HeadersJson { get; set; }

    /// <summary>Optional correlation id.</summary>
    public string? CorrelationId { get; set; }

    /// <summary>When the originating domain event occurred (UTC).</summary>
    public DateTime OccurredAtUtc { get; set; }

    /// <summary>When the source row was written to the outbox (UTC).</summary>
    public DateTime EnqueuedAtUtc { get; set; }

    /// <summary>Total number of dispatch attempts the row accumulated.</summary>
    public int AttemptCount { get; set; }

    /// <summary>Most recent error text (truncated), preserved from the source row.</summary>
    public string? LastError { get; set; }

    /// <summary>When the row reached <see cref="OutboxStatus.Processed"/> (UTC). The retention cutoff is measured against this column.</summary>
    public DateTime? ProcessedAtUtc { get; set; }

    /// <summary>Project this archive row back into the storage-facing <see cref="OutboxRow"/> view.</summary>
    /// <returns>An <see cref="OutboxRow"/> in <see cref="OutboxStatus.Processed"/> state carrying this row's columns.</returns>
    public OutboxRow ToRow() => new()
    {
        Id = Id,
        MessageType = MessageType,
        Payload = Payload,
        HeadersJson = HeadersJson,
        CorrelationId = CorrelationId,
        OccurredAtUtc = OccurredAtUtc,
        EnqueuedAtUtc = EnqueuedAtUtc,
        Status = OutboxStatus.Processed,
        AttemptCount = AttemptCount,
        LastError = LastError,
        ProcessedAtUtc = ProcessedAtUtc,
    };
}
