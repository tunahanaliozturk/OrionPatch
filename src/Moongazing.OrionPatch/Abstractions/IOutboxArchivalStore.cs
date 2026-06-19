namespace Moongazing.OrionPatch.Abstractions;

using Moongazing.OrionPatch.Models;

/// <summary>
/// v0.3.0 archival capability for <see cref="IOutboxStorage"/>. Successfully dispatched rows
/// (<see cref="OutboxStatus.Processed"/>) accumulate in the hot outbox forever unless something
/// reaps them; an ever-growing table degrades claim-query planning and storage cost. This
/// capability moves processed rows older than a retention window out of the active outbox -
/// either into a separate archive store or by purging them outright - while leaving
/// not-yet-dispatched rows (Pending / Claimed) and dead-lettered rows untouched.
/// </summary>
/// <remarks>
/// <para>
/// "Older than" is measured against <see cref="OutboxRow.ProcessedAtUtc"/>, the instant the row
/// was successfully dispatched, NOT its enqueue time: the retention window is "how long do we
/// keep a row after we are done with it", which is the audit/replay horizon operators reason about.
/// </para>
/// <para>
/// The reap is idempotent and incremental: calling it repeatedly only ever affects rows that have
/// crossed the cutoff, and a row archived or purged on one pass is gone on the next. Implementations
/// that archive (rather than purge) expose the archived rows via <see cref="GetArchivedAsync"/>;
/// implementations that purge return an empty archive.
/// </para>
/// </remarks>
public interface IOutboxArchivalStore
{
    /// <summary>
    /// Move every <see cref="OutboxStatus.Processed"/> row whose
    /// <see cref="OutboxRow.ProcessedAtUtc"/> is at or before <c>nowUtc - retention</c> out of the
    /// active outbox. Pending, Claimed, and DeadLettered rows are never touched, and a processed
    /// row still inside the retention window is never touched.
    /// </summary>
    /// <param name="retention">
    /// How long a processed row is retained in the hot outbox after dispatch. Must be non-negative;
    /// <see cref="TimeSpan.Zero"/> reaps every processed row immediately.
    /// </param>
    /// <param name="nowUtc">The UTC "now" against which the cutoff is computed; supplied by the caller's clock so the reap is deterministic under test.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of rows moved out of the active outbox on this pass.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="retention"/> is negative.</exception>
    Task<int> ArchiveProcessedAsync(TimeSpan retention, DateTime nowUtc, CancellationToken cancellationToken = default);

    /// <summary>
    /// Snapshot the rows currently held in the archive. Purging implementations return an empty
    /// list (purged rows are discarded, not retained); archiving implementations return the moved rows.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<IReadOnlyList<OutboxRow>> GetArchivedAsync(CancellationToken cancellationToken = default);
}
