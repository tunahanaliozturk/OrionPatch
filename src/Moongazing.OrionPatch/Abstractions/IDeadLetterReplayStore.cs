namespace Moongazing.OrionPatch.Abstractions;

using Moongazing.OrionPatch.Models;

/// <summary>
/// v0.3.3 dead-letter <em>replay</em> (redrive) capability. A
/// <see cref="DeadLetteredMessage"/> held by an <see cref="IDeadLetterStore"/> is otherwise a
/// terminal record: it can be read for triage but never put back into circulation. This
/// capability closes that loop by re-enqueuing a dead-lettered message into the active outbox as a
/// fresh <see cref="OutboxStatus.Pending"/> row so the dispatcher picks it up again, once the
/// underlying failure has been fixed.
/// </summary>
/// <remarks>
/// <para>
/// Implementations are co-located with <see cref="IOutboxStorage"/> and
/// <see cref="IDeadLetterStore"/> so the redrive (append a fresh pending outbox row, remove the
/// dead-letter record) is atomic per message: the message is never both dead-lettered and live,
/// and a crash mid-move leaves it in exactly one place. The re-enqueued row is a NEW row id with a
/// reset attempt count and cleared failure context; the original payload, headers, and correlation
/// id are preserved, and a <see cref="RedrivenFromHeader"/> header is stamped carrying the source
/// dead-letter id for traceability.
/// </para>
/// <para>
/// Redrive is idempotent on the dead-letter id: a double-click or a retried call for a message
/// that is already redriven (or never present) is a clean no-op, reported via
/// <see cref="RedriveResult.Skipped"/> rather than enqueuing a duplicate.
/// </para>
/// <para>
/// A storage backend opts in by implementing this interface in addition to
/// <see cref="IDeadLetterStore"/>; backends that do not implement it simply expose no redrive
/// path, which is fully backward compatible.
/// </para>
/// </remarks>
public interface IDeadLetterReplayStore
{
    /// <summary>
    /// Header key stamped onto a redriven message's fresh outbox row, carrying the source
    /// dead-letter id (the original outbox row id) as its value so a redriven message can be
    /// traced back to the terminal record it was replayed from.
    /// </summary>
    public const string RedrivenFromHeader = "redriven-from";

    /// <summary>
    /// Re-enqueue a single dead-lettered message back into the active outbox as a fresh pending row
    /// and remove it from the dead-letter store, atomically. The new row keeps the original payload,
    /// headers, and correlation id, resets the attempt count to zero, clears the failure context,
    /// and stamps a <see cref="RedrivenFromHeader"/> header with the source id.
    /// </summary>
    /// <param name="messageId">Dead-letter id (the originating outbox row id) to redrive.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// <see langword="true"/> when this call re-enqueued the message; <see langword="false"/> when
    /// the message was already redriven or not present (the idempotent no-op).
    /// </returns>
    Task<bool> RedriveAsync(Guid messageId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Re-enqueue every dead-lettered message matching <paramref name="filter"/> back into the
    /// active outbox, in bounded batches, and remove each from the dead-letter store. Used to
    /// recover a whole class of failures (for example every message of one type dead-lettered
    /// during a downstream outage) once the cause is resolved.
    /// </summary>
    /// <param name="filter">Selects which dead-lettered messages to redrive. <see cref="RedriveFilter.All"/> redrives the entire store.</param>
    /// <param name="batchSize">
    /// Maximum messages moved per batch, so a large backlog drains over several short transactions
    /// rather than one long lock. Must be positive.
    /// </param>
    /// <param name="cancellationToken">Cancellation token. The operation is resumable: a cancelled or failed bulk redrive leaves already-moved messages re-enqueued and the rest in the dead-letter store.</param>
    /// <returns>The aggregate <see cref="RedriveResult"/> across every batch: total re-enqueued and total skipped.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="batchSize"/> is not positive.</exception>
    Task<RedriveResult> RedriveAsync(RedriveFilter filter, int batchSize, CancellationToken cancellationToken = default);
}
