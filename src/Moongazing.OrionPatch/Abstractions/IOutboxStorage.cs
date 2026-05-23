using Moongazing.OrionPatch.Models;

namespace Moongazing.OrionPatch.Abstractions;

/// <summary>
/// Storage SPI for outbox rows. Implementations: <c>OrionPatch.EntityFrameworkCore</c>
/// (production), <c>OrionPatch.Testing</c> (in-memory).
/// </summary>
public interface IOutboxStorage
{
    /// <summary>Persist a batch of new rows. Called by the storage-bound IOutbox during SaveChanges.</summary>
    /// <param name="rows">Rows to append.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task AppendAsync(IReadOnlyList<OutboxRow> rows, CancellationToken cancellationToken = default);

    /// <summary>
    /// Atomically claim up to <paramref name="batchSize"/> pending or lease-expired rows
    /// under the dispatcher's identity for the given lease duration. Returns the rows
    /// the caller now owns.
    /// </summary>
    /// <param name="batchSize">Maximum number of rows to claim in this call.</param>
    /// <param name="dispatcherIdentity">Stable identity of the dispatcher attempting the claim; stored in <see cref="OutboxRow.ClaimedBy"/>.</param>
    /// <param name="leaseDuration">How long the claim is valid; lease expires at ClaimedAtUtc + this duration.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<IReadOnlyList<OutboxRow>> ClaimNextAsync(int batchSize, string dispatcherIdentity, TimeSpan leaseDuration, CancellationToken cancellationToken = default);

    /// <summary>Mark a claimed row as <see cref="OutboxStatus.Processed"/>.</summary>
    /// <param name="rowId">Id of the row to complete.</param>
    /// <param name="processedAtUtc">UTC timestamp recorded as <see cref="OutboxRow.ProcessedAtUtc"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task CompleteAsync(Guid rowId, DateTime processedAtUtc, CancellationToken cancellationToken = default);

    /// <summary>
    /// Record a failed dispatch attempt that should be retried. The row returns to
    /// <see cref="OutboxStatus.Pending"/> and its <c>NextAttemptAtUtc</c> is set to the
    /// supplied UTC anchor.
    /// </summary>
    /// <param name="rowId">Id of the row whose attempt failed.</param>
    /// <param name="errorMessage">Truncated error text written to the row's <c>LastError</c> column.</param>
    /// <param name="nextAttemptAtUtc">Earliest UTC at which the next dispatch attempt should run.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    Task FailAsync(Guid rowId, string errorMessage, DateTime nextAttemptAtUtc, CancellationToken cancellationToken = default);

    /// <summary>
    /// Record a terminal dispatch failure: the row's <see cref="OutboxStatus"/> flips to
    /// <see cref="OutboxStatus.DeadLettered"/> and no further attempts are made until an
    /// operator resets the row.
    /// </summary>
    /// <param name="rowId">Id of the row that exceeded the retry budget.</param>
    /// <param name="errorMessage">Truncated error text written to the row's <c>LastError</c> column.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    Task DeadLetterAsync(Guid rowId, string errorMessage, CancellationToken cancellationToken = default);

    /// <summary>Count rows currently in <see cref="OutboxStatus.Pending"/>. Used by the queue-depth metric.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<long> QueueDepthAsync(CancellationToken cancellationToken = default);
}
