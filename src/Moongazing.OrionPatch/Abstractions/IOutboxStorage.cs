using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
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
    /// <param name="ct">Cancellation token.</param>
    Task AppendAsync(IReadOnlyList<OutboxRow> rows, CancellationToken ct);

    /// <summary>
    /// Atomically claim up to <paramref name="batchSize"/> pending or lease-expired rows
    /// under the dispatcher's identity for the given lease duration. Returns the rows
    /// the caller now owns.
    /// </summary>
    /// <param name="batchSize">Maximum number of rows to claim in this call.</param>
    /// <param name="dispatcherIdentity">Stable identity of the dispatcher attempting the claim; stored in <see cref="OutboxRow.ClaimedBy"/>.</param>
    /// <param name="leaseDuration">How long the claim is valid; lease expires at ClaimedAtUtc + this duration.</param>
    /// <param name="ct">Cancellation token.</param>
    Task<IReadOnlyList<OutboxRow>> ClaimNextAsync(int batchSize, string dispatcherIdentity, TimeSpan leaseDuration, CancellationToken ct);

    /// <summary>Mark a claimed row as <see cref="OutboxStatus.Processed"/>.</summary>
    /// <param name="rowId">Id of the row to complete.</param>
    /// <param name="processedAtUtc">UTC timestamp recorded as <see cref="OutboxRow.ProcessedAtUtc"/>.</param>
    /// <param name="ct">Cancellation token.</param>
    Task CompleteAsync(Guid rowId, DateTime processedAtUtc, CancellationToken ct);

    /// <summary>
    /// Record a failed dispatch attempt. If <paramref name="deadLetter"/> is true,
    /// the row's <see cref="OutboxStatus"/> flips to <see cref="OutboxStatus.DeadLettered"/>;
    /// otherwise it returns to <see cref="OutboxStatus.Pending"/> with the next-attempt anchor set.
    /// </summary>
    /// <param name="rowId">Id of the row that failed to dispatch.</param>
    /// <param name="errorMessage">Truncated error text to persist on the row.</param>
    /// <param name="nextAttemptAtUtc">Earliest UTC at which the row may be re-claimed; ignored when <paramref name="deadLetter"/> is true.</param>
    /// <param name="deadLetter">When true, the row is moved to <see cref="OutboxStatus.DeadLettered"/>; otherwise returned to <see cref="OutboxStatus.Pending"/>.</param>
    /// <param name="ct">Cancellation token.</param>
    Task FailAsync(Guid rowId, string errorMessage, DateTime? nextAttemptAtUtc, bool deadLetter, CancellationToken ct);

    /// <summary>Count rows currently in <see cref="OutboxStatus.Pending"/>. Used by the queue-depth metric.</summary>
    /// <param name="ct">Cancellation token.</param>
    Task<long> QueueDepthAsync(CancellationToken ct);
}
