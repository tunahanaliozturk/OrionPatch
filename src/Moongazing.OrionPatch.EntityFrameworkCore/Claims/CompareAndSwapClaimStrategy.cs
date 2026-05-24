namespace Moongazing.OrionPatch.EntityFrameworkCore.Claims;

using Microsoft.EntityFrameworkCore;
using Moongazing.OrionPatch.Models;

/// <summary>
/// Provider-agnostic compare-and-swap claim. Selects candidate row ids first, then
/// attempts a single <c>UPDATE … WHERE</c> per candidate that succeeds only if the row
/// is still in the expected state. Rows that lost the race are skipped. Used on SQLite
/// (no <c>SKIP LOCKED</c> support and decorative <c>RowVersion</c>) and as the safe
/// default for unrecognized providers.
/// </summary>
internal sealed class CompareAndSwapClaimStrategy : IClaimStrategy
{
    /// <inheritdoc/>
    /// <param name="db">DbContext bound to the consumer's connection; must be non-null.</param>
    /// <param name="batchSize">Maximum rows to claim; must be positive.</param>
    /// <param name="dispatcherIdentity">Identity string written to <see cref="OutboxRow.ClaimedBy"/>; must be non-empty.</param>
    /// <param name="leaseDuration">Lease window; expired claims are eligible to be stolen.</param>
    /// <param name="utcNow">Current UTC used to compute lease expiry.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    public async Task<IReadOnlyList<OutboxRow>> ClaimNextAsync(
        DbContext db,
        int batchSize,
        string dispatcherIdentity,
        TimeSpan leaseDuration,
        DateTime utcNow,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(db);
        ArgumentException.ThrowIfNullOrEmpty(dispatcherIdentity);

        var leaseExpiry = utcNow - leaseDuration;

        // Step 1: shortlist eligible candidate IDs in FIFO order. We over-select
        // (2x batchSize) so a few CAS losers don't starve the batch under contention.
        var candidates = await db.Set<OutboxRow>()
            .AsNoTracking()
            .Where(r =>
                (r.Status == OutboxStatus.Pending && (r.NextAttemptAtUtc == null || r.NextAttemptAtUtc <= utcNow)) ||
                (r.Status == OutboxStatus.Claimed && r.ClaimedAtUtc != null && r.ClaimedAtUtc < leaseExpiry))
            .OrderBy(r => r.EnqueuedAtUtc)
            .Take(batchSize * 2)
            .Select(r => new CandidateRef(r.Id, r.Status, r.ClaimedAtUtc))
            .ToListAsync(cancellationToken).ConfigureAwait(false);

        // Step 2: per-candidate CAS UPDATE. Stop once we've claimed batchSize rows.
        var claimedIds = new List<Guid>(batchSize);
        foreach (var c in candidates)
        {
            if (claimedIds.Count >= batchSize)
            {
                break;
            }

            int affected;
            if (c.Status == OutboxStatus.Pending)
            {
                affected = await db.Set<OutboxRow>()
                    .Where(r => r.Id == c.Id && r.Status == OutboxStatus.Pending)
                    .ExecuteUpdateAsync(s => s
                        .SetProperty(r => r.Status, OutboxStatus.Claimed)
                        .SetProperty(r => r.ClaimedAtUtc, (DateTime?)utcNow)
                        .SetProperty(r => r.ClaimedBy, (string?)dispatcherIdentity),
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            else
            {
                // Lease-expired Claimed row — CAS on the old ClaimedAtUtc so we only steal
                // if no other dispatcher has refreshed the lease in the meantime.
                var oldClaimedAt = c.ClaimedAtUtc!.Value;
                affected = await db.Set<OutboxRow>()
                    .Where(r => r.Id == c.Id && r.Status == OutboxStatus.Claimed && r.ClaimedAtUtc == oldClaimedAt)
                    .ExecuteUpdateAsync(s => s
                        .SetProperty(r => r.ClaimedAtUtc, (DateTime?)utcNow)
                        .SetProperty(r => r.ClaimedBy, (string?)dispatcherIdentity),
                        cancellationToken)
                    .ConfigureAwait(false);
            }

            if (affected == 1)
            {
                claimedIds.Add(c.Id);
            }
        }

        if (claimedIds.Count == 0)
        {
            return Array.Empty<OutboxRow>();
        }

        // Step 3: reload the claimed rows so the caller sees their post-update state.
        // Order by EnqueuedAtUtc to preserve FIFO observable order.
        var claimed = await db.Set<OutboxRow>()
            .AsNoTracking()
            .Where(r => claimedIds.Contains(r.Id))
            .OrderBy(r => r.EnqueuedAtUtc)
            .ToListAsync(cancellationToken).ConfigureAwait(false);

        return claimed;
    }

    /// <summary>Projection used by the candidate shortlist query.</summary>
    /// <param name="Id">Candidate row identifier.</param>
    /// <param name="Status">Snapshot of the candidate's status at shortlist time.</param>
    /// <param name="ClaimedAtUtc">Snapshot of the candidate's ClaimedAtUtc at shortlist time (used for lease-expiry CAS).</param>
    private sealed record CandidateRef(Guid Id, OutboxStatus Status, DateTime? ClaimedAtUtc);
}
