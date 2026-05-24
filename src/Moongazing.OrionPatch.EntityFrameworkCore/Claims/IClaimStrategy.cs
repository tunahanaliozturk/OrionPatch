namespace Moongazing.OrionPatch.EntityFrameworkCore.Claims;

using Microsoft.EntityFrameworkCore;
using Moongazing.OrionPatch.Models;

/// <summary>
/// Provider-specific atomic claim strategy. Implementations select up to <c>batchSize</c>
/// eligible rows, flip their status to <see cref="OutboxStatus.Claimed"/> under the
/// supplied dispatcher identity and lease, and return the claimed rows.
/// </summary>
internal interface IClaimStrategy
{
    /// <summary>
    /// Atomically claim up to <paramref name="batchSize"/> rows.
    /// </summary>
    /// <param name="db">DbContext bound to the consumer's connection; must be non-null.</param>
    /// <param name="batchSize">Maximum rows to claim; must be positive.</param>
    /// <param name="dispatcherIdentity">Identity string written to <see cref="OutboxRow.ClaimedBy"/>; must be non-empty.</param>
    /// <param name="leaseDuration">Lease window; expired claims are eligible to be stolen.</param>
    /// <param name="utcNow">Current UTC; supplied so the strategy can compute lease-expiry consistently.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>The rows the caller now owns, freshly reloaded so their lifecycle fields reflect the post-claim state.</returns>
    Task<IReadOnlyList<OutboxRow>> ClaimNextAsync(
        DbContext db,
        int batchSize,
        string dispatcherIdentity,
        TimeSpan leaseDuration,
        DateTime utcNow,
        CancellationToken cancellationToken = default);
}
