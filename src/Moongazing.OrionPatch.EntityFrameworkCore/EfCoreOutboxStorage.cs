namespace Moongazing.OrionPatch.EntityFrameworkCore;

using Microsoft.EntityFrameworkCore;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.EntityFrameworkCore.Claims;
using Moongazing.OrionPatch.Models;

/// <summary>
/// EF Core-backed <see cref="IOutboxStorage"/>. Claim semantics are delegated to a
/// provider-aware <see cref="IClaimStrategy"/> picked via
/// <see cref="ProviderClaimStrategy.For"/>; Complete / Fail / DeadLetter mutations run
/// through <see cref="EntityFrameworkQueryableExtensions"/>'s
/// <c>ExecuteUpdateAsync</c> for single-round-trip atomicity. <c>QueueDepthAsync</c>
/// is a no-tracking <c>COUNT</c>.
/// </summary>
/// <remarks>
/// <para>
/// This type is registered per-scope alongside the consumer's <see cref="DbContext"/>;
/// the dispatcher hosted service resolves a fresh storage per claim cycle. EF Core's
/// thread-affinity restriction applies — do not invoke any of the lifecycle methods
/// concurrently on the same <see cref="DbContext"/>.
/// </para>
/// <para>
/// Complete/Fail/DeadLetter mutations use <c>ExecuteUpdateAsync</c>, which bypasses
/// EF Core's change tracker. The storage class itself never tracks <see cref="Models.OutboxRow"/>
/// (every read uses <c>AsNoTracking()</c>), so the library is self-consistent. If a consumer
/// mixes their own tracked <c>OutboxRow</c> queries against the same DbContext, those entities
/// will become stale after any storage write.
/// </para>
/// <para>
/// This type does not emit telemetry. <see cref="Hosting.OutboxDispatcherHostedService"/>
/// instruments <see cref="Abstractions.IOutboxStorage"/> calls externally; storage stays
/// transparent to keep the per-operation cost predictable and to avoid double-counting.
/// </para>
/// </remarks>
public sealed class EfCoreOutboxStorage : IOutboxStorage
{
    private readonly DbContext db;
    private readonly IClaimStrategy claimStrategy;

    /// <summary>
    /// Create the storage bound to a specific <see cref="DbContext"/>. The claim strategy
    /// is auto-selected by <see cref="ProviderClaimStrategy.For"/> from the DbContext's
    /// database provider.
    /// </summary>
    /// <param name="db">DbContext used for all storage operations; must be non-null.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="db"/> is null.</exception>
    public EfCoreOutboxStorage(DbContext db)
        : this(db, ProviderClaimStrategy.For(db ?? throw new ArgumentNullException(nameof(db))))
    {
    }

    /// <summary>
    /// Test-seam constructor that takes an explicit claim strategy. Used by the unit-test
    /// project to inject the same strategy <see cref="ProviderClaimStrategy.For"/> would
    /// have resolved for the bound DbContext, without going through the static resolver.
    /// </summary>
    /// <param name="db">DbContext used for all storage operations; must be non-null.</param>
    /// <param name="claimStrategy">Provider-aware claim strategy; must be non-null.</param>
    /// <exception cref="ArgumentNullException">Thrown when any argument is null.</exception>
    internal EfCoreOutboxStorage(DbContext db, IClaimStrategy claimStrategy)
    {
        ArgumentNullException.ThrowIfNull(db);
        ArgumentNullException.ThrowIfNull(claimStrategy);
        this.db = db;
        this.claimStrategy = claimStrategy;
    }

    /// <inheritdoc/>
    public async Task AppendAsync(IReadOnlyList<OutboxRow> rows, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rows);
        if (rows.Count == 0)
        {
            return;
        }
        db.AddRange(rows);
        await db.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public Task<IReadOnlyList<OutboxRow>> ClaimNextAsync(int batchSize, string dispatcherIdentity, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(dispatcherIdentity);
        return claimStrategy.ClaimNextAsync(db, batchSize, dispatcherIdentity, leaseDuration, DateTime.UtcNow, cancellationToken);
    }

    /// <inheritdoc/>
    public async Task CompleteAsync(Guid rowId, DateTime processedAtUtc, CancellationToken cancellationToken = default)
    {
        await db.Set<OutboxRow>()
            .Where(r => r.Id == rowId)
            .ExecuteUpdateAsync(s => s
                .SetProperty(r => r.Status, OutboxStatus.Processed)
                .SetProperty(r => r.ProcessedAtUtc, (DateTime?)processedAtUtc)
                .SetProperty(r => r.ClaimedAtUtc, (DateTime?)null)
                .SetProperty(r => r.ClaimedBy, (string?)null),
                cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task FailAsync(Guid rowId, string errorMessage, DateTime nextAttemptAtUtc, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(errorMessage);
        await db.Set<OutboxRow>()
            .Where(r => r.Id == rowId)
            .ExecuteUpdateAsync(s => s
                .SetProperty(r => r.Status, OutboxStatus.Pending)
                .SetProperty(r => r.AttemptCount, r => r.AttemptCount + 1)
                .SetProperty(r => r.LastError, (string?)errorMessage)
                .SetProperty(r => r.NextAttemptAtUtc, (DateTime?)nextAttemptAtUtc)
                .SetProperty(r => r.ClaimedAtUtc, (DateTime?)null)
                .SetProperty(r => r.ClaimedBy, (string?)null),
                cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task DeadLetterAsync(Guid rowId, string errorMessage, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(errorMessage);
        await db.Set<OutboxRow>()
            .Where(r => r.Id == rowId)
            .ExecuteUpdateAsync(s => s
                .SetProperty(r => r.Status, OutboxStatus.DeadLettered)
                .SetProperty(r => r.AttemptCount, r => r.AttemptCount + 1)
                .SetProperty(r => r.LastError, (string?)errorMessage)
                .SetProperty(r => r.ClaimedAtUtc, (DateTime?)null)
                .SetProperty(r => r.ClaimedBy, (string?)null),
                cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task<long> QueueDepthAsync(CancellationToken cancellationToken = default)
    {
        return await db.Set<OutboxRow>()
            .AsNoTracking()
            .Where(r => r.Status == OutboxStatus.Pending)
            .LongCountAsync(cancellationToken).ConfigureAwait(false);
    }
}
