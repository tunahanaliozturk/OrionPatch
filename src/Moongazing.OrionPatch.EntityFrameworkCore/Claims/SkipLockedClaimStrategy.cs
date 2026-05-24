namespace Moongazing.OrionPatch.EntityFrameworkCore.Claims;

using Microsoft.EntityFrameworkCore;
using Moongazing.OrionPatch.Models;

/// <summary>
/// Provider dialect for <see cref="SkipLockedClaimStrategy"/>.
/// </summary>
internal enum SqlDialect
{
    /// <summary>Microsoft SQL Server (2008+); uses <c>WITH (UPDLOCK, READPAST, ROWLOCK)</c> and <c>OUTPUT inserted.*</c>.</summary>
    SqlServer,
    /// <summary>PostgreSQL (9.5+); uses <c>FOR UPDATE SKIP LOCKED</c> and <c>RETURNING</c>.</summary>
    PostgreSql,
    /// <summary>MySQL (8.0+) / MariaDB (10.6+); uses <c>FOR UPDATE SKIP LOCKED</c>.</summary>
    MySql,
}

/// <summary>
/// Production-provider claim strategy targeting databases with <c>SKIP LOCKED</c>-style
/// row locking. Selected by <see cref="ProviderClaimStrategy.For"/> for SQL Server,
/// PostgreSQL, and MySQL connections.
/// </summary>
/// <remarks>
/// <para>
/// <b>v0.1.0 status:</b> the dialect enum + provider routing are in place, but all three
/// dialects currently delegate to <see cref="CompareAndSwapClaimStrategy"/>. CompareAndSwap
/// is correct under contention — every claim is an <c>UPDATE … WHERE</c> that succeeds only
/// if the row is still in its expected state, so concurrent dispatchers cannot both win the
/// same row — it is simply chattier than true <c>FOR UPDATE SKIP LOCKED</c> because race
/// losers consume a no-op round-trip.
/// </para>
/// <para>
/// <b>TODO v0.2:</b> replace the per-dialect delegation with native SQL:
/// <list type="bullet">
///   <item><description>PostgreSQL: <c>WITH eligible AS (SELECT "Id" FROM "OrionPatch_Outbox" WHERE … ORDER BY "EnqueuedAtUtc" LIMIT @batchSize FOR UPDATE SKIP LOCKED) UPDATE "OrionPatch_Outbox" SET … WHERE "Id" IN (SELECT "Id" FROM eligible) RETURNING "Id"</c>.</description></item>
///   <item><description>SQL Server: <c>UPDATE TOP (@batchSize) o WITH (UPDLOCK, READPAST, ROWLOCK) SET … OUTPUT inserted.[Id] WHERE …</c>.</description></item>
///   <item><description>MySQL 8: two-statement under explicit transaction — <c>SELECT … FOR UPDATE SKIP LOCKED</c> then <c>UPDATE … WHERE Id IN (…)</c>.</description></item>
/// </list>
/// The v0.2 work also needs real-provider integration tests (Testcontainers) before the
/// native SQL paths can be claimed safe.
/// </para>
/// </remarks>
/// <param name="dialect">SQL dialect this strategy targets.</param>
internal sealed class SkipLockedClaimStrategy(SqlDialect dialect) : IClaimStrategy
{
    private readonly CompareAndSwapClaimStrategy fallback = new();

    /// <summary>Dialect this strategy was constructed with.</summary>
    public SqlDialect Dialect => dialect;

    /// <inheritdoc/>
    /// <param name="db">DbContext bound to the consumer's connection; must be non-null.</param>
    /// <param name="batchSize">Maximum rows to claim; must be positive.</param>
    /// <param name="dispatcherIdentity">Identity string written to <see cref="OutboxRow.ClaimedBy"/>; must be non-empty.</param>
    /// <param name="leaseDuration">Lease window; expired claims are eligible to be stolen.</param>
    /// <param name="utcNow">Current UTC used to compute lease expiry.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    public Task<IReadOnlyList<OutboxRow>> ClaimNextAsync(
        DbContext db,
        int batchSize,
        string dispatcherIdentity,
        TimeSpan leaseDuration,
        DateTime utcNow,
        CancellationToken cancellationToken = default)
    {
        // TODO v0.2: dispatch on `dialect` to dialect-specific raw SQL with parameterized
        // FOR UPDATE SKIP LOCKED / OUTPUT clauses (see remarks on the class). Until then,
        // CompareAndSwap is the safe and correct path.
        _ = dialect;
        return fallback.ClaimNextAsync(db, batchSize, dispatcherIdentity, leaseDuration, utcNow, cancellationToken);
    }
}
