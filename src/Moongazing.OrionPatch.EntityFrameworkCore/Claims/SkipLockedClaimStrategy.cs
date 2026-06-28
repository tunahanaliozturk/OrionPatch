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
/// Production-provider claim strategy targeting databases with <c>SKIP LOCKED</c>-style row
/// locking. Selected by <see cref="ProviderClaimStrategy.For"/> for SQL Server, PostgreSQL, and
/// MySQL connections.
/// </summary>
/// <remarks>
/// <para>
/// <b>v0.4.0:</b> the claim is now genuinely provider-native. Each dialect issues a real atomic
/// lock-and-claim statement (<c>FOR UPDATE SKIP LOCKED</c> on PostgreSQL / MySQL,
/// <c>WITH (UPDLOCK, READPAST, ROWLOCK)</c> on SQL Server) through
/// <see cref="NativeSkipLockedClaimStrategy"/>, so high-contention multi-dispatcher deployments no
/// longer pay the portable compare-and-swap fallback's per-loser no-op round-trip. Two competing
/// dispatchers never claim the same row: a row another dispatcher has locked is skipped, not
/// blocked on. The portable <see cref="CompareAndSwapClaimStrategy"/> remains the path for SQLite
/// and unrecognized providers (it is not wired in here; <see cref="ProviderClaimStrategy.For"/>
/// routes those providers to it directly).
/// </para>
/// </remarks>
/// <param name="dialect">SQL dialect this strategy targets.</param>
internal sealed class SkipLockedClaimStrategy(SqlDialect dialect) : IClaimStrategy
{
    private readonly NativeSkipLockedClaimStrategy native = new(dialect);

    /// <summary>Dialect this strategy was constructed with.</summary>
    internal SqlDialect Dialect => dialect;

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
        => native.ClaimNextAsync(db, batchSize, dispatcherIdentity, leaseDuration, utcNow, cancellationToken);
}
