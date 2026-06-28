namespace Moongazing.OrionPatch.EntityFrameworkCore.Tests.Native;

using Xunit;

/// <summary>
/// Native <c>WITH (UPDLOCK, READPAST, ROWLOCK, READCOMMITTEDLOCK)</c> claim against a real SQL Server
/// container whose database has <c>READ_COMMITTED_SNAPSHOT</c> turned ON (see
/// <see cref="SqlServerFixture"/>). Under RCSI a naive <c>READPAST</c> would read through row
/// versions and silently fail to skip locked rows, so two dispatchers could double-claim. This test
/// is the regression guard for that: it races concurrent claimers under RCSI and asserts every row
/// goes to exactly one of them. Skips only when Docker itself is unavailable.
/// </summary>
[Collection(SqlServerClaimGroup.Name)]
public sealed class SqlServerNativeClaimTests(SqlServerFixture fixture)
{
    [SkippableFact]
    public async Task NativeClaim_HandsEachRowToExactlyOneClaimer_UnderContention_OnRcsiDatabase()
    {
        Skip.If(fixture.SkipReason is not null, fixture.SkipReason);

        await NativeClaimConcurrency.AssertExclusiveClaimAsync(
            () => new NativeClaimDbContext(fixture.NewOptions()));
    }
}
