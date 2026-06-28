namespace Moongazing.OrionPatch.EntityFrameworkCore.Tests.Native;

using Xunit;

/// <summary>
/// Native <c>WITH (UPDLOCK, READPAST, ROWLOCK)</c> claim against a real SQL Server container. Skips
/// with a clear message when Docker is unavailable (the container could not start).
/// </summary>
[Collection(SqlServerClaimGroup.Name)]
public sealed class SqlServerNativeClaimTests(SqlServerFixture fixture)
{
    [SkippableFact]
    public async Task NativeClaim_HandsEachRowToExactlyOneClaimer_UnderContention()
    {
        Skip.If(fixture.SkipReason is not null, fixture.SkipReason);

        await NativeClaimConcurrency.AssertExclusiveClaimAsync(
            () => new NativeClaimDbContext(fixture.NewOptions()));
    }
}
