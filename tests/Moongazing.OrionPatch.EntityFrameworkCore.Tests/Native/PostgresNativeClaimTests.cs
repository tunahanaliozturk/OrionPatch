namespace Moongazing.OrionPatch.EntityFrameworkCore.Tests.Native;

using Xunit;

/// <summary>
/// Native <c>FOR UPDATE SKIP LOCKED</c> claim against a real PostgreSQL container. Skips with a
/// clear message when Docker is unavailable (the container could not start).
/// </summary>
[Collection(PostgresClaimGroup.Name)]
public sealed class PostgresNativeClaimTests(PostgresFixture fixture)
{
    [SkippableFact]
    public async Task NativeClaim_HandsEachRowToExactlyOneClaimer_UnderContention()
    {
        Skip.If(fixture.SkipReason is not null, fixture.SkipReason);

        await NativeClaimConcurrency.AssertExclusiveClaimAsync(
            () => new NativeClaimDbContext(fixture.NewOptions()));
    }
}
