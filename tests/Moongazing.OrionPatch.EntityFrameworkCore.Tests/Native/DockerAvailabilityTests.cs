namespace Moongazing.OrionPatch.EntityFrameworkCore.Tests.Native;

using System;
using System.Threading.Tasks;
using Xunit;

/// <summary>
/// Tests the honesty contract of <see cref="DockerAvailability"/>: when Docker is available a setup
/// failure must propagate (fail the suite), never be swallowed into a skip. The previous fixtures
/// turned every exception into a skip, which would hide a broken native-claim suite behind a
/// "Docker not running" message on CI.
/// </summary>
public sealed class DockerAvailabilityTests
{
    [SkippableFact]
    public async Task SetupFailureWhenDockerAvailable_FailsAndDoesNotSkip()
    {
        // This assertion only has meaning when Docker is actually up - that is the regime the contract
        // protects. With Docker down the helper correctly returns a skip reason, which is the other
        // half of the contract but not what this test pins.
        var boom = new InvalidOperationException("schema creation failed");
        var startRan = false;

        string? skip = null;
        InvalidOperationException? thrown = null;
        try
        {
            skip = await DockerAvailability.StartOrSkipAsync(
                startAsync: () =>
                {
                    startRan = true;
                    return Task.CompletedTask;
                },
                setupAsync: () => throw boom);
        }
        catch (InvalidOperationException ex)
        {
            thrown = ex;
        }

        Skip.If(!startRan && skip is not null, "Docker is unavailable; setup-propagation contract is not exercised.");

        // Docker was available (start ran), so the setup failure must have propagated unchanged - not
        // been converted to a skip.
        Assert.Null(skip);
        Assert.Same(boom, thrown);
    }

    [Fact]
    public async Task NullDelegates_AreRejected()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            DockerAvailability.StartOrSkipAsync(null!, () => Task.CompletedTask));
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            DockerAvailability.StartOrSkipAsync(() => Task.CompletedTask, null!));
    }
}
