namespace Moongazing.OrionPatch.Tests.Configuration;

using Moongazing.OrionPatch.Configuration;
using Xunit;

public class BackoffStrategyTests
{
    [Fact]
    public void Exponential_ShouldDoubleEachAttempt_UntilMaxCap()
    {
        var b = BackoffStrategy.Exponential(initial: TimeSpan.FromSeconds(1), max: TimeSpan.FromSeconds(30));
        Assert.Equal(TimeSpan.FromSeconds(1), b(1));
        Assert.Equal(TimeSpan.FromSeconds(2), b(2));
        Assert.Equal(TimeSpan.FromSeconds(4), b(3));
        Assert.Equal(TimeSpan.FromSeconds(8), b(4));
        Assert.Equal(TimeSpan.FromSeconds(16), b(5));
        Assert.Equal(TimeSpan.FromSeconds(30), b(6));
        Assert.Equal(TimeSpan.FromSeconds(30), b(20));
    }

    [Fact]
    public void Exponential_ShouldReturnZero_WhenAttemptIsNonPositive()
    {
        var b = BackoffStrategy.Exponential(TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1));
        Assert.Equal(TimeSpan.Zero, b(0));
        Assert.Equal(TimeSpan.Zero, b(-1));
    }

    [Fact]
    public void Fixed_ShouldReturnSameDelay_ForEveryAttempt()
    {
        var b = BackoffStrategy.Fixed(TimeSpan.FromSeconds(2));
        Assert.Equal(TimeSpan.FromSeconds(2), b(1));
        Assert.Equal(TimeSpan.FromSeconds(2), b(99));
    }
}
