namespace Moongazing.OrionPatch.Tests.Models;

using Moongazing.OrionPatch.Models;
using Xunit;

public sealed class RedriveResultTests
{
    [Fact]
    public void Empty_is_zero_zero()
    {
        Assert.Equal(0, RedriveResult.Empty.Redriven);
        Assert.Equal(0, RedriveResult.Empty.Skipped);
        Assert.Equal(0, RedriveResult.Empty.Total);
    }

    [Fact]
    public void Total_sums_redriven_and_skipped()
    {
        var result = new RedriveResult(4, 3);
        Assert.Equal(7, result.Total);
    }

    [Fact]
    public void Addition_sums_componentwise()
    {
        var sum = new RedriveResult(2, 1) + new RedriveResult(3, 5);
        Assert.Equal(new RedriveResult(5, 6), sum);
    }

    [Fact]
    public void Add_matches_operator()
    {
        var a = new RedriveResult(2, 1);
        var b = new RedriveResult(3, 5);
        Assert.Equal(a + b, RedriveResult.Add(a, b));
    }

    [Fact]
    public void Empty_is_addition_identity()
    {
        var r = new RedriveResult(9, 4);
        Assert.Equal(r, r + RedriveResult.Empty);
    }
}
