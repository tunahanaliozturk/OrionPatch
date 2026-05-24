namespace Moongazing.OrionPatch.Tests.Configuration;

using Moongazing.OrionPatch.Configuration;
using Xunit;

public class OrionPatchOptionsTests
{
    [Fact]
    public void Defaults_ShouldMatchSpec_WhenConstructed()
    {
        var o = new OrionPatchOptions();
        Assert.Equal(TimeSpan.FromSeconds(1), o.PollingInterval);
        Assert.Equal(50, o.BatchSize);
        Assert.Equal(5, o.MaxAttempts);
        Assert.Equal(TimeSpan.FromMinutes(2), o.LeaseDuration);
        Assert.True(o.DispatcherEnabled);
        Assert.NotNull(o.BackoffStrategy);
        Assert.Equal(TimeSpan.FromSeconds(1), o.BackoffStrategy(1));
        Assert.NotNull(o.DispatcherIdentityFactory);
        Assert.NotNull(o.JsonOptions);
    }

    [Fact]
    public void DispatcherIdentityFactory_ShouldReturnNonEmpty_WhenInvoked()
    {
        var o = new OrionPatchOptions();
        var id = o.DispatcherIdentityFactory();
        Assert.False(string.IsNullOrWhiteSpace(id));
    }
}
