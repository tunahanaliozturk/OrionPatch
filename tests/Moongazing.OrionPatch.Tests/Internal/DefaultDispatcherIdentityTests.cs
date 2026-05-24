namespace Moongazing.OrionPatch.Tests.Internal;

using Moongazing.OrionPatch.Internal;
using Xunit;

public class DefaultDispatcherIdentityTests
{
    [Fact]
    public void Create_ShouldReturnMachineSlashProcessId_WhenInvoked()
    {
        var id = DefaultDispatcherIdentity.Create();
        Assert.Contains("/", id);
        var parts = id.Split('/');
        Assert.Equal(2, parts.Length);
        Assert.False(string.IsNullOrWhiteSpace(parts[0]));
        Assert.True(int.TryParse(parts[1], out _));
    }
}
