namespace Moongazing.OrionPatch.Tests.Internal;

using Moongazing.OrionPatch.Internal;
using Moongazing.OrionPatch.Models;
using Xunit;

public class MessageTypeNameResolverTests
{
    private sealed class SampleEvent { }

    [Fact]
    public void Resolve_ShouldReturnFullName_WhenNoOverride()
    {
        var r = new MessageTypeNameResolver();
        Assert.Equal(typeof(SampleEvent).FullName, r.Resolve(typeof(SampleEvent), options: null));
    }

    [Fact]
    public void Resolve_ShouldReturnOverride_WhenSet()
    {
        var r = new MessageTypeNameResolver();
        var opts = new OutboxEnqueueOptions { MessageType = "App.Custom" };
        Assert.Equal("App.Custom", r.Resolve(typeof(SampleEvent), opts));
    }

    [Fact]
    public void Resolve_ShouldReturnTypeName_WhenFullNameIsNull()
    {
        // open generics have FullName == null; ensure we don't NRE
        var r = new MessageTypeNameResolver();
        var openGeneric = typeof(List<>);
        var name = r.Resolve(openGeneric, options: null);
        Assert.False(string.IsNullOrEmpty(name));
    }
}
