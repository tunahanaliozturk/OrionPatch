namespace Moongazing.OrionPatch.Testing.Tests;

using Microsoft.Extensions.DependencyInjection;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.DependencyInjection;
using Moongazing.OrionPatch.Testing.DependencyInjection;
using Xunit;

public class UseInMemoryTests
{
    [Fact]
    public void UseInMemory_ShouldRegisterInMemoryStorageAsSingleton_WhenCalled()
    {
        var services = new ServiceCollection();
        services.AddOrionPatch().UseInMemory();

        var sp = services.BuildServiceProvider();
        var storage = sp.GetRequiredService<IOutboxStorage>();
        var concrete = sp.GetRequiredService<InMemoryOutboxStorage>();

        Assert.IsType<InMemoryOutboxStorage>(storage);
        Assert.Same(storage, concrete);
    }

    [Fact]
    public void UseInMemory_ShouldRegisterInMemoryOutboxAsSingleton_WhenCalled()
    {
        var services = new ServiceCollection();
        services.AddOrionPatch().UseInMemory();

        var sp = services.BuildServiceProvider();
        var outbox = sp.GetRequiredService<IOutbox>();
        var concrete = sp.GetRequiredService<InMemoryOutbox>();

        Assert.IsType<InMemoryOutbox>(outbox);
        Assert.Same(outbox, concrete);
    }

    [Fact]
    public void UseInMemory_ShouldThrow_WhenBuilderIsNull()
    {
        OrionPatchBuilder builder = null!;
        Assert.Throws<ArgumentNullException>(() => builder.UseInMemory());
    }

    [Fact]
    public void UseInMemory_ShouldReplacePriorStorageRegistrations_WhenCalledTwice()
    {
        var services = new ServiceCollection();
        services.AddOrionPatch()
            .UseInMemory()
            .UseInMemory();

        var sp = services.BuildServiceProvider();
        var all = sp.GetServices<IOutboxStorage>().ToList();

        Assert.Single(all);
    }
}
