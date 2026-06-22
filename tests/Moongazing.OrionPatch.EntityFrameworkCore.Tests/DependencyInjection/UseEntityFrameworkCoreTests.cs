namespace Moongazing.OrionPatch.EntityFrameworkCore.Tests.DependencyInjection;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.DependencyInjection;
using Moongazing.OrionPatch.EntityFrameworkCore;
using Moongazing.OrionPatch.EntityFrameworkCore.DependencyInjection;
using Xunit;

public class UseEntityFrameworkCoreTests
{
    [Fact]
    public void UseEntityFrameworkCore_ShouldRegisterIOutboxAsScoped_WhenCalled()
    {
        using var provider = BuildProvider();
        using var scope = provider.CreateScope();

        var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();

        Assert.IsType<EfCoreOutbox>(outbox);

        using var scope2 = provider.CreateScope();
        var outbox2 = scope2.ServiceProvider.GetRequiredService<IOutbox>();
        Assert.NotSame(outbox, outbox2);
    }

    [Fact]
    public void UseEntityFrameworkCore_ShouldRegisterIOutboxStorageAsScoped_WhenCalled()
    {
        using var provider = BuildProvider();
        using var scope = provider.CreateScope();

        var storage = scope.ServiceProvider.GetRequiredService<IOutboxStorage>();

        Assert.IsType<EfCoreOutboxStorage>(storage);

        using var scope2 = provider.CreateScope();
        var storage2 = scope2.ServiceProvider.GetRequiredService<IOutboxStorage>();
        Assert.NotSame(storage, storage2);
    }

    [Fact]
    public void UseEntityFrameworkCore_ShouldExposeStorageAsDeadLetterAndArchivalStore_OnSameInstance()
    {
        using var provider = BuildProvider();
        using var scope = provider.CreateScope();

        var storage = scope.ServiceProvider.GetRequiredService<IOutboxStorage>();
        var deadLetter = scope.ServiceProvider.GetRequiredService<IDeadLetterStore>();
        var archival = scope.ServiceProvider.GetRequiredService<IOutboxArchivalStore>();

        Assert.IsType<EfCoreOutboxStorage>(deadLetter);
        Assert.IsType<EfCoreOutboxStorage>(archival);
        // All three abstractions resolve to the one per-scope storage so the dead-letter move and
        // the reap run against the same bound DbContext as the outbox lifecycle.
        Assert.Same(storage, deadLetter);
        Assert.Same(storage, archival);
    }

    [Fact]
    public void UseEntityFrameworkCore_PurgeOverload_ShouldReturnSameBuilder_WhenCalled()
    {
        var services = new ServiceCollection();
        var builder = services.AddOrionPatch();

        var returned = builder.UseEntityFrameworkCore<DiAppDbContext>(purgeOnArchive: true);

        Assert.Same(builder, returned);
    }

    [Fact]
    public void UseEntityFrameworkCore_PurgeOverload_ShouldThrow_WhenBuilderIsNull()
    {
        OrionPatchBuilder builder = null!;

        Assert.Throws<ArgumentNullException>(() => builder.UseEntityFrameworkCore<DiAppDbContext>(purgeOnArchive: true));
    }

    [Fact]
    public void UseEntityFrameworkCore_ShouldRegisterInterceptorAsSingleton_WhenCalled()
    {
        using var provider = BuildProvider();

        var a = provider.GetRequiredService<OrionPatchSaveChangesInterceptor>();
        var b = provider.GetRequiredService<OrionPatchSaveChangesInterceptor>();

        Assert.Same(a, b);
    }

    [Fact]
    public void UseEntityFrameworkCore_ShouldReturnSameBuilder_WhenCalled()
    {
        var services = new ServiceCollection();
        var builder = services.AddOrionPatch();

        var returned = builder.UseEntityFrameworkCore<DiAppDbContext>();

        Assert.Same(builder, returned);
    }

    [Fact]
    public void UseEntityFrameworkCore_ShouldThrow_WhenBuilderIsNull()
    {
        OrionPatchBuilder builder = null!;

        Assert.Throws<ArgumentNullException>(() => builder.UseEntityFrameworkCore<DiAppDbContext>());
    }

    [Fact]
    public void UseOrionPatch_OnDbContextOptionsBuilder_ShouldReturnSameBuilder_WhenCalled()
    {
        var services = new ServiceCollection();
        services.AddOrionPatch().UseEntityFrameworkCore<DiAppDbContext>();
        using var provider = services.BuildServiceProvider();

        var optionsBuilder = new DbContextOptionsBuilder<DiAppDbContext>()
            .UseSqlite("DataSource=:memory:");

        var returned = optionsBuilder.UseOrionPatch(provider);

        Assert.Same(optionsBuilder, returned);
    }

    private static ServiceProvider BuildProvider()
    {
        var services = new ServiceCollection();
        services.AddDbContext<DiAppDbContext>(options => options.UseSqlite("DataSource=:memory:"));
        services.AddOrionPatch()
            .UseEntityFrameworkCore<DiAppDbContext>()
            .UseChannelSink();
        return services.BuildServiceProvider();
    }
}

internal sealed class DiAppDbContext(DbContextOptions<DiAppDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyOrionPatchConfiguration();
    }
}
