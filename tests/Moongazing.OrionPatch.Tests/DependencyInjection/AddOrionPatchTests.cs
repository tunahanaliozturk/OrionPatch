namespace Moongazing.OrionPatch.Tests.DependencyInjection;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Channels;
using Moongazing.OrionPatch.Configuration;
using Moongazing.OrionPatch.DependencyInjection;
using Xunit;

public class AddOrionPatchTests
{
    [Fact]
    public void AddOrionPatch_ShouldRegisterOptionsAndCoreServices_WhenCalled()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddOrionPatch();

        var sp = services.BuildServiceProvider();
        var opts = sp.GetRequiredService<IOptions<OrionPatchOptions>>().Value;

        Assert.NotNull(opts);
        Assert.NotNull(sp.GetRequiredService<IOutboxDispatcherClock>());
    }

    [Fact]
    public void AddOrionPatch_ShouldApplyConfigureDelegate_WhenSupplied()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddOrionPatch(o => o.BatchSize = 7);

        var sp = services.BuildServiceProvider();
        var opts = sp.GetRequiredService<IOptions<OrionPatchOptions>>().Value;

        Assert.Equal(7, opts.BatchSize);
    }

    [Fact]
    public void UseChannelSink_ShouldRegisterChannelSinkAsIOutboxSink_WhenCalled()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddOrionPatch().UseChannelSink(o => Assert.Equal(1000, o.Capacity));

        var sp = services.BuildServiceProvider();
        var sink = sp.GetRequiredService<IOutboxSink>();

        Assert.IsType<ChannelOutboxSink>(sink);

        // Same instance should also be resolvable as concrete ChannelOutboxSink
        // (so consumers can grab the Reader without re-binding).
        var concrete = sp.GetRequiredService<ChannelOutboxSink>();
        Assert.Same(sink, concrete);
    }

    [Fact]
    public void UseSink_ShouldRegisterCustomSink_WhenCalled()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddOrionPatch().UseSink<RecordingSink>();

        var sp = services.BuildServiceProvider();
        var sink = sp.GetRequiredService<IOutboxSink>();

        Assert.IsType<RecordingSink>(sink);
    }

    [Fact]
    public void AddOrionPatch_ShouldThrow_WhenServicesIsNull()
    {
        IServiceCollection services = null!;
        Assert.Throws<ArgumentNullException>(() => services.AddOrionPatch());
    }

    [Fact]
    public void UseSink_ShouldThrow_WhenBuilderIsNull()
    {
        OrionPatchBuilder builder = null!;
        Assert.Throws<ArgumentNullException>(() => builder.UseSink<RecordingSink>());
    }

    [Fact]
    public void UseChannelSink_ShouldThrow_WhenBuilderIsNull()
    {
        OrionPatchBuilder builder = null!;
        Assert.Throws<ArgumentNullException>(() => builder.UseChannelSink());
    }

    [Fact]
    public void UseSink_ShouldReplacePreviousSink_WhenCalledAfterUseChannelSink()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddOrionPatch()
            .UseChannelSink()
            .UseSink<RecordingSink>();

        var sp = services.BuildServiceProvider();
        var sink = sp.GetRequiredService<IOutboxSink>();
        var all = sp.GetServices<IOutboxSink>().ToList();

        Assert.IsType<RecordingSink>(sink);
        Assert.Single(all);
    }

    private sealed class RecordingSink : IOutboxSink
    {
        public Task SendAsync(Moongazing.OrionPatch.Models.OutboxEnvelope envelope, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;
    }
}
