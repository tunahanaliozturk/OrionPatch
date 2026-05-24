namespace Moongazing.OrionPatch.EntityFrameworkCore.Tests;

using System.Text.Json;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Channels;
using Moongazing.OrionPatch.DependencyInjection;
using Moongazing.OrionPatch.EntityFrameworkCore;
using Moongazing.OrionPatch.EntityFrameworkCore.DependencyInjection;
using Moongazing.OrionPatch.Hosting;
using Moongazing.OrionPatch.Models;
using Xunit;

public class EndToEndDispatchTests
{
    private sealed record OrderConfirmed(Guid OrderId, int TotalCents);

    [Fact]
    public async Task FullCycle_ShouldDeliverEnqueuedMessageToChannelSink_WhenDispatcherRuns()
    {
        var connectionString = $"DataSource=file:e2e-{Guid.NewGuid():N}?mode=memory&cache=shared";

        // Keep a single connection open for the lifetime of the test so the shared-cache
        // in-memory database is not torn down between scopes.
        await using var keepAlive = new SqliteConnection(connectionString);
        await keepAlive.OpenAsync();

        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDbContext<E2EDbContext>((sp, options) =>
        {
            options.UseSqlite(connectionString);
            options.UseOrionPatch(sp);
        });

        services
            .AddOrionPatch(o => o.PollingInterval = TimeSpan.FromMilliseconds(50))
            .UseEntityFrameworkCore<E2EDbContext>()
            .UseChannelSink();

        await using var provider = services.BuildServiceProvider();

        // Setup: create schema.
        using (var setupScope = provider.CreateScope())
        {
            var db = setupScope.ServiceProvider.GetRequiredService<E2EDbContext>();
            await db.Database.EnsureCreatedAsync();
        }

        var orderId = Guid.NewGuid();

        // Enqueue + save in a separate scope to prove cross-scope visibility.
        using (var enqueueScope = provider.CreateScope())
        {
            var db = enqueueScope.ServiceProvider.GetRequiredService<E2EDbContext>();
            var outbox = enqueueScope.ServiceProvider.GetRequiredService<IOutbox>();
            outbox.Enqueue(new OrderConfirmed(orderId, 4242));
            await db.SaveChangesAsync();
        }

        var hosted = provider
            .GetServices<IHostedService>()
            .OfType<OutboxDispatcherHostedService>()
            .Single();

        var sink = provider.GetRequiredService<ChannelOutboxSink>();

        await hosted.StartAsync(default);
        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var envelope = await sink.Reader.ReadAsync(cts.Token);

            Assert.Equal(typeof(OrderConfirmed).FullName, envelope.MessageType);
            using var doc = JsonDocument.Parse(envelope.Payload);
            Assert.Equal(orderId, doc.RootElement.GetProperty("orderId").GetGuid());
            Assert.Equal(4242, doc.RootElement.GetProperty("totalCents").GetInt32());
        }
        finally
        {
            await hosted.StopAsync(default);
        }
    }
}

internal sealed class E2EDbContext(DbContextOptions<E2EDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyOrionPatchConfiguration();
    }
}
