namespace Moongazing.OrionPatch.Sample;

using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Channels;
using Moongazing.OrionPatch.DependencyInjection;
using Moongazing.OrionPatch.EntityFrameworkCore.DependencyInjection;

internal static class Program
{
    public static async Task Main()
    {
        var connectionString = $"DataSource=file:sample-{Guid.NewGuid():N}?mode=memory&cache=shared";
        await using var keepAlive = new SqliteConnection(connectionString);
        await keepAlive.OpenAsync();

        var builder = Host.CreateApplicationBuilder();

        builder.Services.AddDbContext<SampleDbContext>((sp, options) =>
        {
            options.UseSqlite(connectionString);
            options.UseOrionPatch(sp);
        });

        builder.Services.AddOrionPatch(o => o.PollingInterval = TimeSpan.FromMilliseconds(100))
            .UseEntityFrameworkCore<SampleDbContext>()
            .UseChannelSink();

        using var host = builder.Build();

        // EnsureCreated on the shared connection.
        using (var scope = host.Services.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<SampleDbContext>();
            await db.Database.EnsureCreatedAsync();
        }

        await host.StartAsync();

        // Background consumer drains the channel and prints what arrives.
        var sink = host.Services.GetRequiredService<ChannelOutboxSink>();
        using var consumerCts = new CancellationTokenSource();
        var consumerTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var envelope in sink.Reader.ReadAllAsync(consumerCts.Token))
                {
                    Console.WriteLine($"[OrionPatch sample] Dispatched {envelope.MessageType} payload={envelope.Payload}");
                }
            }
            catch (OperationCanceledException) { /* graceful shutdown */ }
        }, consumerCts.Token);

        // Producer: enqueue three OrderConfirmed events in one SaveChanges.
        using (var scope = host.Services.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<SampleDbContext>();
            var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();

            foreach (var totalCents in new[] { 100, 250, 999 })
            {
                var evt = new OrderConfirmed(Guid.NewGuid(), totalCents);
                outbox.Enqueue(evt);
                Console.WriteLine($"[OrionPatch sample] Enqueued {nameof(OrderConfirmed)} Id={evt.Id} TotalCents={evt.TotalCents}");
            }
            await db.SaveChangesAsync();
        }

        // Give the dispatcher ~2 seconds to drain the outbox.
        await Task.Delay(TimeSpan.FromSeconds(2));

        consumerCts.Cancel();
        await host.StopAsync();
        try { await consumerTask; } catch (OperationCanceledException) { /* expected */ }
    }
}
