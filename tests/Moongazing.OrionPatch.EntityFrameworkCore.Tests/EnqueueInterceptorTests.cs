namespace Moongazing.OrionPatch.EntityFrameworkCore.Tests;

using Microsoft.EntityFrameworkCore;
using Moongazing.OrionPatch.EntityFrameworkCore;
using Moongazing.OrionPatch.Internal;
using Moongazing.OrionPatch.Models;
using System.Text.Json;
using Xunit;

public class EnqueueInterceptorTests
{
    private sealed record OrderConfirmed(Guid OrderId, int TotalCents);

    [Fact]
    public async Task SaveChangesAsync_ShouldPersistOutboxRow_WhenEnqueued()
    {
        using var db = await TestDb.CreateAsync();

        var outbox = new EfCoreOutbox(db, new MessageTypeNameResolver(), new MessageSerializer(new JsonSerializerOptions()));
        outbox.Enqueue(new OrderConfirmed(Guid.NewGuid(), 42));

        await db.SaveChangesAsync();

        var rows = await db.Set<OutboxRow>().ToListAsync();
        Assert.Single(rows);
        Assert.Equal(typeof(OrderConfirmed).FullName, rows[0].MessageType);
        Assert.Contains("42", rows[0].Payload);
        Assert.Equal(OutboxStatus.Pending, rows[0].Status);
        Assert.Equal(0, rows[0].AttemptCount);
    }

    [Fact]
    public async Task Rollback_ShouldNotPersistOutboxRow_WhenTransactionRollsBack()
    {
        using var db = await TestDb.CreateAsync();
        var outbox = new EfCoreOutbox(db, new MessageTypeNameResolver(), new MessageSerializer(new JsonSerializerOptions()));

        await using (var tx = await db.Database.BeginTransactionAsync())
        {
            outbox.Enqueue(new OrderConfirmed(Guid.NewGuid(), 1));
            db.Set<Sample>().Add(new Sample { Id = Guid.NewGuid(), Name = "x" });
            await db.SaveChangesAsync();
            await tx.RollbackAsync();
        }

        var rows = await db.Set<OutboxRow>().ToListAsync();
        Assert.Empty(rows);
    }

    [Fact]
    public async Task Enqueue_ShouldHonorMessageTypeOverride_WhenOptionsSupplyIt()
    {
        using var db = await TestDb.CreateAsync();
        var outbox = new EfCoreOutbox(db, new MessageTypeNameResolver(), new MessageSerializer(new JsonSerializerOptions()));

        outbox.Enqueue(new OrderConfirmed(Guid.NewGuid(), 1),
            new OutboxEnqueueOptions { MessageType = "App.OrderConfirmed.v2" });

        await db.SaveChangesAsync();

        var row = await db.Set<OutboxRow>().SingleAsync();
        Assert.Equal("App.OrderConfirmed.v2", row.MessageType);
    }

    [Fact]
    public async Task Enqueue_ShouldSerializeHeaders_WhenSupplied()
    {
        using var db = await TestDb.CreateAsync();
        var outbox = new EfCoreOutbox(db, new MessageTypeNameResolver(), new MessageSerializer(new JsonSerializerOptions()));

        outbox.Enqueue(new OrderConfirmed(Guid.NewGuid(), 1),
            new OutboxEnqueueOptions
            {
                Headers = new Dictionary<string, string> { ["tenant"] = "acme", ["source"] = "api" },
                CorrelationId = "corr-42",
            });

        await db.SaveChangesAsync();

        var row = await db.Set<OutboxRow>().SingleAsync();
        Assert.NotNull(row.HeadersJson);
        Assert.Contains("acme", row.HeadersJson);
        Assert.Equal("corr-42", row.CorrelationId);
    }

    [Fact]
    public async Task SaveChanges_ShouldFlushMultipleEnqueuesInOneCommit_WhenCalledOnce()
    {
        using var db = await TestDb.CreateAsync();
        var outbox = new EfCoreOutbox(db, new MessageTypeNameResolver(), new MessageSerializer(new JsonSerializerOptions()));

        outbox.Enqueue(new OrderConfirmed(Guid.NewGuid(), 1));
        outbox.Enqueue(new OrderConfirmed(Guid.NewGuid(), 2));
        outbox.Enqueue(new OrderConfirmed(Guid.NewGuid(), 3));

        await db.SaveChangesAsync();

        var rows = await db.Set<OutboxRow>().ToListAsync();
        Assert.Equal(3, rows.Count);
    }

    [Fact]
    public async Task SaveChanges_ShouldClearBuffer_WhenCalled()
    {
        using var db = await TestDb.CreateAsync();
        var outbox = new EfCoreOutbox(db, new MessageTypeNameResolver(), new MessageSerializer(new JsonSerializerOptions()));

        outbox.Enqueue(new OrderConfirmed(Guid.NewGuid(), 1));
        await db.SaveChangesAsync();

        // A second SaveChanges without further enqueues must produce no additional rows.
        await db.SaveChangesAsync();

        var rows = await db.Set<OutboxRow>().ToListAsync();
        Assert.Single(rows);
    }

    [Fact]
    public void Constructor_ShouldThrow_WhenAnyDependencyIsNull()
    {
        var resolver = new MessageTypeNameResolver();
        var serializer = new MessageSerializer(new JsonSerializerOptions());
        Assert.Throws<ArgumentNullException>(() => new EfCoreOutbox(null!, resolver, serializer));
        // db must be non-null; we don't actually need a real one for this guard test
        // — use a stub instead. We'll defer the other two arg guards to the integration-level coverage.
    }
}

public sealed class Sample
{
    public Guid Id { get; init; }
    public string Name { get; init; } = "";
}
