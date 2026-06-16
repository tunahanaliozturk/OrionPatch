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
    public async Task Enqueue_ShouldStampRealWriteTimeInEnqueuedAtUtc_WhenOccurredAtUtcIsBackdated()
    {
        using var db = await TestDb.CreateAsync();
        var outbox = new EfCoreOutbox(db, new MessageTypeNameResolver(), new MessageSerializer(new JsonSerializerOptions()));
        var backdated = DateTime.UtcNow - TimeSpan.FromHours(6);

        outbox.Enqueue(new OrderConfirmed(Guid.NewGuid(), 1),
            new OutboxEnqueueOptions { OccurredAtUtc = backdated });
        await db.SaveChangesAsync();

        var row = await db.Set<OutboxRow>().SingleAsync();

        // OccurredAtUtc keeps the caller's backdate; EnqueuedAtUtc is the real write time, so the
        // enqueue-based telemetry measures outbox dwell rather than the 6h backdate.
        Assert.Equal(backdated, row.OccurredAtUtc);
        Assert.True(row.EnqueuedAtUtc > row.OccurredAtUtc);
        Assert.True(row.EnqueuedAtUtc >= DateTime.UtcNow - TimeSpan.FromMinutes(1));
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
    public async Task Enqueue_ShouldSerializeHeadersWithConfiguredJsonOptions_WhenOptionsHasACustomNamingPolicy()
    {
        using var db = await TestDb.CreateAsync();
        var configured = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
            DictionaryKeyPolicy = JsonNamingPolicy.SnakeCaseLower,
        };
        var outbox = new EfCoreOutbox(db, new MessageTypeNameResolver(), new MessageSerializer(configured));

        outbox.Enqueue(new OrderConfirmed(Guid.NewGuid(), 1),
            new OutboxEnqueueOptions
            {
                Headers = new Dictionary<string, string> { ["TenantId"] = "acme" },
            });
        await db.SaveChangesAsync();

        var row = await db.Set<OutboxRow>().SingleAsync();
        Assert.NotNull(row.HeadersJson);
        // Snake-case dictionary key policy must rewrite "TenantId" -> "tenant_id" on serialize.
        Assert.Contains("tenant_id", row.HeadersJson);
        Assert.DoesNotContain("TenantId", row.HeadersJson);
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
    public async Task SaveChangesFailed_ShouldRebufferRows_WhenSaveThrows()
    {
        using var db = await TestDb.CreateAsync();
        var outbox = new EfCoreOutbox(db, new MessageTypeNameResolver(), new MessageSerializer(new JsonSerializerOptions()));

        outbox.Enqueue(new OrderConfirmed(Guid.NewGuid(), 1));

        // Force the first SaveChanges to throw by adding an entity that violates a required-field constraint.
        db.Set<Sample>().Add(new Sample { Id = Guid.NewGuid(), Name = null! }); // Name is required

        await Assert.ThrowsAnyAsync<Exception>(() => db.SaveChangesAsync());

        // The buffered outbox row should be re-buffered, not lost.
        Assert.Single(outbox.Buffer);

        // The change tracker should not retain a non-detached OutboxRow that would double-insert on retry.
        Assert.DoesNotContain(db.ChangeTracker.Entries<OutboxRow>(), e => e.State != EntityState.Detached);

        // Fix the offending entity and retry.
        db.ChangeTracker.Clear();
        db.Set<Sample>().Add(new Sample { Id = Guid.NewGuid(), Name = "fixed" });
        await db.SaveChangesAsync();

        var rows = await db.Set<OutboxRow>().AsNoTracking().ToListAsync();
        Assert.Single(rows);
    }

    [Fact]
    public async Task SaveChanges_ShouldNotDoubleInsert_WhenCalledTwiceAfterFlush()
    {
        using var db = await TestDb.CreateAsync();
        var outbox = new EfCoreOutbox(db, new MessageTypeNameResolver(), new MessageSerializer(new JsonSerializerOptions()));

        outbox.Enqueue(new OrderConfirmed(Guid.NewGuid(), 1));
        await db.SaveChangesAsync();
        await db.SaveChangesAsync(); // second save with no further enqueues

        var rows = await db.Set<OutboxRow>().AsNoTracking().ToListAsync();
        Assert.Single(rows); // not 2 — Commit cleared PendingFlush so the second save adds nothing.
    }

    [Fact]
    public async Task SaveChanges_ShouldPersistBothBatches_WhenCalledTwiceWithSeparateEnqueues()
    {
        using var db = await TestDb.CreateAsync();
        var outbox = new EfCoreOutbox(db, new MessageTypeNameResolver(), new MessageSerializer(new JsonSerializerOptions()));

        outbox.Enqueue(new OrderConfirmed(Guid.NewGuid(), 1));
        await db.SaveChangesAsync();

        outbox.Enqueue(new OrderConfirmed(Guid.NewGuid(), 2));
        await db.SaveChangesAsync();

        var rows = await db.Set<OutboxRow>().AsNoTracking().ToListAsync();
        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public async Task RowVersion_ShouldBeNullOnSqlite_AfterInsert_PinningTask6Behavior()
    {
        using var db = await TestDb.CreateAsync();
        var outbox = new EfCoreOutbox(db, new MessageTypeNameResolver(), new MessageSerializer(new JsonSerializerOptions()));

        outbox.Enqueue(new OrderConfirmed(Guid.NewGuid(), 1));
        await db.SaveChangesAsync();

        var row = await db.Set<OutboxRow>().AsNoTracking().SingleAsync();
        var rowVersion = db.Entry(row).Property<byte[]>("RowVersion").CurrentValue;

        // Pinning behavior: SQLite does NOT auto-populate IsRowVersion() on insert.
        // Task 6's SQLite claim strategy must handle this either by manual assignment
        // or by using a different concurrency primitive (e.g., compare-and-swap on
        // Status + ClaimedBy without relying on RowVersion).
        Assert.Null(rowVersion);
    }

    [Fact]
    public async Task Constructor_ShouldThrow_WhenAnyDependencyIsNull()
    {
        using var db = await TestDb.CreateAsync();
        var resolver = new MessageTypeNameResolver();
        var serializer = new MessageSerializer(new JsonSerializerOptions());

        Assert.Throws<ArgumentNullException>(() => new EfCoreOutbox(null!, resolver, serializer));
        Assert.Throws<ArgumentNullException>(() => new EfCoreOutbox(db, null!, serializer));
        Assert.Throws<ArgumentNullException>(() => new EfCoreOutbox(db, resolver, null!));
    }
}

public sealed class Sample
{
    public Guid Id { get; init; }
    public string Name { get; init; } = "";
}
