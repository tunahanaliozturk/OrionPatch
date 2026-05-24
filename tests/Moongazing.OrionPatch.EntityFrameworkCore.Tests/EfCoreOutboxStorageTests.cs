namespace Moongazing.OrionPatch.EntityFrameworkCore.Tests;

using Microsoft.EntityFrameworkCore;
using Moongazing.OrionPatch.EntityFrameworkCore;
using Moongazing.OrionPatch.EntityFrameworkCore.Claims;
using Moongazing.OrionPatch.Models;
using Xunit;

public class EfCoreOutboxStorageTests
{
    [Fact]
    public async Task ClaimNextAsync_ShouldReturnPendingRow_AndFlipStatusToClaimed_WhenCalled()
    {
        await using var db = await TestDb.CreateAsync();
        db.Add(NewPendingRow());
        await db.SaveChangesAsync();

        var storage = new EfCoreOutboxStorage(db, ProviderClaimStrategy.For(db));
        var batch = await storage.ClaimNextAsync(batchSize: 10, "dispatcher-1", TimeSpan.FromMinutes(1));

        Assert.Single(batch);
        var reloaded = await db.Set<OutboxRow>().AsNoTracking().FirstAsync();
        Assert.Equal(OutboxStatus.Claimed, reloaded.Status);
        Assert.Equal("dispatcher-1", reloaded.ClaimedBy);
        Assert.NotNull(reloaded.ClaimedAtUtc);
    }

    [Fact]
    public async Task ClaimNextAsync_ShouldReturnEmpty_WhenNoEligibleRows()
    {
        await using var db = await TestDb.CreateAsync();
        var storage = new EfCoreOutboxStorage(db, ProviderClaimStrategy.For(db));

        var batch = await storage.ClaimNextAsync(10, "d1", TimeSpan.FromMinutes(1));

        Assert.Empty(batch);
    }

    [Fact]
    public async Task ClaimNextAsync_ShouldNotReturnRow_WhenNextAttemptIsInFuture()
    {
        await using var db = await TestDb.CreateAsync();
        var row = NewPendingRow();
        row.NextAttemptAtUtc = DateTime.UtcNow.AddMinutes(5);
        db.Add(row);
        await db.SaveChangesAsync();

        var storage = new EfCoreOutboxStorage(db, ProviderClaimStrategy.For(db));
        var batch = await storage.ClaimNextAsync(10, "d1", TimeSpan.FromMinutes(1));

        Assert.Empty(batch);
    }

    [Fact]
    public async Task ClaimNextAsync_ShouldRespectLeaseExpiry_WhenClaimedRowIsStale()
    {
        await using var db = await TestDb.CreateAsync();
        var row = NewPendingRow();
        row.Status = OutboxStatus.Claimed;
        row.ClaimedAtUtc = DateTime.UtcNow.AddMinutes(-10);
        row.ClaimedBy = "old-dispatcher";
        db.Add(row);
        await db.SaveChangesAsync();

        var storage = new EfCoreOutboxStorage(db, ProviderClaimStrategy.For(db));
        var batch = await storage.ClaimNextAsync(10, "new-dispatcher", TimeSpan.FromMinutes(1));

        Assert.Single(batch);
        var reloaded = await db.Set<OutboxRow>().AsNoTracking().FirstAsync();
        Assert.Equal(OutboxStatus.Claimed, reloaded.Status);
        Assert.Equal("new-dispatcher", reloaded.ClaimedBy);
    }

    [Fact]
    public async Task ClaimNextAsync_ShouldNotReclaim_WhenLeaseIsStillValid()
    {
        await using var db = await TestDb.CreateAsync();
        var row = NewPendingRow();
        row.Status = OutboxStatus.Claimed;
        row.ClaimedAtUtc = DateTime.UtcNow.AddSeconds(-5);
        row.ClaimedBy = "active-dispatcher";
        db.Add(row);
        await db.SaveChangesAsync();

        var storage = new EfCoreOutboxStorage(db, ProviderClaimStrategy.For(db));
        var batch = await storage.ClaimNextAsync(10, "interloper", TimeSpan.FromMinutes(1));

        Assert.Empty(batch);
    }

    [Fact]
    public async Task ClaimNextAsync_ShouldRespectBatchSize_WhenMoreEligibleRowsExist()
    {
        await using var db = await TestDb.CreateAsync();
        for (var i = 0; i < 5; i++)
        {
            db.Add(NewPendingRow());
        }
        await db.SaveChangesAsync();

        var storage = new EfCoreOutboxStorage(db, ProviderClaimStrategy.For(db));
        var batch = await storage.ClaimNextAsync(batchSize: 3, "d1", TimeSpan.FromMinutes(1));

        Assert.Equal(3, batch.Count);
    }

    [Fact]
    public async Task ClaimNextAsync_ShouldOrderByEnqueuedAtAscending_WhenMultipleEligibleRowsExist()
    {
        await using var db = await TestDb.CreateAsync();
        var older = NewPendingRow();
        var newer = NewPendingRow();
        // Construct fresh rows with explicit EnqueuedAtUtc because the model is init-only.
        older = new OutboxRow
        {
            Id = older.Id,
            MessageType = older.MessageType,
            Payload = older.Payload,
            OccurredAtUtc = older.OccurredAtUtc,
            EnqueuedAtUtc = DateTime.UtcNow.AddSeconds(-10),
            Status = OutboxStatus.Pending,
        };
        newer = new OutboxRow
        {
            Id = newer.Id,
            MessageType = newer.MessageType,
            Payload = newer.Payload,
            OccurredAtUtc = newer.OccurredAtUtc,
            EnqueuedAtUtc = DateTime.UtcNow,
            Status = OutboxStatus.Pending,
        };
        db.Add(newer);
        db.Add(older);
        await db.SaveChangesAsync();

        var storage = new EfCoreOutboxStorage(db, ProviderClaimStrategy.For(db));
        var batch = await storage.ClaimNextAsync(batchSize: 1, "d1", TimeSpan.FromMinutes(1));

        Assert.Single(batch);
        Assert.Equal(older.Id, batch[0].Id);
    }

    [Fact]
    public async Task CompleteAsync_ShouldSetStatusProcessed_WhenCalled()
    {
        await using var db = await TestDb.CreateAsync();
        var row = NewPendingRow();
        row.Status = OutboxStatus.Claimed;
        db.Add(row);
        await db.SaveChangesAsync();

        var storage = new EfCoreOutboxStorage(db, ProviderClaimStrategy.For(db));
        var processedAt = DateTime.UtcNow;
        await storage.CompleteAsync(row.Id, processedAt);

        var reloaded = await db.Set<OutboxRow>().AsNoTracking().FirstAsync();
        Assert.Equal(OutboxStatus.Processed, reloaded.Status);
        Assert.NotNull(reloaded.ProcessedAtUtc);
    }

    [Fact]
    public async Task FailAsync_ShouldIncrementAttempt_AndSetNextAttempt_AndReturnToPending()
    {
        await using var db = await TestDb.CreateAsync();
        var row = NewPendingRow();
        row.Status = OutboxStatus.Claimed;
        row.AttemptCount = 1;
        db.Add(row);
        await db.SaveChangesAsync();

        var storage = new EfCoreOutboxStorage(db, ProviderClaimStrategy.For(db));
        var nextAttempt = DateTime.UtcNow.AddSeconds(30);
        await storage.FailAsync(row.Id, "transient error", nextAttempt);

        var reloaded = await db.Set<OutboxRow>().AsNoTracking().FirstAsync();
        Assert.Equal(OutboxStatus.Pending, reloaded.Status);
        Assert.Equal(2, reloaded.AttemptCount);
        Assert.Equal("transient error", reloaded.LastError);
        Assert.NotNull(reloaded.NextAttemptAtUtc);
        Assert.Null(reloaded.ClaimedBy);
        Assert.Null(reloaded.ClaimedAtUtc);
    }

    [Fact]
    public async Task DeadLetterAsync_ShouldSetStatusDeadLettered_WhenCalled()
    {
        await using var db = await TestDb.CreateAsync();
        var row = NewPendingRow();
        row.Status = OutboxStatus.Claimed;
        row.AttemptCount = 4;
        db.Add(row);
        await db.SaveChangesAsync();

        var storage = new EfCoreOutboxStorage(db, ProviderClaimStrategy.For(db));
        await storage.DeadLetterAsync(row.Id, "permanent error");

        var reloaded = await db.Set<OutboxRow>().AsNoTracking().FirstAsync();
        Assert.Equal(OutboxStatus.DeadLettered, reloaded.Status);
        Assert.Equal(5, reloaded.AttemptCount);
        Assert.Equal("permanent error", reloaded.LastError);
        Assert.Null(reloaded.ClaimedBy);
    }

    [Fact]
    public async Task QueueDepthAsync_ShouldCountPendingRowsOnly_WhenInvoked()
    {
        await using var db = await TestDb.CreateAsync();
        db.Add(NewPendingRow());
        db.Add(NewPendingRow());
        var processed = NewPendingRow();
        processed.Status = OutboxStatus.Processed;
        db.Add(processed);
        var claimed = NewPendingRow();
        claimed.Status = OutboxStatus.Claimed;
        db.Add(claimed);
        await db.SaveChangesAsync();

        var storage = new EfCoreOutboxStorage(db, ProviderClaimStrategy.For(db));
        var depth = await storage.QueueDepthAsync();

        Assert.Equal(2L, depth);
    }

    [Fact]
    public async Task AppendAsync_ShouldPersistRows_WhenCalled()
    {
        await using var db = await TestDb.CreateAsync();
        var storage = new EfCoreOutboxStorage(db, ProviderClaimStrategy.For(db));

        await storage.AppendAsync(new[] { NewPendingRow(), NewPendingRow() });

        var count = await db.Set<OutboxRow>().AsNoTracking().CountAsync();
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task Constructor_ShouldThrow_WhenAnyDependencyIsNull()
    {
        await using var db = await TestDb.CreateAsync();
        var strategy = new CompareAndSwapClaimStrategy();
        Assert.Throws<ArgumentNullException>(() => new EfCoreOutboxStorage(null!, strategy));
        Assert.Throws<ArgumentNullException>(() => new EfCoreOutboxStorage(db, null!));
    }

    private static OutboxRow NewPendingRow() => new()
    {
        Id = Guid.NewGuid(),
        MessageType = "TestMessage",
        Payload = "{}",
        OccurredAtUtc = DateTime.UtcNow,
        EnqueuedAtUtc = DateTime.UtcNow,
        Status = OutboxStatus.Pending,
    };
}
