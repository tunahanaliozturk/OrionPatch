namespace Moongazing.OrionPatch.EntityFrameworkCore.Tests;

using Microsoft.EntityFrameworkCore;
using Moongazing.OrionPatch.EntityFrameworkCore;
using Moongazing.OrionPatch.EntityFrameworkCore.Claims;
using Moongazing.OrionPatch.Models;
using Xunit;

public class EfCoreArchivalStoreTests
{
    private static readonly DateTime Now = new(2026, 1, 10, 0, 0, 0, DateTimeKind.Utc);

    private static EfCoreOutboxStorage Storage(AppDbContext db, bool purge = false) =>
        new(db, ProviderClaimStrategy.For(db), purge);

    private static OutboxRow Processed(DateTime processedAtUtc)
    {
        return new OutboxRow
        {
            Id = Guid.NewGuid(),
            MessageType = "T",
            Payload = "{}",
            OccurredAtUtc = processedAtUtc,
            EnqueuedAtUtc = processedAtUtc,
            Status = OutboxStatus.Processed,
            ProcessedAtUtc = processedAtUtc,
        };
    }

    private static OutboxRow WithStatus(OutboxStatus status)
    {
        return new OutboxRow
        {
            Id = Guid.NewGuid(),
            MessageType = "T",
            Payload = "{}",
            OccurredAtUtc = Now,
            EnqueuedAtUtc = Now,
            Status = status,
        };
    }

    [Fact]
    public async Task ArchiveProcessedAsync_ShouldArchiveProcessedRows_PastRetention()
    {
        await using var db = await TestDb.CreateAsync();
        var old = Processed(Now.AddDays(-8));     // older than 7d retention -> archived
        var recent = Processed(Now.AddDays(-1));  // inside retention window -> kept
        db.AddRange(old, recent);
        await db.SaveChangesAsync();

        var moved = await Storage(db).ArchiveProcessedAsync(TimeSpan.FromDays(7), Now, CancellationToken.None);

        Assert.Equal(1, moved);
        var remaining = await db.Set<OutboxRow>().AsNoTracking().SingleAsync();
        Assert.Equal(recent.Id, remaining.Id);
        var archived = await db.Set<OutboxArchiveRow>().AsNoTracking().SingleAsync();
        Assert.Equal(old.Id, archived.Id);
        Assert.Equal(old.ProcessedAtUtc, archived.ProcessedAtUtc);
    }

    [Fact]
    public async Task ArchiveProcessedAsync_ShouldNotTouchPendingClaimedOrDeadLettered()
    {
        await using var db = await TestDb.CreateAsync();
        var pending = WithStatus(OutboxStatus.Pending);
        var claimed = WithStatus(OutboxStatus.Claimed);
        var deadLettered = WithStatus(OutboxStatus.DeadLettered);
        var processedOld = Processed(Now.AddDays(-30));
        db.AddRange(pending, claimed, deadLettered, processedOld);
        await db.SaveChangesAsync();

        var moved = await Storage(db).ArchiveProcessedAsync(TimeSpan.Zero, Now, CancellationToken.None);

        Assert.Equal(1, moved);
        var remainingIds = await db.Set<OutboxRow>().AsNoTracking().Select(r => r.Id).ToListAsync();
        Assert.Equal(3, remainingIds.Count);
        Assert.Contains(pending.Id, remainingIds);
        Assert.Contains(claimed.Id, remainingIds);
        Assert.Contains(deadLettered.Id, remainingIds);
        Assert.DoesNotContain(processedOld.Id, remainingIds);
    }

    [Fact]
    public async Task ArchiveProcessedAsync_ShouldRespectCutoffBoundary_Inclusive()
    {
        await using var db = await TestDb.CreateAsync();
        // Exactly at the cutoff (Now - retention) is eligible (<=), one tick newer is not.
        var atCutoff = Processed(Now.AddDays(-7));
        var justInside = Processed(Now.AddDays(-7).AddTicks(1));
        db.AddRange(atCutoff, justInside);
        await db.SaveChangesAsync();

        var moved = await Storage(db).ArchiveProcessedAsync(TimeSpan.FromDays(7), Now, CancellationToken.None);

        Assert.Equal(1, moved);
        var remaining = await db.Set<OutboxRow>().AsNoTracking().SingleAsync();
        Assert.Equal(justInside.Id, remaining.Id);
    }

    [Fact]
    public async Task ArchiveProcessedAsync_ShouldBeIdempotent_OnRepeatedPasses()
    {
        await using var db = await TestDb.CreateAsync();
        var old = Processed(Now.AddDays(-10));
        db.Add(old);
        await db.SaveChangesAsync();

        var first = await Storage(db).ArchiveProcessedAsync(TimeSpan.FromDays(7), Now, CancellationToken.None);
        var second = await Storage(db).ArchiveProcessedAsync(TimeSpan.FromDays(7), Now, CancellationToken.None);

        Assert.Equal(1, first);
        Assert.Equal(0, second);
        Assert.Equal(1, await db.Set<OutboxArchiveRow>().AsNoTracking().CountAsync());
    }

    [Fact]
    public async Task ArchiveProcessedAsync_PurgeMode_ShouldDiscardRows_WithoutArchiving()
    {
        await using var db = await TestDb.CreateAsync();
        var old = Processed(Now.AddDays(-10));
        db.Add(old);
        await db.SaveChangesAsync();

        var moved = await Storage(db, purge: true).ArchiveProcessedAsync(TimeSpan.FromDays(7), Now, CancellationToken.None);

        Assert.Equal(1, moved);
        Assert.Equal(0, await db.Set<OutboxRow>().AsNoTracking().CountAsync());
        Assert.Equal(0, await db.Set<OutboxArchiveRow>().AsNoTracking().CountAsync());
        var archivedSnapshot = await Storage(db, purge: true).GetArchivedAsync(CancellationToken.None);
        Assert.Empty(archivedSnapshot);
    }

    [Fact]
    public async Task GetArchivedAsync_ShouldReturnArchivedRows_InArchiveMode()
    {
        await using var db = await TestDb.CreateAsync();
        var old = Processed(Now.AddDays(-10));
        db.Add(old);
        await db.SaveChangesAsync();

        await Storage(db).ArchiveProcessedAsync(TimeSpan.FromDays(7), Now, CancellationToken.None);
        var archived = await Storage(db).GetArchivedAsync(CancellationToken.None);

        var single = Assert.Single(archived);
        Assert.Equal(old.Id, single.Id);
        Assert.Equal(OutboxStatus.Processed, single.Status);
    }

    [Fact]
    public async Task ArchiveProcessedAsync_ShouldThrow_OnNegativeRetention()
    {
        await using var db = await TestDb.CreateAsync();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            Storage(db).ArchiveProcessedAsync(TimeSpan.FromSeconds(-1), Now, CancellationToken.None));
    }

    [Fact]
    public async Task ArchiveProcessedAsync_ShouldReapEntireBacklog_AcrossBatchBoundary()
    {
        // More rows than the internal batch size (500) so the reap must loop. All are past the
        // cutoff, so every row should end up archived and the active outbox emptied.
        await using var db = await TestDb.CreateAsync();
        const int count = 1100;
        for (var i = 0; i < count; i++)
        {
            db.Add(Processed(Now.AddDays(-10).AddSeconds(i)));
        }
        await db.SaveChangesAsync();

        var moved = await Storage(db).ArchiveProcessedAsync(TimeSpan.FromDays(7), Now, CancellationToken.None);

        Assert.Equal(count, moved);
        Assert.Equal(0, await db.Set<OutboxRow>().AsNoTracking().CountAsync());
        Assert.Equal(count, await db.Set<OutboxArchiveRow>().AsNoTracking().CountAsync());
    }
}
