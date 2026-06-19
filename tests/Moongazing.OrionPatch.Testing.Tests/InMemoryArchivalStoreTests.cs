namespace Moongazing.OrionPatch.Testing.Tests;

using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Models;
using Xunit;

public class InMemoryArchivalStoreTests
{
    private static readonly DateTime Now = new(2026, 1, 10, 0, 0, 0, DateTimeKind.Utc);

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
        var storage = new InMemoryOutboxStorage();
        var old = Processed(Now.AddDays(-8));     // older than 7d retention -> archived
        var recent = Processed(Now.AddDays(-1));  // inside retention window -> kept
        await storage.AppendAsync(new[] { old, recent });

        var moved = await ((IOutboxArchivalStore)storage)
            .ArchiveProcessedAsync(TimeSpan.FromDays(7), Now, CancellationToken.None);

        Assert.Equal(1, moved);
        var remaining = Assert.Single(storage.Rows);
        Assert.Equal(recent.Id, remaining.Id);
        var archived = Assert.Single(storage.ArchivedRows);
        Assert.Equal(old.Id, archived.Id);
    }

    [Fact]
    public async Task ArchiveProcessedAsync_ShouldNotTouchPendingClaimedOrDeadLettered()
    {
        var storage = new InMemoryOutboxStorage();
        var pending = WithStatus(OutboxStatus.Pending);
        var claimed = WithStatus(OutboxStatus.Claimed);
        var deadLettered = WithStatus(OutboxStatus.DeadLettered);
        var processedOld = Processed(Now.AddDays(-30));
        await storage.AppendAsync(new[] { pending, claimed, deadLettered, processedOld });

        var moved = await ((IOutboxArchivalStore)storage)
            .ArchiveProcessedAsync(TimeSpan.Zero, Now, CancellationToken.None);

        Assert.Equal(1, moved);
        Assert.Equal(3, storage.Rows.Count);
        Assert.Contains(storage.Rows, r => r.Id == pending.Id);
        Assert.Contains(storage.Rows, r => r.Id == claimed.Id);
        Assert.Contains(storage.Rows, r => r.Id == deadLettered.Id);
        Assert.DoesNotContain(storage.Rows, r => r.Id == processedOld.Id);
    }

    [Fact]
    public async Task ArchiveProcessedAsync_ShouldRespectCutoffBoundary_Inclusive()
    {
        var storage = new InMemoryOutboxStorage();
        // Exactly at the cutoff (Now - retention) is eligible (<=), one tick newer is not.
        var atCutoff = Processed(Now.AddDays(-7));
        var justInside = Processed(Now.AddDays(-7).AddTicks(1));
        await storage.AppendAsync(new[] { atCutoff, justInside });

        var moved = await ((IOutboxArchivalStore)storage)
            .ArchiveProcessedAsync(TimeSpan.FromDays(7), Now, CancellationToken.None);

        Assert.Equal(1, moved);
        var remaining = Assert.Single(storage.Rows);
        Assert.Equal(justInside.Id, remaining.Id);
    }

    [Fact]
    public async Task ArchiveProcessedAsync_ShouldBeIdempotent_OnRepeatedPasses()
    {
        var storage = new InMemoryOutboxStorage();
        var old = Processed(Now.AddDays(-10));
        await storage.AppendAsync(new[] { old });

        var first = await ((IOutboxArchivalStore)storage)
            .ArchiveProcessedAsync(TimeSpan.FromDays(7), Now, CancellationToken.None);
        var second = await ((IOutboxArchivalStore)storage)
            .ArchiveProcessedAsync(TimeSpan.FromDays(7), Now, CancellationToken.None);

        Assert.Equal(1, first);
        Assert.Equal(0, second);
        Assert.Single(storage.ArchivedRows);
    }

    [Fact]
    public async Task ArchiveProcessedAsync_PurgeMode_ShouldDiscardRows_WithoutArchiving()
    {
        var storage = new InMemoryOutboxStorage(purgeOnArchive: true);
        var old = Processed(Now.AddDays(-10));
        await storage.AppendAsync(new[] { old });

        var moved = await ((IOutboxArchivalStore)storage)
            .ArchiveProcessedAsync(TimeSpan.FromDays(7), Now, CancellationToken.None);

        Assert.Equal(1, moved);
        Assert.Empty(storage.Rows);
        Assert.Empty(storage.ArchivedRows);
        var archivedSnapshot = await ((IOutboxArchivalStore)storage).GetArchivedAsync(CancellationToken.None);
        Assert.Empty(archivedSnapshot);
    }

    [Fact]
    public async Task ArchiveProcessedAsync_ShouldThrow_OnNegativeRetention()
    {
        var storage = new InMemoryOutboxStorage();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            ((IOutboxArchivalStore)storage)
                .ArchiveProcessedAsync(TimeSpan.FromSeconds(-1), Now, CancellationToken.None));
    }
}
