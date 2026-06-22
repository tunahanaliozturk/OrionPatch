namespace Moongazing.OrionPatch.EntityFrameworkCore.Tests;

using Microsoft.EntityFrameworkCore;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.EntityFrameworkCore;
using Moongazing.OrionPatch.EntityFrameworkCore.Claims;
using Moongazing.OrionPatch.Models;
using Xunit;

public class EfCoreDeadLetterStoreTests
{
    private static readonly DateTime Now = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private static EfCoreOutboxStorage Storage(AppDbContext db) => new(db, ProviderClaimStrategy.For(db));

    private static OutboxRow NewClaimed()
    {
        return new OutboxRow
        {
            Id = Guid.NewGuid(),
            MessageType = "T",
            Payload = "{\"v\":1}",
            HeadersJson = "{\"h\":\"x\"}",
            CorrelationId = "corr-1",
            OccurredAtUtc = Now,
            EnqueuedAtUtc = Now,
            Status = OutboxStatus.Claimed,
            AttemptCount = 4,
        };
    }

    [Fact]
    public async Task DeadLetterAsync_ShouldMoveRowOutOfActiveOutbox_AndCaptureContext()
    {
        await using var db = await TestDb.CreateAsync();
        var row = NewClaimed();
        db.Add(row);
        await db.SaveChangesAsync();

        var moved = await Storage(db).DeadLetterAsync(
            row.Id, new DeadLetterContext("boom", 5, Now), CancellationToken.None);

        Assert.True(moved);
        Assert.Equal(0, await db.Set<OutboxRow>().AsNoTracking().CountAsync());
        var dead = await db.Set<DeadLetterRow>().AsNoTracking().SingleAsync();
        Assert.Equal(row.Id, dead.Id);
        Assert.Equal("T", dead.MessageType);
        Assert.Equal("{\"v\":1}", dead.Payload);
        Assert.Equal("{\"h\":\"x\"}", dead.HeadersJson);
        Assert.Equal("corr-1", dead.CorrelationId);
        Assert.Equal(5, dead.AttemptCount);
        Assert.Equal("boom", dead.FinalError);
        Assert.Equal(Now, dead.DeadLetteredAtUtc);
        Assert.Equal(row.EnqueuedAtUtc, dead.EnqueuedAtUtc);
    }

    [Fact]
    public async Task DeadLetterAsync_ShouldBeIdempotent_WhenCalledTwiceForSameRow()
    {
        await using var db = await TestDb.CreateAsync();
        var row = NewClaimed();
        db.Add(row);
        await db.SaveChangesAsync();

        var first = await Storage(db).DeadLetterAsync(
            row.Id, new DeadLetterContext("boom", 5, Now), CancellationToken.None);
        var second = await Storage(db).DeadLetterAsync(
            row.Id, new DeadLetterContext("boom-again", 6, Now.AddMinutes(1)), CancellationToken.None);

        Assert.True(first);
        Assert.False(second);
        // Exactly one dead-letter row, with the FIRST call's context preserved (replay is a no-op).
        var dead = await db.Set<DeadLetterRow>().AsNoTracking().SingleAsync();
        Assert.Equal(5, dead.AttemptCount);
        Assert.Equal("boom", dead.FinalError);
        Assert.Equal(0, await db.Set<OutboxRow>().AsNoTracking().CountAsync());
    }

    [Fact]
    public async Task DeadLetterAsync_ShouldBeIdempotent_WhenReplayedOnAFreshContext()
    {
        // A crash-replayed terminal path runs on a NEW DbContext (new scope). The dead-letter row
        // persisted by the first call must still be seen so the move lands exactly once.
        var connection = TestDb.OpenSharedConnection();
        await using (var db = await TestDb.CreateAsync(connection))
        {
            var row = NewClaimed();
            db.Add(row);
            await db.SaveChangesAsync();
            var first = await Storage(db).DeadLetterAsync(row.Id, new DeadLetterContext("boom", 5, Now), CancellationToken.None);
            Assert.True(first);
        }

        await using (var db2 = await TestDb.CreateAsync(connection))
        {
            var rowId = await db2.Set<DeadLetterRow>().AsNoTracking().Select(d => d.Id).SingleAsync();
            var second = await Storage(db2).DeadLetterAsync(rowId, new DeadLetterContext("boom-again", 6, Now), CancellationToken.None);
            Assert.False(second);
            Assert.Equal(1, await db2.Set<DeadLetterRow>().AsNoTracking().CountAsync());
        }

        await connection.DisposeAsync();
    }

    [Fact]
    public async Task DeadLetterAsync_ShouldReturnFalse_WhenRowDoesNotExist()
    {
        await using var db = await TestDb.CreateAsync();

        var moved = await Storage(db).DeadLetterAsync(
            Guid.NewGuid(), new DeadLetterContext("boom", 5, Now), CancellationToken.None);

        Assert.False(moved);
        Assert.Equal(0, await db.Set<DeadLetterRow>().AsNoTracking().CountAsync());
    }

    [Fact]
    public async Task DeadLetterAsync_ShouldReject_WhenFinalErrorIsEmpty()
    {
        await using var db = await TestDb.CreateAsync();
        var row = NewClaimed();
        db.Add(row);
        await db.SaveChangesAsync();

        await Assert.ThrowsAnyAsync<ArgumentException>(() =>
            Storage(db).DeadLetterAsync(row.Id, new DeadLetterContext(string.Empty, 5, Now), CancellationToken.None));
    }

    [Fact]
    public async Task GetDeadLetteredAsync_ShouldReturnAllRoutedMessages_NewestFirst()
    {
        await using var db = await TestDb.CreateAsync();
        var a = NewClaimed();
        var b = NewClaimed();
        db.AddRange(a, b);
        await db.SaveChangesAsync();

        await Storage(db).DeadLetterAsync(a.Id, new DeadLetterContext("a", 5, Now), CancellationToken.None);
        await Storage(db).DeadLetterAsync(b.Id, new DeadLetterContext("b", 5, Now.AddMinutes(1)), CancellationToken.None);

        var all = await Storage(db).GetDeadLetteredAsync(CancellationToken.None);

        Assert.Equal(2, all.Count);
        Assert.Equal(b.Id, all[0].Id); // newest first
        Assert.Equal(a.Id, all[1].Id);
    }

    [Fact]
    public async Task GetDeadLetteredAsync_Paged_ShouldRespectSkipAndTake()
    {
        await using var db = await TestDb.CreateAsync();
        for (var i = 0; i < 5; i++)
        {
            var row = NewClaimed();
            db.Add(row);
            await db.SaveChangesAsync();
            await Storage(db).DeadLetterAsync(row.Id, new DeadLetterContext($"e{i}", 5, Now.AddMinutes(i)), CancellationToken.None);
        }

        var page = await Storage(db).GetDeadLetteredAsync(skip: 1, take: 2, CancellationToken.None);

        Assert.Equal(2, page.Count);
        // Newest is e4 (skipped); page should be e3 then e2.
        Assert.Equal("e3", page[0].FinalError);
        Assert.Equal("e2", page[1].FinalError);
    }

    [Fact]
    public async Task GetDeadLetteredAsync_Paged_ShouldThrow_OnInvalidArguments()
    {
        await using var db = await TestDb.CreateAsync();
        var storage = Storage(db);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => storage.GetDeadLetteredAsync(-1, 10, CancellationToken.None));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => storage.GetDeadLetteredAsync(0, 0, CancellationToken.None));
    }

    [Fact]
    public async Task DeadLetteredRow_ShouldNotBeClaimable_AfterRouting()
    {
        await using var db = await TestDb.CreateAsync();
        var row = NewClaimed();
        db.Add(row);
        await db.SaveChangesAsync();

        await Storage(db).DeadLetterAsync(row.Id, new DeadLetterContext("boom", 5, Now), CancellationToken.None);

        var claimed = await Storage(db).ClaimNextAsync(10, "dispatcher", TimeSpan.FromMinutes(1));

        Assert.Empty(claimed);
    }
}
