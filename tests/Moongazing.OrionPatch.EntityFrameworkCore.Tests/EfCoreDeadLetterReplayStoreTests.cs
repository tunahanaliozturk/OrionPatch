namespace Moongazing.OrionPatch.EntityFrameworkCore.Tests;

using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.EntityFrameworkCore;
using Moongazing.OrionPatch.EntityFrameworkCore.Claims;
using Moongazing.OrionPatch.Models;
using Xunit;

public class EfCoreDeadLetterReplayStoreTests
{
    private static readonly DateTime Now = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private static EfCoreOutboxStorage Storage(AppDbContext db) => new(db, ProviderClaimStrategy.For(db));

    private static OutboxRow NewClaimed(string messageType = "T")
    {
        return new OutboxRow
        {
            Id = Guid.NewGuid(),
            MessageType = messageType,
            Payload = "{\"v\":1}",
            HeadersJson = "{\"trace\":\"abc\"}",
            CorrelationId = "corr-1",
            OccurredAtUtc = Now,
            EnqueuedAtUtc = Now,
            Status = OutboxStatus.Claimed,
            AttemptCount = 5,
        };
    }

    private static async Task<OutboxRow> SeedDeadLetterAsync(AppDbContext db, string messageType = "T", DateTime? deadLetteredAt = null)
    {
        var row = NewClaimed(messageType);
        db.Add(row);
        await db.SaveChangesAsync();
        await Storage(db).DeadLetterAsync(
            row.Id, new DeadLetterContext("boom", 5, deadLetteredAt ?? Now), CancellationToken.None);
        return row;
    }

    [Fact]
    public async Task RedriveAsync_ById_ShouldReEnqueueAsFreshPendingRow_AndRemoveFromDeadLetter()
    {
        await using var db = await TestDb.CreateAsync();
        var row = await SeedDeadLetterAsync(db);

        var moved = await Storage(db).RedriveAsync(row.Id, CancellationToken.None);

        Assert.True(moved);
        Assert.Equal(0, await db.Set<DeadLetterRow>().AsNoTracking().CountAsync());

        var live = await db.Set<OutboxRow>().AsNoTracking().SingleAsync();
        Assert.Equal(row.Id, live.Id);
        Assert.Equal(OutboxStatus.Pending, live.Status);
        Assert.Equal(0, live.AttemptCount);
        Assert.Null(live.LastError);
        Assert.Null(live.ProcessedAtUtc);
        Assert.Equal("{\"v\":1}", live.Payload);
        Assert.Equal("corr-1", live.CorrelationId);
        Assert.Equal(Now, live.OccurredAtUtc);

        var headers = JsonSerializer.Deserialize<Dictionary<string, string>>(live.HeadersJson!)!;
        Assert.Equal(row.Id.ToString("N"), headers[IDeadLetterReplayStore.RedrivenFromHeader]);
        Assert.Equal("abc", headers["trace"]);
    }

    [Fact]
    public async Task RedriveAsync_ById_ShouldBeIdempotent_OnReRun()
    {
        await using var db = await TestDb.CreateAsync();
        var row = await SeedDeadLetterAsync(db);

        var first = await Storage(db).RedriveAsync(row.Id, CancellationToken.None);
        var second = await Storage(db).RedriveAsync(row.Id, CancellationToken.None);

        Assert.True(first);
        Assert.False(second);
        Assert.Equal(1, await db.Set<OutboxRow>().AsNoTracking().CountAsync());
        Assert.Equal(0, await db.Set<DeadLetterRow>().AsNoTracking().CountAsync());
    }

    [Fact]
    public async Task RedriveAsync_ById_ShouldBeCleanNoOp_WhenIdAbsent()
    {
        await using var db = await TestDb.CreateAsync();

        var moved = await Storage(db).RedriveAsync(Guid.NewGuid(), CancellationToken.None);

        Assert.False(moved);
        Assert.Equal(0, await db.Set<OutboxRow>().AsNoTracking().CountAsync());
        Assert.Equal(0, await db.Set<DeadLetterRow>().AsNoTracking().CountAsync());
    }

    [Fact]
    public async Task RedriveAsync_ById_ShouldBeIdempotent_OnFreshContext()
    {
        // A retried redrive on a NEW DbContext (new scope) must still see the first call's effect.
        var connection = TestDb.OpenSharedConnection();
        Guid rowId;
        await using (var db = await TestDb.CreateAsync(connection))
        {
            var row = await SeedDeadLetterAsync(db);
            rowId = row.Id;
            Assert.True(await Storage(db).RedriveAsync(rowId, CancellationToken.None));
        }

        await using (var db2 = await TestDb.CreateAsync(connection))
        {
            var second = await Storage(db2).RedriveAsync(rowId, CancellationToken.None);
            Assert.False(second);
            Assert.Equal(1, await db2.Set<OutboxRow>().AsNoTracking().CountAsync());
            Assert.Equal(0, await db2.Set<DeadLetterRow>().AsNoTracking().CountAsync());
        }

        await connection.DisposeAsync();
    }

    [Fact]
    public async Task RedrivenRow_ShouldBeClaimableAgain()
    {
        await using var db = await TestDb.CreateAsync();
        var row = await SeedDeadLetterAsync(db);
        await Storage(db).RedriveAsync(row.Id, CancellationToken.None);

        var claimed = await Storage(db).ClaimNextAsync(10, "dispatcher", TimeSpan.FromMinutes(1));

        var claimedRow = Assert.Single(claimed);
        Assert.Equal(row.Id, claimedRow.Id);
    }

    [Fact]
    public async Task RedriveAsync_Bulk_ShouldRedriveFilteredSet_InBatches_AndReturnCounts()
    {
        await using var db = await TestDb.CreateAsync();
        for (var i = 0; i < 3; i++)
        {
            await SeedDeadLetterAsync(db, "A", Now.AddMinutes(i));
        }
        for (var i = 0; i < 2; i++)
        {
            await SeedDeadLetterAsync(db, "B", Now.AddMinutes(i));
        }

        var result = await Storage(db).RedriveAsync(
            new RedriveFilter(MessageType: "A"), batchSize: 2, CancellationToken.None);

        Assert.Equal(3, result.Redriven);
        Assert.Equal(0, result.Skipped);
        Assert.Equal(3, await db.Set<OutboxRow>().AsNoTracking().CountAsync());
        Assert.All(await db.Set<OutboxRow>().AsNoTracking().ToListAsync(), r => Assert.Equal("A", r.MessageType));
        // The two "B" rows stay dead-lettered.
        Assert.Equal(2, await db.Set<DeadLetterRow>().AsNoTracking().CountAsync());
    }

    [Fact]
    public async Task RedriveAsync_Bulk_ShouldFilterByWindow()
    {
        await using var db = await TestDb.CreateAsync();
        var inside = await SeedDeadLetterAsync(db, "T", Now);
        var outside = await SeedDeadLetterAsync(db, "T", Now.AddHours(2));

        var result = await Storage(db).RedriveAsync(
            new RedriveFilter(DeadLetteredAtOrAfterUtc: Now, DeadLetteredBeforeUtc: Now.AddHours(1)),
            batchSize: 10,
            CancellationToken.None);

        Assert.Equal(1, result.Redriven);
        var live = await db.Set<OutboxRow>().AsNoTracking().SingleAsync();
        Assert.Equal(inside.Id, live.Id);
        var dead = await db.Set<DeadLetterRow>().AsNoTracking().SingleAsync();
        Assert.Equal(outside.Id, dead.Id);
    }

    [Fact]
    public async Task RedriveAsync_Bulk_ShouldReturnEmpty_WhenFilterMatchesNothing()
    {
        await using var db = await TestDb.CreateAsync();
        await SeedDeadLetterAsync(db, "A");

        var result = await Storage(db).RedriveAsync(
            new RedriveFilter(MessageType: "nope"), batchSize: 10, CancellationToken.None);

        Assert.Equal(RedriveResult.Empty, result);
        Assert.Equal(1, await db.Set<DeadLetterRow>().AsNoTracking().CountAsync());
        Assert.Equal(0, await db.Set<OutboxRow>().AsNoTracking().CountAsync());
    }

    [Fact]
    public async Task RedriveAsync_Bulk_ShouldRejectNonPositiveBatchSize()
    {
        await using var db = await TestDb.CreateAsync();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            Storage(db).RedriveAsync(RedriveFilter.All, batchSize: 0, CancellationToken.None));
    }

    [Fact]
    public async Task RedriveAsync_Bulk_All_ShouldDrainEntireStore()
    {
        await using var db = await TestDb.CreateAsync();
        for (var i = 0; i < 5; i++)
        {
            await SeedDeadLetterAsync(db, "T", Now.AddMinutes(i));
        }

        var result = await Storage(db).RedriveAsync(RedriveFilter.All, batchSize: 2, CancellationToken.None);

        Assert.Equal(5, result.Redriven);
        Assert.Equal(0, result.Skipped);
        Assert.Equal(0, await db.Set<DeadLetterRow>().AsNoTracking().CountAsync());
        Assert.Equal(5, await db.Set<OutboxRow>().AsNoTracking().CountAsync());
    }
}
