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

    [Fact]
    public async Task RedriveAsync_ById_ShouldNotDiscardCallersOtherTrackedChanges_NoChangeTrackerClear()
    {
        // FINDING 1 (data loss): the redrive must NOT clear the whole change tracker. A caller that
        // staged unrelated entities on the same shared DbContext must keep that data after a redrive.
        // The prior ChangeTracker.Clear() silently DISCARDED these entries, so they would never
        // persist - this test asserts they survive. (Only the conflicting outbox row id is detached.)
        await using var db = await TestDb.CreateAsync();
        var row = await SeedDeadLetterAsync(db);

        // The caller stages two unrelated entities on the SAME context - exactly the brownfield
        // "shared DbContext, mixed tracked work" hazard the prior Clear() obliterated.
        var pendingSample = new Sample { Id = Guid.NewGuid(), Name = "keep-me" };
        var pendingOutbox = new OutboxRow
        {
            Id = Guid.NewGuid(),
            MessageType = "Other",
            Payload = "{\"keep\":true}",
            HeadersJson = null,
            CorrelationId = "other-corr",
            OccurredAtUtc = Now,
            EnqueuedAtUtc = Now,
            Status = OutboxStatus.Pending,
            AttemptCount = 0,
            NextAttemptAtUtc = Now,
        };
        db.Add(pendingSample);
        db.Add(pendingOutbox);

        var moved = await Storage(db).RedriveAsync(row.Id, CancellationToken.None);
        Assert.True(moved);

        // The caller's unrelated entities are STILL tracked (not detached/discarded by a Clear): each
        // is in a persisting state, never Detached. Under the old Clear() they would be gone entirely.
        Assert.NotEqual(EntityState.Detached, db.Entry(pendingSample).State);
        Assert.NotEqual(EntityState.Detached, db.Entry(pendingOutbox).State);

        // And their data survives end-to-end: committing the caller's unit of work persists both,
        // then a no-tracking read pulls them back from the database. Under the old Clear() these rows
        // were discarded before the caller's SaveChanges ever saw them, so neither would exist.
        await db.SaveChangesAsync();
        Assert.NotNull(await db.Set<Sample>().AsNoTracking().FirstOrDefaultAsync(s => s.Id == pendingSample.Id));
        Assert.NotNull(await db.Set<OutboxRow>().AsNoTracking().FirstOrDefaultAsync(r => r.Id == pendingOutbox.Id));

        // The redriven row landed too, and the dead-letter row is gone.
        Assert.NotNull(await db.Set<OutboxRow>().AsNoTracking().FirstOrDefaultAsync(r => r.Id == row.Id));
        Assert.Equal(0, await db.Set<DeadLetterRow>().AsNoTracking().CountAsync());
    }

    [Fact]
    public async Task RedriveAsync_ById_ShouldBeIdempotent_WhenConcurrentRedriveAlreadyEnqueued_AndNotThrow()
    {
        // FINDING 2: a concurrent redrive that already inserted the fresh outbox row (the redrive
        // reuses the source id, so the second insert hits the outbox PK) must be reconciled as a
        // verified duplicate - a skip (false) - not thrown, and the dead-letter row must still be
        // removed so the message ends up live exactly once.
        var connection = TestDb.OpenSharedConnection();
        try
        {
            await using var db = await TestDb.CreateAsync(connection);
            var row = await SeedDeadLetterAsync(db);

            // Simulate the winning concurrent redrive: a live pending outbox row already exists under
            // the same (reused) id, while the dead-letter row is still present.
            await using (var other = await TestDb.CreateAsync(connection))
            {
                other.Add(new OutboxRow
                {
                    Id = row.Id,
                    MessageType = "T",
                    Payload = "{\"v\":1}",
                    HeadersJson = null,
                    CorrelationId = "corr-1",
                    OccurredAtUtc = Now,
                    EnqueuedAtUtc = Now,
                    Status = OutboxStatus.Pending,
                    AttemptCount = 0,
                    NextAttemptAtUtc = Now,
                });
                await other.SaveChangesAsync();
            }

            // This redrive loses the race on the insert; it must NOT throw.
            var moved = await Storage(db).RedriveAsync(row.Id, CancellationToken.None);

            Assert.False(moved); // verified duplicate -> skip
            // Exactly one live row (no duplicate), dead-letter row reconciled away.
            Assert.Equal(1, await db.Set<OutboxRow>().AsNoTracking().CountAsync(r => r.Id == row.Id));
            Assert.Equal(0, await db.Set<DeadLetterRow>().AsNoTracking().CountAsync());
        }
        finally
        {
            await connection.DisposeAsync();
        }
    }

    [Fact]
    public async Task RedriveAsync_Bulk_ShouldCrossFullAllSkipBatch_AndStillRedriveLaterRows()
    {
        // FINDING 3: a full batch in which every row is skipped must NOT terminate the sweep. The
        // loop must keep paging until no more matching dead-letter rows remain.
        await using var db = await TestDb.CreateAsync();

        // Seed 4 dead-letter rows of type "T", newest-first ordering by DeadLetteredAtUtc:
        //   newest two (i=3,2) are pre-enqueued live (so they redrive as verified-duplicate SKIPS),
        //   oldest two (i=1,0) are not (so they REDRIVE).
        var rows = new List<OutboxRow>();
        for (var i = 0; i < 4; i++)
        {
            rows.Add(await SeedDeadLetterAsync(db, "T", Now.AddMinutes(i)));
        }

        // Seeding (append source rows, then dead-letter them) leaves the source OutboxRow instances
        // tracked as Unchanged on this shared context. Detach them before re-adding live rows below
        // so the test's own setup does not collide on the identity map.
        db.ChangeTracker.Clear();

        // Pre-enqueue the two newest ids' live outbox rows so the first full batch is all skips.
        foreach (var r in rows.Skip(2))
        {
            db.Add(new OutboxRow
            {
                Id = r.Id,
                MessageType = "T",
                Payload = "{\"v\":1}",
                HeadersJson = null,
                CorrelationId = "corr-1",
                OccurredAtUtc = Now,
                EnqueuedAtUtc = Now,
                Status = OutboxStatus.Pending,
                AttemptCount = 0,
                NextAttemptAtUtc = Now,
            });
        }
        await db.SaveChangesAsync();
        db.ChangeTracker.Clear();

        // batchSize 2: batch 1 = the two newest (both skipped, full batch). A redriven==0 early-exit
        // would strand the older two; correct paging must continue and redrive them.
        var result = await Storage(db).RedriveAsync(
            new RedriveFilter(MessageType: "T"), batchSize: 2, CancellationToken.None);

        Assert.Equal(2, result.Redriven);
        Assert.Equal(2, result.Skipped);
        // All dead-letter rows are drained (skips reconcile their dead-letter rows away too).
        Assert.Equal(0, await db.Set<DeadLetterRow>().AsNoTracking().CountAsync());
        // The two older ids are now live; the two pre-enqueued ids remain (one each, no duplicates).
        Assert.Equal(4, await db.Set<OutboxRow>().AsNoTracking().CountAsync());
        foreach (var r in rows)
        {
            Assert.Equal(1, await db.Set<OutboxRow>().AsNoTracking().CountAsync(x => x.Id == r.Id));
        }
    }

    [Fact]
    public async Task RedriveAsync_Bulk_MessageTypeMatch_ShouldBeOrdinalCaseSensitive()
    {
        // FINDING 6: MessageType filtering is exact, case-sensitive, ordinal - identical to the
        // in-memory store. A differently-cased filter must match nothing.
        await using var db = await TestDb.CreateAsync();
        await SeedDeadLetterAsync(db, "OrderShipped", Now);

        var wrongCase = await Storage(db).RedriveAsync(
            new RedriveFilter(MessageType: "ordershipped"), batchSize: 10, CancellationToken.None);
        Assert.Equal(0, wrongCase.Redriven);
        Assert.Equal(1, await db.Set<DeadLetterRow>().AsNoTracking().CountAsync());

        var exact = await Storage(db).RedriveAsync(
            new RedriveFilter(MessageType: "OrderShipped"), batchSize: 10, CancellationToken.None);
        Assert.Equal(1, exact.Redriven);
        Assert.Equal(0, await db.Set<DeadLetterRow>().AsNoTracking().CountAsync());
    }
}
