namespace Moongazing.OrionPatch.EntityFrameworkCore.Tests;

using Microsoft.EntityFrameworkCore;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.EntityFrameworkCore;
using Moongazing.OrionPatch.EntityFrameworkCore.Claims;
using Moongazing.OrionPatch.Models;
using Moongazing.OrionPatch.Testing;
using Xunit;

/// <summary>
/// FINDINGS 5 and 6: the in-memory and EF Core stores must interpret a redrive's row id and a
/// <see cref="RedriveFilter"/> identically. These tests drive the SAME scenario through both
/// backends and assert they agree on which messages a filter selects and on the by-id contract.
/// </summary>
public class CrossStoreRedriveAgreementTests
{
    private static readonly DateTime Now = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private static OutboxRow NewClaimed(Guid id, string messageType, DateTime occurredAt)
        => new()
        {
            Id = id,
            MessageType = messageType,
            Payload = "{\"v\":1}",
            HeadersJson = "{\"trace\":\"abc\"}",
            CorrelationId = "corr-1",
            OccurredAtUtc = occurredAt,
            EnqueuedAtUtc = occurredAt,
            Status = OutboxStatus.Claimed,
            AttemptCount = 5,
        };

    private sealed record Seed(Guid Id, string MessageType, DateTime DeadLetteredAt);

    private static IReadOnlyList<Seed> Scenario() =>
    [
        new(Guid.NewGuid(), "OrderShipped", Now),
        new(Guid.NewGuid(), "OrderShipped", Now.AddMinutes(30)),
        new(Guid.NewGuid(), "ordershipped", Now.AddMinutes(45)), // different case -> must NOT match "OrderShipped"
        new(Guid.NewGuid(), "OrderCancelled", Now.AddHours(2)),
    ];

    private static async Task<InMemoryOutboxStorage> SeedInMemoryAsync(IReadOnlyList<Seed> seeds)
    {
        var storage = new InMemoryOutboxStorage();
        foreach (var s in seeds)
        {
            await storage.AppendAsync([NewClaimed(s.Id, s.MessageType, Now)]);
            await ((IDeadLetterStore)storage).DeadLetterAsync(
                s.Id, new DeadLetterContext("boom", 5, s.DeadLetteredAt), CancellationToken.None);
        }
        return storage;
    }

    private static async Task SeedEfAsync(AppDbContext db, IReadOnlyList<Seed> seeds)
    {
        var storage = new EfCoreOutboxStorage(db, ProviderClaimStrategy.For(db));
        foreach (var s in seeds)
        {
            db.Add(NewClaimed(s.Id, s.MessageType, Now));
            await db.SaveChangesAsync();
            await ((IDeadLetterStore)storage).DeadLetterAsync(
                s.Id, new DeadLetterContext("boom", 5, s.DeadLetteredAt), CancellationToken.None);
        }
    }

    public static TheoryData<RedriveFilter> Filters()
    {
        var data = new TheoryData<RedriveFilter>();
        data.Add(RedriveFilter.All);
        data.Add(new RedriveFilter(MessageType: "OrderShipped"));
        data.Add(new RedriveFilter(MessageType: "ordershipped"));
        data.Add(new RedriveFilter(MessageType: "OrderCancelled"));
        data.Add(new RedriveFilter(MessageType: "does-not-exist"));
        data.Add(new RedriveFilter(DeadLetteredAtOrAfterUtc: Now, DeadLetteredBeforeUtc: Now.AddHours(1)));
        data.Add(new RedriveFilter(MessageType: "OrderShipped", DeadLetteredBeforeUtc: Now.AddMinutes(40)));
        return data;
    }

    [Theory]
    [MemberData(nameof(Filters))]
    public async Task BothStores_ShouldRedriveTheSameSet_ForTheSameFilter(RedriveFilter filter)
    {
        var seeds = Scenario();

        // In-memory store.
        var inMemory = await SeedInMemoryAsync(seeds);
        var inMemoryResult = await ((IDeadLetterReplayStore)inMemory).RedriveAsync(filter, batchSize: 2, CancellationToken.None);
        var inMemoryLiveIds = inMemory.Rows.Select(r => r.Id).OrderBy(g => g).ToList();

        // EF Core store.
        await using var db = await TestDb.CreateAsync();
        await SeedEfAsync(db, seeds);
        var efResult = await new EfCoreOutboxStorage(db, ProviderClaimStrategy.For(db))
            .RedriveAsync(filter, batchSize: 2, CancellationToken.None);
        var efLiveIds = await db.Set<OutboxRow>().AsNoTracking().Select(r => r.Id).ToListAsync();
        efLiveIds.Sort();

        // Both stores agree on the redriven/skipped counts and on exactly which ids became live.
        Assert.Equal(inMemoryResult.Redriven, efResult.Redriven);
        Assert.Equal(inMemoryResult.Skipped, efResult.Skipped);
        Assert.Equal(inMemoryLiveIds, efLiveIds);
    }

    [Fact]
    public async Task BothStores_ById_ShouldInterpretDeadLetterIdIdentically()
    {
        // The by-id contract: RedriveAsync(id) takes the DeadLetteredMessage.Id (== source outbox
        // row id). Both stores must redrive the same target for the same id and no-op the same way
        // for an absent id.
        var seeds = Scenario();
        var target = seeds[1].Id;
        var absent = Guid.NewGuid();

        var inMemory = await SeedInMemoryAsync(seeds);
        await using var db = await TestDb.CreateAsync();
        await SeedEfAsync(db, seeds);
        var ef = new EfCoreOutboxStorage(db, ProviderClaimStrategy.For(db));

        Assert.True(await ((IDeadLetterReplayStore)inMemory).RedriveAsync(target, CancellationToken.None));
        Assert.True(await ef.RedriveAsync(target, CancellationToken.None));

        Assert.False(await ((IDeadLetterReplayStore)inMemory).RedriveAsync(absent, CancellationToken.None));
        Assert.False(await ef.RedriveAsync(absent, CancellationToken.None));

        // Exactly the target id is live in both stores; the dead-letter records of the rest match.
        Assert.Equal(new[] { target }, inMemory.Rows.Select(r => r.Id).ToArray());
        var efLive = await db.Set<OutboxRow>().AsNoTracking().Select(r => r.Id).ToListAsync();
        Assert.Equal(new[] { target }, efLive.ToArray());

        Assert.Equal(
            inMemory.DeadLetteredMessages.Select(m => m.Id).OrderBy(g => g),
            (await db.Set<DeadLetterRow>().AsNoTracking().Select(d => d.Id).ToListAsync()).OrderBy(g => g));
    }
}
