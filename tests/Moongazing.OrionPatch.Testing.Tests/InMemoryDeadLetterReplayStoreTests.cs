namespace Moongazing.OrionPatch.Testing.Tests;

using System.Text.Json;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Models;
using Xunit;

public class InMemoryDeadLetterReplayStoreTests
{
    private static readonly DateTime Now = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

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

    private static async Task<(InMemoryOutboxStorage Storage, OutboxRow Row)> DeadLetteredStorageAsync(string messageType = "T")
    {
        var storage = new InMemoryOutboxStorage();
        var row = NewClaimed(messageType);
        await storage.AppendAsync(new[] { row });
        await ((IDeadLetterStore)storage).DeadLetterAsync(row.Id, new DeadLetterContext("boom", 5, Now), CancellationToken.None);
        return (storage, row);
    }

    [Fact]
    public async Task RedriveAsync_ById_ShouldReEnqueueAsFreshPendingRow_AndRemoveFromDeadLetter()
    {
        var (storage, row) = await DeadLetteredStorageAsync();

        var moved = await ((IDeadLetterReplayStore)storage).RedriveAsync(row.Id, CancellationToken.None);

        Assert.True(moved);
        Assert.Empty(storage.DeadLetteredMessages);

        var live = Assert.Single(storage.Rows);
        Assert.Equal(row.Id, live.Id);
        Assert.Equal(OutboxStatus.Pending, live.Status);
        Assert.Equal(0, live.AttemptCount);
        Assert.Null(live.LastError);
        Assert.Null(live.ProcessedAtUtc);
        Assert.Null(live.ClaimedAtUtc);
        Assert.Null(live.ClaimedBy);
        // Original payload / correlation id / occurrence time preserved.
        Assert.Equal("{\"v\":1}", live.Payload);
        Assert.Equal("corr-1", live.CorrelationId);
        Assert.Equal(Now, live.OccurredAtUtc);

        // The redriven-from header is stamped, and pre-existing headers survive.
        var headers = JsonSerializer.Deserialize<Dictionary<string, string>>(live.HeadersJson!)!;
        Assert.Equal(row.Id.ToString("N"), headers[IDeadLetterReplayStore.RedrivenFromHeader]);
        Assert.Equal("abc", headers["trace"]);
    }

    [Fact]
    public async Task RedriveAsync_ById_ShouldBeIdempotent_OnReRun()
    {
        var (storage, row) = await DeadLetteredStorageAsync();

        var first = await ((IDeadLetterReplayStore)storage).RedriveAsync(row.Id, CancellationToken.None);
        var second = await ((IDeadLetterReplayStore)storage).RedriveAsync(row.Id, CancellationToken.None);

        Assert.True(first);
        Assert.False(second);
        // Still exactly one live row, no duplicate enqueue.
        Assert.Single(storage.Rows);
        Assert.Empty(storage.DeadLetteredMessages);
    }

    [Fact]
    public async Task RedriveAsync_ById_ShouldBeCleanNoOp_WhenIdAbsent()
    {
        var storage = new InMemoryOutboxStorage();

        var moved = await ((IDeadLetterReplayStore)storage).RedriveAsync(Guid.NewGuid(), CancellationToken.None);

        Assert.False(moved);
        Assert.Empty(storage.Rows);
        Assert.Empty(storage.DeadLetteredMessages);
    }

    [Fact]
    public async Task RedrivenRow_ShouldBeDispatchableAgain()
    {
        var (storage, row) = await DeadLetteredStorageAsync();
        await ((IDeadLetterReplayStore)storage).RedriveAsync(row.Id, CancellationToken.None);

        var clock = new TestClock(Now);
        var sink = new CapturingOutboxSink();
        var dispatcher = new DeterministicDispatcher(storage, sink, clock);

        var dispatched = await dispatcher.DispatchOnceAsync();

        Assert.Equal(1, dispatched);
        var envelope = Assert.Single(sink.Sent);
        Assert.Equal(row.Id, envelope.Id);
        Assert.Equal(1, envelope.AttemptNumber); // fresh row: first attempt again
    }

    [Fact]
    public async Task RedriveAsync_Bulk_ShouldRedriveFilteredSet_InBatches_AndReturnCounts()
    {
        var storage = new InMemoryOutboxStorage();
        // 3 of type "A", 2 of type "B"; all dead-lettered.
        var aIds = new List<Guid>();
        for (var i = 0; i < 3; i++)
        {
            var row = NewClaimed("A");
            aIds.Add(row.Id);
            await storage.AppendAsync(new[] { row });
            await ((IDeadLetterStore)storage).DeadLetterAsync(row.Id, new DeadLetterContext("e", 5, Now), CancellationToken.None);
        }
        for (var i = 0; i < 2; i++)
        {
            var row = NewClaimed("B");
            await storage.AppendAsync(new[] { row });
            await ((IDeadLetterStore)storage).DeadLetterAsync(row.Id, new DeadLetterContext("e", 5, Now), CancellationToken.None);
        }

        var result = await ((IDeadLetterReplayStore)storage).RedriveAsync(
            new RedriveFilter(MessageType: "A"), batchSize: 2, CancellationToken.None);

        Assert.Equal(3, result.Redriven);
        Assert.Equal(0, result.Skipped);
        // Only the "A" messages moved back; the two "B" messages stay dead-lettered.
        Assert.Equal(3, storage.Rows.Count);
        Assert.All(storage.Rows, r => Assert.Equal("A", r.MessageType));
        Assert.Equal(2, storage.DeadLetteredMessages.Count);
        Assert.All(storage.DeadLetteredMessages, m => Assert.Equal("B", m.MessageType));
        Assert.All(aIds, id => Assert.Contains(storage.Rows, r => r.Id == id));
    }

    [Fact]
    public async Task RedriveAsync_Bulk_All_ShouldRedriveEverything()
    {
        var storage = new InMemoryOutboxStorage();
        for (var i = 0; i < 4; i++)
        {
            var row = NewClaimed();
            await storage.AppendAsync(new[] { row });
            await ((IDeadLetterStore)storage).DeadLetterAsync(row.Id, new DeadLetterContext("e", 5, Now), CancellationToken.None);
        }

        var result = await ((IDeadLetterReplayStore)storage).RedriveAsync(RedriveFilter.All, batchSize: 10, CancellationToken.None);

        Assert.Equal(4, result.Redriven);
        Assert.Equal(0, result.Skipped);
        Assert.Empty(storage.DeadLetteredMessages);
        Assert.Equal(4, storage.Rows.Count);
    }

    [Fact]
    public async Task RedriveAsync_Bulk_ShouldFilterByWindow()
    {
        var storage = new InMemoryOutboxStorage();
        var inside = NewClaimed();
        var outside = NewClaimed();
        await storage.AppendAsync(new[] { inside, outside });
        await ((IDeadLetterStore)storage).DeadLetterAsync(inside.Id, new DeadLetterContext("e", 5, Now), CancellationToken.None);
        await ((IDeadLetterStore)storage).DeadLetterAsync(outside.Id, new DeadLetterContext("e", 5, Now.AddHours(2)), CancellationToken.None);

        var result = await ((IDeadLetterReplayStore)storage).RedriveAsync(
            new RedriveFilter(DeadLetteredAtOrAfterUtc: Now, DeadLetteredBeforeUtc: Now.AddHours(1)),
            batchSize: 10,
            CancellationToken.None);

        Assert.Equal(1, result.Redriven);
        var live = Assert.Single(storage.Rows);
        Assert.Equal(inside.Id, live.Id);
        var stillDead = Assert.Single(storage.DeadLetteredMessages);
        Assert.Equal(outside.Id, stillDead.Id);
    }

    [Fact]
    public async Task RedriveAsync_Bulk_ShouldReturnEmpty_WhenFilterMatchesNothing()
    {
        var (storage, _) = await DeadLetteredStorageAsync("A");

        var result = await ((IDeadLetterReplayStore)storage).RedriveAsync(
            new RedriveFilter(MessageType: "does-not-exist"), batchSize: 10, CancellationToken.None);

        Assert.Equal(RedriveResult.Empty, result);
        Assert.Single(storage.DeadLetteredMessages);
    }

    [Fact]
    public async Task RedriveAsync_Bulk_ShouldRejectNonPositiveBatchSize()
    {
        var (storage, _) = await DeadLetteredStorageAsync();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            ((IDeadLetterReplayStore)storage).RedriveAsync(RedriveFilter.All, batchSize: 0, CancellationToken.None));
    }

    [Fact]
    public async Task AssertRedriven_ShouldFindReEnqueuedRow()
    {
        var (storage, row) = await DeadLetteredStorageAsync();
        await ((IDeadLetterReplayStore)storage).RedriveAsync(row.Id, CancellationToken.None);

        var found = storage.AssertRedriven(r => r.Id == row.Id);

        Assert.Equal(row.Id, found.Id);
        Assert.Equal(OutboxStatus.Pending, found.Status);
    }
}
