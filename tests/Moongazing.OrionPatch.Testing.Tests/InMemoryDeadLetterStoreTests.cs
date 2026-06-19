namespace Moongazing.OrionPatch.Testing.Tests;

using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Models;
using Xunit;

public class InMemoryDeadLetterStoreTests
{
    private static OutboxRow NewClaimed(DateTime now)
    {
        return new OutboxRow
        {
            Id = Guid.NewGuid(),
            MessageType = "T",
            Payload = "{\"v\":1}",
            HeadersJson = "{\"h\":\"x\"}",
            CorrelationId = "corr-1",
            OccurredAtUtc = now,
            EnqueuedAtUtc = now,
            Status = OutboxStatus.Claimed,
            AttemptCount = 4,
        };
    }

    [Fact]
    public async Task DeadLetterAsync_ShouldMoveRowOutOfActiveOutbox_AndCaptureContext()
    {
        var now = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var storage = new InMemoryOutboxStorage();
        var row = NewClaimed(now);
        await storage.AppendAsync(new[] { row });

        var moved = await ((IDeadLetterStore)storage).DeadLetterAsync(
            row.Id, new DeadLetterContext("boom", 5, now), CancellationToken.None);

        Assert.True(moved);
        Assert.Empty(storage.Rows);
        var dead = Assert.Single(storage.DeadLetteredMessages);
        Assert.Equal(row.Id, dead.Id);
        Assert.Equal("T", dead.MessageType);
        Assert.Equal("{\"v\":1}", dead.Payload);
        Assert.Equal("{\"h\":\"x\"}", dead.HeadersJson);
        Assert.Equal("corr-1", dead.CorrelationId);
        Assert.Equal(5, dead.AttemptCount);
        Assert.Equal("boom", dead.FinalError);
        Assert.Equal(now, dead.DeadLetteredAtUtc);
        Assert.Equal(row.EnqueuedAtUtc, dead.EnqueuedAtUtc);
    }

    [Fact]
    public async Task DeadLetterAsync_ShouldBeIdempotent_WhenCalledTwiceForSameRow()
    {
        var now = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var storage = new InMemoryOutboxStorage();
        var row = NewClaimed(now);
        await storage.AppendAsync(new[] { row });

        var first = await ((IDeadLetterStore)storage).DeadLetterAsync(
            row.Id, new DeadLetterContext("boom", 5, now), CancellationToken.None);
        var second = await ((IDeadLetterStore)storage).DeadLetterAsync(
            row.Id, new DeadLetterContext("boom-again", 6, now.AddMinutes(1)), CancellationToken.None);

        Assert.True(first);
        Assert.False(second);
        // Exactly one message, with the FIRST call's context preserved (the replay is a no-op).
        var dead = Assert.Single(storage.DeadLetteredMessages);
        Assert.Equal(5, dead.AttemptCount);
        Assert.Equal("boom", dead.FinalError);
    }

    [Fact]
    public async Task DeadLetterAsync_ShouldReturnFalse_WhenRowDoesNotExist()
    {
        var storage = new InMemoryOutboxStorage();

        var moved = await ((IDeadLetterStore)storage).DeadLetterAsync(
            Guid.NewGuid(),
            new DeadLetterContext("boom", 5, DateTime.UtcNow),
            CancellationToken.None);

        Assert.False(moved);
        Assert.Empty(storage.DeadLetteredMessages);
    }

    [Fact]
    public async Task DeadLetterAsync_ShouldReject_WhenFinalErrorIsEmpty()
    {
        var now = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var storage = new InMemoryOutboxStorage();
        var row = NewClaimed(now);
        await storage.AppendAsync(new[] { row });

        await Assert.ThrowsAnyAsync<ArgumentException>(() =>
            ((IDeadLetterStore)storage).DeadLetterAsync(
                row.Id, new DeadLetterContext(string.Empty, 5, now), CancellationToken.None));
    }

    [Fact]
    public async Task GetDeadLetteredAsync_ShouldReturnAllRoutedMessages()
    {
        var now = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var storage = new InMemoryOutboxStorage();
        var a = NewClaimed(now);
        var b = NewClaimed(now);
        await storage.AppendAsync(new[] { a, b });

        await ((IDeadLetterStore)storage).DeadLetterAsync(a.Id, new DeadLetterContext("a", 5, now), CancellationToken.None);
        await ((IDeadLetterStore)storage).DeadLetterAsync(b.Id, new DeadLetterContext("b", 5, now), CancellationToken.None);

        var all = await ((IDeadLetterStore)storage).GetDeadLetteredAsync(CancellationToken.None);

        Assert.Equal(2, all.Count);
        Assert.Contains(all, m => m.Id == a.Id);
        Assert.Contains(all, m => m.Id == b.Id);
    }

    [Fact]
    public async Task DeadLetteredRow_ShouldNotBeClaimable_AfterRouting()
    {
        var now = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var storage = new InMemoryOutboxStorage();
        var row = NewClaimed(now);
        await storage.AppendAsync(new[] { row });

        await ((IDeadLetterStore)storage).DeadLetterAsync(
            row.Id, new DeadLetterContext("boom", 5, now), CancellationToken.None);

        var claimed = await storage.ClaimNextAsync(10, "dispatcher", TimeSpan.FromMinutes(1));

        Assert.Empty(claimed);
    }
}
