namespace Moongazing.OrionPatch.Testing.Tests;

using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Configuration;
using Moongazing.OrionPatch.Models;
using Xunit;

public class DeterministicDispatcherTests
{
    [Fact]
    public async Task DispatchOnceAsync_ShouldDeliverRowToSink_WhenStorageHasPending()
    {
        var storage = new InMemoryOutboxStorage();
        var sink = new CapturingOutboxSink();
        var clock = new TestClock(new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        var outbox = new InMemoryOutbox(storage);
        var dispatcher = new DeterministicDispatcher(storage, sink, clock);

        outbox.Enqueue(new TestMessage("hello"));

        var processed = await dispatcher.DispatchOnceAsync();

        Assert.Equal(1, processed);
        Assert.Single(sink.Sent);
        Assert.Contains("hello", sink.Sent[0].Payload);
    }

    [Fact]
    public async Task DispatchOnceAsync_ShouldMarkProcessed_WhenSinkSucceeds()
    {
        var storage = new InMemoryOutboxStorage();
        var sink = new CapturingOutboxSink();
        var clock = new TestClock(new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        var outbox = new InMemoryOutbox(storage);
        var dispatcher = new DeterministicDispatcher(storage, sink, clock);

        outbox.Enqueue(new TestMessage("a"));
        await dispatcher.DispatchOnceAsync();

        var stored = Assert.Single(storage.Rows);
        Assert.Equal(OutboxStatus.Processed, stored.Status);
        Assert.Equal(clock.UtcNow, stored.ProcessedAtUtc);
    }

    [Fact]
    public async Task DispatchOnceAsync_ShouldFail_WhenSinkThrows()
    {
        var storage = new InMemoryOutboxStorage();
        var throwingSink = new ThrowingSink();
        var clock = new TestClock(new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        var options = new OrionPatchOptions { MaxAttempts = 5 };
        var outbox = new InMemoryOutbox(storage);
        var dispatcher = new DeterministicDispatcher(storage, throwingSink, clock, options);

        outbox.Enqueue(new TestMessage("a"));
        var processed = await dispatcher.DispatchOnceAsync();

        var stored = Assert.Single(storage.Rows);
        Assert.Equal(0, processed);
        Assert.Equal(OutboxStatus.Pending, stored.Status);
        Assert.Equal(1, stored.AttemptCount);
        Assert.NotNull(stored.LastError);
    }

    [Fact]
    public async Task DispatchOnceAsync_ShouldRouteToDeadLetterStore_WhenAttemptExceedsMax()
    {
        var storage = new InMemoryOutboxStorage();
        var throwingSink = new ThrowingSink();
        var clock = new TestClock(new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        var options = new OrionPatchOptions { MaxAttempts = 1 };
        var outbox = new InMemoryOutbox(storage);
        var dispatcher = new DeterministicDispatcher(storage, throwingSink, clock, options);

        outbox.Enqueue(new TestMessage("a"));
        await dispatcher.DispatchOnceAsync();

        // v0.3.0: the in-memory storage implements IDeadLetterStore, so the exhausted row is
        // ROUTED OUT of the active outbox into the dead-letter store rather than flipped in place.
        Assert.Empty(storage.Rows);
        var dead = Assert.Single(storage.DeadLetteredMessages);
        Assert.Equal(1, dead.AttemptCount);
        Assert.Contains("nope", dead.FinalError, StringComparison.Ordinal);
        Assert.Equal(clock.UtcNow, dead.DeadLetteredAtUtc);
    }

    [Fact]
    public async Task DispatchOnceAsync_ShouldDeadLetterExactlyOnce_AndNotRetry()
    {
        var storage = new InMemoryOutboxStorage();
        var throwingSink = new ThrowingSink();
        var clock = new TestClock(new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        var options = new OrionPatchOptions { MaxAttempts = 1 };
        var outbox = new InMemoryOutbox(storage);
        var dispatcher = new DeterministicDispatcher(storage, throwingSink, clock, options);

        outbox.Enqueue(new TestMessage("a"));

        // First pass dead-letters the row. Subsequent passes have nothing to claim because the
        // row has left the active outbox: the message is dead-lettered exactly once and never
        // retried again.
        await dispatcher.DispatchOnceAsync();
        var secondPass = await dispatcher.DispatchOnceAsync();
        var thirdPass = await dispatcher.DispatchOnceAsync();

        Assert.Equal(0, secondPass);
        Assert.Equal(0, thirdPass);
        Assert.Single(storage.DeadLetteredMessages);
        Assert.Empty(storage.Rows);
    }

    [Fact]
    public async Task DispatchOnceAsync_ShouldPreserveFailureContext_WhenRoutedToDeadLetter()
    {
        var storage = new InMemoryOutboxStorage();
        var throwingSink = new ThrowingSink();
        var clock = new TestClock(new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        // MaxAttempts = 3 so the row fails twice (Pending) then dead-letters on the third attempt.
        var options = new OrionPatchOptions
        {
            MaxAttempts = 3,
            BackoffStrategy = _ => TimeSpan.Zero,
        };
        var outbox = new InMemoryOutbox(storage);
        var dispatcher = new DeterministicDispatcher(storage, throwingSink, clock, options);

        outbox.Enqueue(new TestMessage("ctx"));

        await dispatcher.DispatchOnceAsync(); // attempt 1 -> Pending
        await dispatcher.DispatchOnceAsync(); // attempt 2 -> Pending
        await dispatcher.DispatchOnceAsync(); // attempt 3 -> dead-letter

        var dead = Assert.Single(storage.DeadLetteredMessages);
        Assert.Equal(3, dead.AttemptCount);
        Assert.Contains("ctx", dead.Payload, StringComparison.Ordinal);
        Assert.Contains("nope", dead.FinalError, StringComparison.Ordinal);
    }

    private sealed record TestMessage(string Greeting);

    private sealed class ThrowingSink : IOutboxSink
    {
        public Task SendAsync(OutboxEnvelope envelope, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("nope");
    }
}
