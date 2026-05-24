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
    public async Task DispatchOnceAsync_ShouldDeadLetter_WhenAttemptExceedsMax()
    {
        var storage = new InMemoryOutboxStorage();
        var throwingSink = new ThrowingSink();
        var clock = new TestClock(new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        var options = new OrionPatchOptions { MaxAttempts = 1 };
        var outbox = new InMemoryOutbox(storage);
        var dispatcher = new DeterministicDispatcher(storage, throwingSink, clock, options);

        outbox.Enqueue(new TestMessage("a"));
        await dispatcher.DispatchOnceAsync();

        var stored = Assert.Single(storage.Rows);
        Assert.Equal(OutboxStatus.DeadLettered, stored.Status);
    }

    private sealed record TestMessage(string Greeting);

    private sealed class ThrowingSink : IOutboxSink
    {
        public Task SendAsync(OutboxEnvelope envelope, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("nope");
    }
}
