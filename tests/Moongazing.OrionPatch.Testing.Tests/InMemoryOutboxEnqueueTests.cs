namespace Moongazing.OrionPatch.Testing.Tests;

using Moongazing.OrionPatch.Models;
using Xunit;

public class InMemoryOutboxEnqueueTests
{
    [Fact]
    public async Task Enqueue_stamps_real_write_time_in_EnqueuedAtUtc_even_when_OccurredAtUtc_is_backdated()
    {
        var storage = new InMemoryOutboxStorage();
        var outbox = new InMemoryOutbox(storage);
        var backdated = DateTime.UtcNow - TimeSpan.FromHours(6);

        outbox.Enqueue(new TestMessage("x"), new OutboxEnqueueOptions { OccurredAtUtc = backdated });

        var claimed = await storage.ClaimNextAsync(10, "t", TimeSpan.FromMinutes(1));
        var row = Assert.Single(claimed);

        // OccurredAtUtc keeps the caller's backdate; EnqueuedAtUtc is the real write time, so the
        // enqueue-based telemetry (pickup_lag_ms, dead_letter.age_ms) measures outbox dwell rather
        // than the 6h backdate.
        Assert.Equal(backdated, row.OccurredAtUtc);
        Assert.True(row.EnqueuedAtUtc > row.OccurredAtUtc);
        Assert.True(row.EnqueuedAtUtc >= DateTime.UtcNow - TimeSpan.FromMinutes(1));
    }

    private sealed record TestMessage(string Value);
}
