using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Models;
using Moongazing.OrionPatch.Testing;

namespace Moongazing.OrionPatch.Demo;

/// <summary>
/// Drives the <see cref="IOutboxStorage"/> SPI by hand to show the row lifecycle transitions the
/// dispatcher normally performs: Append (Pending) -> ClaimNext (Claimed, leased) -> Complete
/// (Processed). A second row shows Fail returning a row to Pending with an incremented attempt
/// count and a NextAttemptAtUtc anchor.
/// </summary>
public static class RowLifecycleDemo
{
    public static async Task RunAsync()
    {
        Console.WriteLine("\n== Outbox row lifecycle (claim / complete / fail) ==");

        IOutboxStorage storage = new InMemoryOutboxStorage();
        var inspect = (InMemoryOutboxStorage)storage;

        var happyId = Guid.NewGuid();
        var failId = Guid.NewGuid();
        var now = DateTime.UtcNow;

        await storage.AppendAsync(new[]
        {
            NewPending(happyId, "order.confirmed", now),
            NewPending(failId, "order.shipped.v2", now),
        });
        Console.WriteLine($"  appended 2 rows; statuses: {string.Join(", ", inspect.Rows.Select(r => $"{r.MessageType}={r.Status}"))}");

        // Claim under a dispatcher identity for a 2-minute lease.
        var claimed = await storage.ClaimNextAsync(batchSize: 10, "demo-dispatcher", TimeSpan.FromMinutes(2));
        Console.WriteLine($"  claimed {claimed.Count} row(s) under lease; each now Claimed by '{claimed.First().ClaimedBy}'");

        // Complete the happy row.
        await storage.CompleteAsync(happyId, DateTime.UtcNow);
        var happy = inspect.Rows.Single(r => r.Id == happyId);
        Console.WriteLine($"  completed {happy.MessageType}: status={happy.Status} processedAt={happy.ProcessedAtUtc:O}");

        // Fail the other row: it returns to Pending with attemptCount++ and a backoff anchor.
        await storage.FailAsync(failId, "downstream 503", DateTime.UtcNow.AddSeconds(30));
        var failed = inspect.Rows.Single(r => r.Id == failId);
        Console.WriteLine(
            $"  failed {failed.MessageType}: status={failed.Status} attemptCount={failed.AttemptCount} " +
            $"nextAttempt~+30s lastError='{failed.LastError}'");

        // Dead-letter the same row to show the terminal transition.
        await storage.DeadLetterAsync(failId, "exhausted retries");
        var dead = inspect.Rows.Single(r => r.Id == failId);
        Console.WriteLine($"  dead-lettered {dead.MessageType}: status={dead.Status} attemptCount={dead.AttemptCount}");

        Console.WriteLine($"  final pending queue depth: {await storage.QueueDepthAsync()}");
    }

    private static OutboxRow NewPending(Guid id, string messageType, DateTime now) => new()
    {
        Id = id,
        MessageType = messageType,
        Payload = "{}",
        OccurredAtUtc = now,
        EnqueuedAtUtc = now,
        Status = OutboxStatus.Pending,
        AttemptCount = 0,
        NextAttemptAtUtc = now,
    };
}
