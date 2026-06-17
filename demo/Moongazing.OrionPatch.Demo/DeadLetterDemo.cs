using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Configuration;
using Moongazing.OrionPatch.Models;
using Moongazing.OrionPatch.Testing;

namespace Moongazing.OrionPatch.Demo;

/// <summary>
/// Terminal failure: a permanently failing sink exhausts MaxAttempts. The dispatcher retries up to
/// the budget, then flips the row to DeadLettered so it stops being re-claimed until an operator
/// intervenes. This is the safety valve that keeps a poison message from looping forever.
/// </summary>
public static class DeadLetterDemo
{
    public static async Task RunAsync()
    {
        Console.WriteLine("\n== Dead-letter after exhausting MaxAttempts ==");

        var clock = new TestClock(DateTime.UtcNow.AddHours(-1));
        var options = new OrionPatchOptions
        {
            MaxAttempts = 3,
            BackoffStrategy = BackoffStrategy.Fixed(TimeSpan.Zero),
        };

        var storage = new InMemoryOutboxStorage();
        var sink = new AlwaysFailingOutboxSink();
        var dispatcher = new DeterministicDispatcher(storage, sink, clock, options);

        IOutbox outbox = new InMemoryOutbox(storage);
        outbox.Enqueue(new OrderConfirmed(Guid.NewGuid(), 7777));
        Console.WriteLine($"  enqueued one OrderConfirmed; MaxAttempts={options.MaxAttempts}");

        var pass = 0;
        while (storage.Rows.Any(r => r.Status is OutboxStatus.Pending or OutboxStatus.Claimed) && pass < 10)
        {
            pass++;
            await dispatcher.DispatchOnceAsync();
            var row = storage.Rows.Single();
            Console.WriteLine($"  pass {pass}: status={row.Status} attemptCount={row.AttemptCount}");
        }

        var final = storage.Rows.Single();
        Console.WriteLine(
            $"  final status={final.Status} attemptCount={final.AttemptCount} lastError={final.LastError}");
        Console.WriteLine($"  queue depth (Pending only): {await storage.QueueDepthAsync()} (dead-lettered rows are excluded)");
    }
}
