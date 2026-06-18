using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Configuration;
using Moongazing.OrionPatch.Models;
using Moongazing.OrionPatch.Testing;

namespace Moongazing.OrionPatch.Demo;

/// <summary>
/// Retry-with-backoff: a flaky sink fails the first two attempts and succeeds on the third.
/// Each failure pushes the row back to Pending with an incremented AttemptCount and a
/// NextAttemptAtUtc anchored by the backoff strategy. We print the exponential backoff schedule,
/// then drive successive dispatch passes until the row lands Processed.
/// </summary>
public static class RetryBackoffDemo
{
    public static async Task RunAsync()
    {
        Console.WriteLine("\n== Retry + backoff on a failing sink ==");

        // Show what the default exponential schedule (1s doubling, capped at 30 min) produces.
        var schedule = BackoffStrategy.Exponential(TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(30));
        Console.WriteLine("  exponential schedule (1s base, 30m cap):");
        for (var attempt = 1; attempt <= 5; attempt++)
        {
            Console.WriteLine($"    attempt {attempt} -> wait {schedule(attempt)}");
        }

        // Anchor the clock in the past so that NextAttemptAtUtc (clock.UtcNow + backoff) stays at or
        // before real wall-clock now and the in-memory storage immediately re-claims on the next pass.
        // This keeps the demo deterministic instead of sleeping through real backoff windows.
        var clock = new TestClock(DateTime.UtcNow.AddHours(-1));
        var options = new OrionPatchOptions
        {
            MaxAttempts = 5,
            BackoffStrategy = BackoffStrategy.Fixed(TimeSpan.Zero),
        };

        var storage = new InMemoryOutboxStorage();
        var sink = new FlakyOutboxSink(failuresBeforeSuccess: 2);
        var dispatcher = new DeterministicDispatcher(storage, sink, clock, options);

        IOutbox outbox = new InMemoryOutbox(storage);
        outbox.Enqueue(new OrderConfirmed(Guid.NewGuid(), 4200));
        Console.WriteLine("  enqueued one OrderConfirmed that the sink will reject twice");

        var pass = 0;
        while (storage.Rows.Any(r => r.Status is OutboxStatus.Pending or OutboxStatus.Claimed))
        {
            pass++;
            Console.WriteLine($"  -- dispatch pass {pass} --");
            await dispatcher.DispatchOnceAsync();

            var row = storage.Rows.Single();
            Console.WriteLine(
                $"    row status={row.Status} attemptCount={row.AttemptCount} lastError={row.LastError ?? "(none)"}");

            if (pass > 10)
            {
                Console.WriteLine("    safety break (unexpected)");
                break;
            }
        }

        var final = storage.Rows.Single();
        Console.WriteLine($"  final status={final.Status} after {final.AttemptCount} recorded failure(s)");
    }
}
