using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Configuration;
using Moongazing.OrionPatch.Models;
using Moongazing.OrionPatch.Testing;

namespace Moongazing.OrionPatch.Demo;

/// <summary>
/// Terminal failure: a permanently failing sink exhausts MaxAttempts. The dispatcher retries up to
/// the budget, then routes the row out of the active outbox so it stops being re-claimed until an
/// operator intervenes. This is the safety valve that keeps a poison message from looping forever.
///
/// As of v0.3.0 <see cref="InMemoryOutboxStorage"/> implements <see cref="IDeadLetterStore"/>, so the
/// exhausted row is moved INTO the dead-letter store (and removed from the active outbox) rather than
/// flipped to DeadLettered in place. See <see cref="DeadLetterStoreAndArchivalDemo"/> for the store
/// and archival APIs in full.
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
            var active = storage.Rows.SingleOrDefault();
            Console.WriteLine(active is null
                ? $"  pass {pass}: row routed out of the active outbox"
                : $"  pass {pass}: status={active.Status} attemptCount={active.AttemptCount}");
        }

        // The exhausted row was routed into the dead-letter store and is no longer in the active outbox.
        var dead = await storage.GetDeadLetteredAsync();
        Console.WriteLine($"  active rows remaining: {storage.Rows.Count} (the poison row was routed out)");
        foreach (var message in dead)
        {
            Console.WriteLine(
                $"  dead-lettered: id={message.Id} attempts={message.AttemptCount} finalError={message.FinalError}");
        }
        Console.WriteLine($"  queue depth (Pending only): {await storage.QueueDepthAsync()} (dead-lettered rows are excluded)");
    }
}
