using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Configuration;
using Moongazing.OrionPatch.Models;
using Moongazing.OrionPatch.Testing;

namespace Moongazing.OrionPatch.Demo;

/// <summary>
/// v0.3.0 outbox maintenance: the dead-letter store and archival capabilities of
/// <see cref="InMemoryOutboxStorage"/>, which implements <see cref="IDeadLetterStore"/> and
/// <see cref="IOutboxArchivalStore"/> on top of <see cref="IOutboxStorage"/>.
///
/// Part 1 shows an exhausted row being routed OUT of the hot outbox into the dead-letter store
/// exactly once, carrying its final failure context. Part 2 shows processed rows past a retention
/// window being reaped out of the active outbox so it stays small.
/// </summary>
public static class DeadLetterStoreAndArchivalDemo
{
    public static async Task RunAsync()
    {
        await RunDeadLetterStoreAsync();
        await RunArchivalAsync();
    }

    private static async Task RunDeadLetterStoreAsync()
    {
        Console.WriteLine("\n== Dead-letter store: route an exhausted row out of the hot outbox ==");

        var clock = new TestClock(DateTime.UtcNow.AddHours(-1));
        var options = new OrionPatchOptions
        {
            MaxAttempts = 3,
            BackoffStrategy = BackoffStrategy.Fixed(TimeSpan.Zero),
        };

        // InMemoryOutboxStorage implements IDeadLetterStore, so the dispatcher routes an exhausted
        // row INTO the store (removing it from the active outbox) rather than flipping it in place.
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
            Console.WriteLine($"  pass {pass}: active rows remaining={storage.Rows.Count}");
        }

        // The exhausted row is no longer in the active outbox; it lives in the dead-letter store.
        var dead = await storage.GetDeadLetteredAsync();
        Console.WriteLine($"  active rows after exhaustion: {storage.Rows.Count} (the source row was routed out)");
        foreach (var message in dead)
        {
            Console.WriteLine(
                $"  dead-lettered: id={message.Id} type={message.MessageType} " +
                $"attempts={message.AttemptCount} finalError={message.FinalError}");
        }

        // Routing is idempotent on the row id: a replayed terminal-path call is a no-op.
        var firstMessage = dead[0];
        var routedAgain = await storage.DeadLetterAsync(
            firstMessage.Id,
            new DeadLetterContext("second call", firstMessage.AttemptCount, clock.UtcNow));
        Console.WriteLine($"  repeat DeadLetterAsync routed a new message: {routedAgain} (idempotent, exactly once)");
    }

    private static async Task RunArchivalAsync()
    {
        Console.WriteLine("\n== Archival: reap processed rows past the retention window ==");

        var clock = new TestClock(DateTime.UtcNow.AddDays(-30));
        var options = new OrionPatchOptions { ArchiveRetention = TimeSpan.FromDays(7) };

        var storage = new InMemoryOutboxStorage();
        var sink = new CapturingOutboxSink();
        var dispatcher = new DeterministicDispatcher(storage, sink, clock, options);

        IOutbox outbox = new InMemoryOutbox(storage);
        outbox.Enqueue(new OrderConfirmed(Guid.NewGuid(), 1000));
        outbox.Enqueue(new OrderConfirmed(Guid.NewGuid(), 2000));

        await dispatcher.DispatchOnceAsync();
        Console.WriteLine($"  dispatched and processed rows in active outbox: {storage.Rows.Count}");

        // Advance the clock past the 7-day retention window measured from ProcessedAtUtc, then reap.
        var nowUtc = clock.UtcNow.AddDays(10);
        var reaped = await storage.ArchiveProcessedAsync(options.ArchiveRetention, nowUtc);
        Console.WriteLine($"  ArchiveProcessedAsync reaped: {reaped} processed rows");
        Console.WriteLine($"  active outbox after reap: {storage.Rows.Count} rows");

        // Default in-memory storage archives (rather than purges); the moved rows are observable.
        var archived = await storage.GetArchivedAsync();
        Console.WriteLine($"  archived rows retained for inspection: {archived.Count}");

        // The reap is idempotent and incremental: a second pass affects nothing new.
        var secondReap = await storage.ArchiveProcessedAsync(options.ArchiveRetention, nowUtc);
        Console.WriteLine($"  second ArchiveProcessedAsync pass reaped: {secondReap} (idempotent)");
    }
}
