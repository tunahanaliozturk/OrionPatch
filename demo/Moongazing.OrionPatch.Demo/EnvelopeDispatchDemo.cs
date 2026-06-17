using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Models;
using Moongazing.OrionPatch.Testing;

namespace Moongazing.OrionPatch.Demo;

/// <summary>
/// The happy path: enqueue messages into in-memory storage, run one deterministic dispatch pass,
/// and watch each row materialize into an <see cref="OutboxEnvelope"/> handed to the sink.
/// No EF Core, no broker, no background threads - the dispatcher is driven explicitly.
/// </summary>
public static class EnvelopeDispatchDemo
{
    public static async Task RunAsync()
    {
        Console.WriteLine("\n== Envelope dispatch (enqueue -> claim -> send -> complete) ==");

        var storage = new InMemoryOutboxStorage();
        var sink = new CapturingOutboxSink();
        var clock = new TestClock();
        var outbox = new InMemoryOutbox(storage);
        var dispatcher = new DeterministicDispatcher(storage, sink, clock);

        IOutbox enqueue = outbox;
        foreach (var cents in new[] { 100, 250, 999 })
        {
            var evt = new OrderConfirmed(Guid.NewGuid(), cents);
            enqueue.Enqueue(evt, new OutboxEnqueueOptions
            {
                CorrelationId = $"corr-{cents}",
                Headers = new Dictionary<string, string> { ["tenant"] = "acme" },
            });
            Console.WriteLine($"  enqueued OrderConfirmed OrderId={evt.OrderId} TotalCents={cents}");
        }

        Console.WriteLine($"  queue depth before dispatch: {await storage.QueueDepthAsync()}");

        var dispatched = await dispatcher.DispatchOnceAsync();
        Console.WriteLine($"  dispatcher processed {dispatched} row(s) in one pass");

        foreach (var env in sink.Sent)
        {
            var tenant = env.Headers is not null && env.Headers.TryGetValue("tenant", out var t) ? t : "(none)";
            Console.WriteLine(
                $"  envelope Id={env.Id} type={env.MessageType} attempt={env.AttemptNumber} " +
                $"corr={env.CorrelationId} tenant={tenant} payload={env.Payload}");
        }

        Console.WriteLine($"  queue depth after dispatch: {await storage.QueueDepthAsync()}");
        Console.WriteLine($"  all rows Processed: {storage.Rows.All(r => r.Status == OutboxStatus.Processed)}");
    }
}
