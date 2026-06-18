using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Channels;
using Moongazing.OrionPatch.Models;
using Moongazing.OrionPatch.Testing;

namespace Moongazing.OrionPatch.Demo;

/// <summary>
/// The one concrete sink shipped at v0.1.0: <see cref="ChannelOutboxSink"/>, an in-process
/// System.Threading.Channels pipe. We dispatch envelopes into it and drain the reader side from a
/// consumer loop - the in-memory equivalent of "messages flowing from the transaction to a broker",
/// with zero external dependency.
/// </summary>
public static class ChannelSinkDemo
{
    public static async Task RunAsync()
    {
        Console.WriteLine("\n== In-process channel sink fan-out ==");

        var sink = new ChannelOutboxSink(new ChannelOutboxSinkOptions { Capacity = 16 });
        var storage = new InMemoryOutboxStorage();
        var clock = new TestClock();
        var dispatcher = new DeterministicDispatcher(storage, sink, clock);

        IOutbox outbox = new InMemoryOutbox(storage);
        var payments = new[]
        {
            new PaymentCaptured(Guid.NewGuid(), 1500),
            new PaymentCaptured(Guid.NewGuid(), 3200),
            new PaymentCaptured(Guid.NewGuid(), 8000),
        };
        foreach (var p in payments)
        {
            outbox.Enqueue(p);
        }
        Console.WriteLine($"  enqueued {payments.Length} PaymentCaptured events");

        // Start a consumer that drains the channel reader concurrently with dispatch.
        var expected = payments.Length;
        var consumer = Task.Run(async () =>
        {
            var count = 0;
            await foreach (var env in sink.Reader.ReadAllAsync())
            {
                count++;
                Console.WriteLine($"    consumer received {env.MessageType} payload={env.Payload}");
                if (count >= expected)
                {
                    break;
                }
            }
            return count;
        });

        var dispatched = await dispatcher.DispatchOnceAsync();
        Console.WriteLine($"  dispatcher pushed {dispatched} envelope(s) into the channel");

        var consumed = await consumer;
        Console.WriteLine($"  consumer drained {consumed} envelope(s) from the channel reader");
    }
}
