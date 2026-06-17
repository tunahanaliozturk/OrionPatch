namespace Moongazing.OrionPatch.Benchmarks;

using System.Threading.Channels;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Moongazing.OrionPatch.Channels;
using Moongazing.OrionPatch.Models;

/// <summary>
/// Measures the in-process dispatch loop through <see cref="ChannelOutboxSink"/>: write N
/// envelopes via <see cref="ChannelOutboxSink.SendAsync"/> and drain them via the
/// <see cref="ChannelOutboxSink.Reader"/>. This is the only sink shipped in the core library and
/// stands in for the producer/consumer throughput of a monolith fan-out. No broker, no I/O.
/// Capacity is sized above the batch so writes never block, isolating the channel's enqueue and
/// drain cost from back-pressure effects.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(RuntimeMoniker.Net80)]
[SimpleJob(RuntimeMoniker.Net90)]
public class ChannelOutboxSinkBenchmarks
{
    private OutboxEnvelope[] envelopes = default!;

    [Params(1, 100, 1000)]
    public int MessageCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        envelopes = new OutboxEnvelope[MessageCount];
        var occurredAt = DateTime.UtcNow;
        for (int i = 0; i < MessageCount; i++)
        {
            envelopes[i] = new OutboxEnvelope(
                Guid.NewGuid(),
                "OrderShipped",
                "{\"i\":" + i + "}",
                Headers: null,
                CorrelationId: null,
                occurredAt,
                AttemptNumber: 1);
        }
    }

    [Benchmark]
    public async Task<int> SendAndDrain()
    {
        // Capacity strictly above MessageCount so SendAsync never back-pressures.
        var sink = new ChannelOutboxSink(new ChannelOutboxSinkOptions
        {
            Capacity = MessageCount + 1,
            FullMode = BoundedChannelFullMode.Wait,
        });

        for (int i = 0; i < envelopes.Length; i++)
        {
            await sink.SendAsync(envelopes[i]).ConfigureAwait(false);
        }

        int drained = 0;
        while (drained < envelopes.Length && sink.Reader.TryRead(out _))
        {
            drained++;
        }

        return drained;
    }
}
