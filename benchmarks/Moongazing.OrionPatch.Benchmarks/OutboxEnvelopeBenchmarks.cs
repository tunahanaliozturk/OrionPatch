namespace Moongazing.OrionPatch.Benchmarks;

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Moongazing.OrionPatch.Models;

/// <summary>
/// Measures the allocation cost of materializing an <see cref="OutboxEnvelope"/> record. The
/// dispatcher constructs one envelope per row just before invoking the sink, so this is the
/// per-message allocation on the dispatch hot loop. Headers are exercised separately because the
/// optional dictionary is the main driver of variation in envelope size.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(RuntimeMoniker.Net80)]
[SimpleJob(RuntimeMoniker.Net90)]
public class OutboxEnvelopeBenchmarks
{
    private static readonly Guid Id = Guid.NewGuid();
    private static readonly DateTime OccurredAt = DateTime.UtcNow;

    private static readonly IReadOnlyDictionary<string, string> Headers =
        new Dictionary<string, string>
        {
            ["correlation-id"] = "9f1c8e2a-0b3d-4e5f-8a7b-1c2d3e4f5a6b",
            ["tenant"] = "acme",
            ["source"] = "orders-service",
        };

    private const string Payload =
        "{\"orderId\":\"9f1c8e2a-0b3d-4e5f-8a7b-1c2d3e4f5a6b\",\"total\":129.95,\"currency\":\"EUR\"}";

    [Benchmark(Baseline = true)]
    public OutboxEnvelope CreateWithoutHeaders() =>
        new(Id, "OrderShipped", Payload, Headers: null, CorrelationId: null, OccurredAt, AttemptNumber: 1);

    [Benchmark]
    public OutboxEnvelope CreateWithHeaders() =>
        new(Id, "OrderShipped", Payload, Headers, CorrelationId: "corr-42", OccurredAt, AttemptNumber: 1);
}
