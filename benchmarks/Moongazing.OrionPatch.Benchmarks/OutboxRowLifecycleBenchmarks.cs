namespace Moongazing.OrionPatch.Benchmarks;

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Moongazing.OrionPatch.Models;

/// <summary>
/// Measures construction of an <see cref="OutboxRow"/> plus the in-memory claim/complete and
/// claim/fail lifecycle mutations the dispatcher applies to each row it processes. These
/// transitions (Pending to Claimed to Processed, or Pending to Claimed to Pending-with-backoff)
/// are the per-row state churn on the dispatcher loop, independent of any storage backend.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(RuntimeMoniker.Net80)]
[SimpleJob(RuntimeMoniker.Net90)]
public class OutboxRowLifecycleBenchmarks
{
    private static readonly DateTime Now = DateTime.UtcNow;
    private const string Identity = "host-01/4242";

    private static OutboxRow NewPendingRow() => new()
    {
        Id = Guid.NewGuid(),
        MessageType = "OrderShipped",
        Payload = "{\"orderId\":42}",
        CorrelationId = "corr-42",
        OccurredAtUtc = Now,
        EnqueuedAtUtc = Now,
        Status = OutboxStatus.Pending,
        AttemptCount = 0,
    };

    [Benchmark(Baseline = true)]
    public OutboxRow Construct() => NewPendingRow();

    [Benchmark]
    public OutboxRow ClaimThenComplete()
    {
        var row = NewPendingRow();

        // Claim transition.
        row.Status = OutboxStatus.Claimed;
        row.ClaimedAtUtc = Now;
        row.ClaimedBy = Identity;
        row.AttemptCount++;

        // Complete transition.
        row.Status = OutboxStatus.Processed;
        row.ProcessedAtUtc = Now;

        return row;
    }

    [Benchmark]
    public OutboxRow ClaimThenFailWithBackoff()
    {
        var row = NewPendingRow();

        // Claim transition.
        row.Status = OutboxStatus.Claimed;
        row.ClaimedAtUtc = Now;
        row.ClaimedBy = Identity;
        row.AttemptCount++;

        // Fail transition: release the claim and schedule a retry.
        row.Status = OutboxStatus.Pending;
        row.ClaimedAtUtc = null;
        row.ClaimedBy = null;
        row.LastError = "sink unavailable";
        row.NextAttemptAtUtc = Now.AddSeconds(2);

        return row;
    }
}
