namespace Moongazing.OrionPatch.Telemetry;

using System.Diagnostics;
using System.Diagnostics.Metrics;

/// <summary>
/// Public diagnostic surface for OrionPatch. Subscribe to <see cref="ActivitySource"/>
/// or <see cref="Meter"/> via the standard OpenTelemetry .NET helpers; the source name
/// is also exposed as a string constant for static configuration.
/// </summary>
public static class OrionPatchDiagnostics
{
    /// <summary>The shared name used for both <see cref="ActivitySource"/> and <see cref="Meter"/>.</summary>
    public const string SourceName = "Moongazing.OrionPatch";

    /// <summary><see cref="System.Diagnostics.ActivitySource"/> the dispatcher writes spans into.</summary>
    public static readonly ActivitySource ActivitySource = new(SourceName);

    /// <summary><see cref="System.Diagnostics.Metrics.Meter"/> the dispatcher emits counters and histograms into.</summary>
    public static readonly Meter Meter = new(SourceName);

    /// <summary>Count of messages enqueued via <see cref="Abstractions.IOutbox.Enqueue{T}"/>.</summary>
    public static readonly Counter<long> Enqueued = Meter.CreateCounter<long>("orionpatch.outbox.enqueued");

    /// <summary>Count of envelopes successfully dispatched to the sink.</summary>
    public static readonly Counter<long> Dispatched = Meter.CreateCounter<long>("orionpatch.outbox.dispatched");

    /// <summary>Count of dispatch attempts that failed (still retryable).</summary>
    public static readonly Counter<long> Failed = Meter.CreateCounter<long>("orionpatch.outbox.failed");

    /// <summary>Count of rows that exhausted <see cref="Configuration.OrionPatchOptions.MaxAttempts"/> and were dead-lettered.</summary>
    public static readonly Counter<long> DeadLettered = Meter.CreateCounter<long>("orionpatch.outbox.deadlettered");

    /// <summary>Count of total dispatch attempts (successes + failures combined).</summary>
    public static readonly Counter<long> Attempts = Meter.CreateCounter<long>("orionpatch.outbox.attempts");

    /// <summary>Per-envelope sink dispatch duration in milliseconds.</summary>
    public static readonly Histogram<double> DispatchDuration =
        Meter.CreateHistogram<double>("orionpatch.outbox.dispatch.duration", unit: "ms");

    /// <summary>
    /// v0.2.16 distribution of rows claimed per dispatcher cycle. Operators graph p99
    /// to spot a dispatcher that is consistently maxing out <c>BatchSize</c> (a sign
    /// that throughput is bottlenecked and the batch should be raised) or staying near
    /// 0 (a sign that polling cadence is over-sized for the actual traffic).
    /// Zero-row cycles do NOT emit.
    /// </summary>
    public static readonly Histogram<int> BatchSize =
        Meter.CreateHistogram<int>("orionpatch.outbox.batch_size", unit: "{rows}");

    /// <summary>
    /// v0.2.17 distribution of how long `IOutboxStorage.ClaimNextAsync` takes per
    /// dispatcher cycle (the storage round-trip wall-clock). Operators graph p99 to
    /// spot a storage backend that is slow to claim rows. EVERY cycle emits including
    /// zero-row cycles - poll latency is itself the signal.
    /// </summary>
    public static readonly Histogram<double> PollDuration =
        Meter.CreateHistogram<double>("orionpatch.outbox.poll.duration", unit: "ms");
}
