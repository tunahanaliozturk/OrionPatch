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

    /// <summary>
    /// v0.2.19 counter for the v0.2.18 <see cref="Abstractions.IDeadLetterSink"/>
    /// observer faults. Increments each time the sink throws; the dead-letter database
    /// state is still applied (the sink is observability, not load-bearing) so this
    /// metric is purely operator-facing alerting for "your DLQ notifier is down".
    /// Tagged with <c>exception_type</c>.
    /// </summary>
    public static readonly Counter<long> DeadLetterSinkFailures =
        Meter.CreateCounter<long>("orionpatch.outbox.dead_letter_sink_failures", unit: "{failures}");

    /// <summary>Record a <see cref="Abstractions.IDeadLetterSink"/> failure tagged with the exception type.</summary>
    public static void RecordDeadLetterSinkFailure(string exceptionType)
        => DeadLetterSinkFailures.Add(1,
            new System.Collections.Generic.KeyValuePair<string, object?>("exception_type", exceptionType));

    /// <summary>
    /// v0.2.20 counter for the v0.2.20 <see cref="Abstractions.IOutboxDispatchObserver"/>
    /// failures. Mirrors the v0.2.19 dead-letter-sink failures counter on the success
    /// observer path. The row is already completed (db state is correct); this counter
    /// is purely operator-facing alerting for "your success-observer is misbehaving".
    /// Tagged with <c>exception_type</c>.
    /// </summary>
    public static readonly Counter<long> DispatchObserverFailures =
        Meter.CreateCounter<long>("orionpatch.outbox.dispatch_observer_failures", unit: "{failures}");

    /// <summary>Record a dispatch-observer failure tagged with the exception type.</summary>
    public static void RecordDispatchObserverFailure(string exceptionType)
        => DispatchObserverFailures.Add(1,
            new System.Collections.Generic.KeyValuePair<string, object?>("exception_type", exceptionType));

    /// <summary>
    /// v0.2.21 distribution of per-row dispatch lag in milliseconds (the gap between
    /// <c>OutboxRow.OccurredAtUtc</c> and successful dispatch). Mirrors v6.5.16
    /// <c>orionguard.outbox.dispatcher.queue_lag</c> for the Patch dispatcher.
    /// Operators graph p50/p99 to spot a dispatcher whose queue is backing up before
    /// the steady-state dispatched-count rate visibly slows.
    /// </summary>
    public static readonly Histogram<double> QueueLag =
        Meter.CreateHistogram<double>("orionpatch.outbox.queue_lag", unit: "ms");

    /// <summary>
    /// Record one row's queue lag. Negative inputs are clamped to 0 so clock skew across
    /// enqueue / dispatcher hosts does not pull the histogram p50 down.
    /// </summary>
    public static void RecordQueueLag(double milliseconds)
        => QueueLag.Record(System.Math.Max(0d, milliseconds));
}
