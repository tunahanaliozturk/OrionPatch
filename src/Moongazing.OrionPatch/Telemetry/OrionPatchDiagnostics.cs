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
    /// v0.2.28 idle-poll counter. Increments each dispatcher cycle that claims an empty batch
    /// (the backlog was empty). Operators graph the idle-poll rate against the total poll rate
    /// to answer "is the dispatcher polling too often for the actual traffic? raise
    /// PollingInterval" - a high idle fraction is a cost-of-poll signal, while a low fraction
    /// means the dispatcher is busy and BatchSize may need raising instead. Pairs with the
    /// v0.2.16 batch_size histogram, which deliberately skips these zero-row cycles. Mirrors the
    /// Guard v6.5.17 poll.idle counter on the Patch side.
    /// </summary>
    public static readonly Counter<long> IdlePolls =
        Meter.CreateCounter<long>("orionpatch.outbox.poll.idle", unit: "{polls}");

    /// <summary>Record one idle poll (empty-backlog cycle). Public for consumer-owned dispatchers.</summary>
    public static void RecordIdlePoll() => IdlePolls.Add(1);

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

    /// <summary>
    /// v0.2.29 distribution of how long a row spent in the outbox before it was dead-lettered
    /// (the gap between <c>OutboxRow.EnqueuedAtUtc</c> and the moment it exhausted
    /// <c>MaxAttempts</c>). The failure-path analog to the v0.2.21 <c>queue_lag</c> success
    /// histogram. Operators graph p99 to tune the retry policy: a dead-letter age that tracks
    /// <c>MaxAttempts</c> x the backoff schedule means rows are exhausting genuine transient
    /// retries, whereas a much shorter age means rows are failing fast on terminal errors and the
    /// retry budget is being spent pointlessly.
    /// </summary>
    public static readonly Histogram<double> DeadLetterAge =
        Meter.CreateHistogram<double>("orionpatch.outbox.dead_letter.age_ms", unit: "ms");

    /// <summary>
    /// Record the outbox dwell time of a dead-lettered row. Negative inputs are clamped to 0 so
    /// clock skew across enqueue / dispatcher hosts does not pull the histogram p50 down.
    /// </summary>
    public static void RecordDeadLetterAge(double milliseconds)
        => DeadLetterAge.Record(System.Math.Max(0d, milliseconds));

    /// <summary>
    /// v0.2.23 distribution of how many attempts each row took before reaching its
    /// terminal state (success or dead-letter). Operators graph p99 to spot rows that
    /// burn most of MaxAttempts before stabilising - a signal that BackoffStrategy
    /// needs tuning OR that the sink is flapping under specific message types.
    /// </summary>
    public static readonly Histogram<int> AttemptsPerRow =
        Meter.CreateHistogram<int>("orionpatch.outbox.attempts_per_row", unit: "{attempts}");

    /// <summary>Record a row's final attempt count. Public for consumer-owned dispatchers.</summary>
    public static void RecordAttemptsPerRow(int attempts)
    {
        if (attempts <= 0)
        {
            return;
        }
        AttemptsPerRow.Record(attempts);
    }

    /// <summary>
    /// v0.2.24 distribution of dispatched envelope payload size in bytes (the JSON
    /// Payload string length). Operators graph p99 to spot a message-type whose
    /// payload grew suddenly (regression in the producer, accidental large fan-out)
    /// and to size storage column types / broker frame limits against actual byte
    /// shape rather than guessing. Recorded on the success path so failed sends do
    /// not skew the distribution.
    /// </summary>
    public static readonly Histogram<int> EnvelopeBytes =
        Meter.CreateHistogram<int>("orionpatch.outbox.dispatch.envelope_bytes", unit: "By");

    /// <summary>Record a successfully-dispatched envelope's payload byte length.</summary>
    public static void RecordEnvelopeBytes(int bytes)
    {
        if (bytes <= 0)
        {
            return;
        }
        EnvelopeBytes.Record(bytes);
    }

    /// <summary>
    /// v0.2.25 distribution of <c>IOutboxSink.SendAsync</c> wall-clock per envelope.
    /// Operators graph p99 to isolate the broker / downstream call cost from the
    /// existing <c>dispatch.duration</c> which covers EVERYTHING (deserialize +
    /// envelope build + sink + complete). try/finally so a slow failing sink still
    /// emits the sample.
    /// </summary>
    public static readonly Histogram<double> SinkDuration =
        Meter.CreateHistogram<double>("orionpatch.outbox.sink.duration_ms", unit: "ms");

    /// <summary>Record a sink call's wall-clock. Negatives clamped to 0.</summary>
    public static void RecordSinkDuration(double milliseconds)
        => SinkDuration.Record(System.Math.Max(0d, milliseconds));

    /// <summary>
    /// v0.2.27 distribution of the claim batch fill ratio: claimed rows divided by the
    /// configured BatchSize, as a 0..1 double. Operators graph p99 to right-size
    /// BatchSize: a ratio steadily near 1.0 means the dispatcher is BatchSize-bound
    /// (raise it for throughput), while a ratio steadily near 0 means BatchSize is
    /// over-provisioned for the actual backlog (the absolute batch_size histogram
    /// cannot answer this without the operator knowing the configured BatchSize).
    /// Only non-empty claims emit.
    /// </summary>
    public static readonly Histogram<double> ClaimBatchFillRatio =
        Meter.CreateHistogram<double>("orionpatch.outbox.claim.batch_fill_ratio", unit: "1");

    /// <summary>Record a claim's fill ratio (claimed / batchSize). Clamped to [0, 1].</summary>
    public static void RecordClaimBatchFillRatio(int claimed, int batchSize)
    {
        if (claimed <= 0 || batchSize <= 0)
        {
            return;
        }
        var ratio = (double)claimed / batchSize;
        ClaimBatchFillRatio.Record(ratio > 1d ? 1d : ratio);
    }

    // v0.2.26 queue depth snapshot, fed by the dispatcher each poll cycle via
    // IOutboxStorage.QueueDepthAsync. Mirrors v0.7.x OrionAudit queue_depth so the
    // two outbox families expose the same liveness shape.
    private static long queueDepth;

    /// <summary>v0.2.26: update the queue depth snapshot. Dispatchers call once per cycle.</summary>
    public static void SetQueueDepth(long depth)
        => System.Threading.Interlocked.Exchange(ref queueDepth, depth);

    /// <summary>
    /// v0.2.26 pending-row gauge. Reports the most recent dispatcher observation of
    /// <c>IOutboxStorage.QueueDepthAsync</c>; 0 until the first cycle completes.
    /// Operators alert on sustained growth (dispatcher cannot keep up with producers).
    /// </summary>
    public static readonly ObservableGauge<long> QueueDepth = Meter.CreateObservableGauge<long>(
        "orionpatch.outbox.queue_depth",
        () => System.Threading.Interlocked.Read(ref queueDepth),
        unit: "{rows}",
        description: "Pending outbox rows awaiting dispatch, as last observed by the dispatcher.");
}
