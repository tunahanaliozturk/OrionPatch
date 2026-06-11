namespace Moongazing.OrionPatch.Hosting;

using System.Diagnostics;
using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Configuration;
using Moongazing.OrionPatch.Models;
using Moongazing.OrionPatch.Telemetry;

/// <summary>
/// Background service that polls <see cref="IOutboxStorage"/> for claimable rows,
/// dispatches each through <see cref="IOutboxSink"/> at-least-once, and applies
/// the configured retry + dead-letter policy on failure.
/// </summary>
/// <remarks>
/// Delivery is at-least-once. Duplicates can occur in two known scenarios: (1) the sink
/// succeeds but the subsequent <see cref="IOutboxStorage.CompleteAsync"/> write fails or
/// the process crashes before it runs, so the row stays <see cref="Models.OutboxStatus.Claimed"/>,
/// the lease expires, and another dispatcher re-delivers; (2) the sink call exceeds
/// <see cref="OrionPatchOptions.LeaseDuration"/>, allowing another dispatcher to claim the
/// same row mid-flight. Consumer sinks MUST therefore be idempotent — typically by
/// deduplicating on <see cref="Models.OutboxEnvelope.Id"/> at the destination, or by
/// treating writes as upserts.
/// </remarks>
public sealed partial class OutboxDispatcherHostedService : BackgroundService
{
    private readonly IOutboxStorage storage;
    private readonly IOutboxSink sink;
    private readonly IOptions<OrionPatchOptions> options;
    private readonly IOutboxDispatcherClock clock;
    private readonly ILogger<OutboxDispatcherHostedService> logger;
    // v0.2.18 consumer-supplied dead-letter observer. Optional - null/NullDeadLetterSink
    // is the back-compat default that v0.2.17 consumers see.
    private readonly IDeadLetterSink? deadLetterSink;

    /// <summary>Create the dispatcher with its storage, sink, options, clock, and logger.</summary>
    /// <param name="storage">Outbox row store.</param>
    /// <param name="sink">Destination sink invoked per dispatched envelope.</param>
    /// <param name="options">OrionPatch options snapshot.</param>
    /// <param name="clock">Clock + delay abstraction; tests substitute a controllable instance.</param>
    /// <param name="logger">Logger for loop-level failures.</param>
    /// <exception cref="ArgumentNullException">Thrown when any constructor argument is null.</exception>
    public OutboxDispatcherHostedService(
        IOutboxStorage storage,
        IOutboxSink sink,
        IOptions<OrionPatchOptions> options,
        IOutboxDispatcherClock clock,
        ILogger<OutboxDispatcherHostedService> logger)
        : this(storage, sink, options, clock, logger, deadLetterSink: null)
    {
    }

    /// <summary>v0.2.18 6-arg overload that wires the optional <see cref="IDeadLetterSink"/>.</summary>
    public OutboxDispatcherHostedService(
        IOutboxStorage storage,
        IOutboxSink sink,
        IOptions<OrionPatchOptions> options,
        IOutboxDispatcherClock clock,
        ILogger<OutboxDispatcherHostedService> logger,
        IDeadLetterSink? deadLetterSink)
    {
        ArgumentNullException.ThrowIfNull(storage);
        ArgumentNullException.ThrowIfNull(sink);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(clock);
        ArgumentNullException.ThrowIfNull(logger);
        this.storage = storage;
        this.sink = sink;
        this.options = options;
        this.clock = clock;
        this.logger = logger;
        this.deadLetterSink = deadLetterSink;
    }

    private static OutboxEnvelope? TryBuildEnvelope(OutboxRow row, OrionPatchOptions opts)
    {
        try
        {
            IReadOnlyDictionary<string, string>? headers = null;
            if (!string.IsNullOrEmpty(row.HeadersJson))
            {
                headers = System.Text.Json.JsonSerializer.Deserialize<System.Collections.Generic.Dictionary<string, string>>(
                    row.HeadersJson, opts.JsonOptions);
            }
            return new OutboxEnvelope(row.Id, row.MessageType, row.Payload, headers, row.CorrelationId, row.OccurredAtUtc, row.AttemptCount + 1);
        }
#pragma warning disable CA1031
        catch
#pragma warning restore CA1031
        {
            return null;
        }
    }

    [LoggerMessage(EventId = 4001, Level = LogLevel.Warning,
        Message = "Dead-letter sink failed for outbox row {RowId}; dead-letter state is already persisted.")]
    private partial void LogDeadLetterSinkFailed(System.Guid rowId, System.Exception exception);

    /// <summary>
    /// Runs the claim-dispatch-complete loop until the host requests shutdown.
    /// </summary>
    /// <param name="stoppingToken">Cancellation token signalled when the host is stopping.</param>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var opts = options.Value;
        var identity = opts.DispatcherIdentityFactory();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var pollSw = System.Diagnostics.Stopwatch.StartNew();
                var batch = await storage
                    .ClaimNextAsync(opts.BatchSize, identity, opts.LeaseDuration, stoppingToken)
                    .ConfigureAwait(false);
                pollSw.Stop();
                // v0.2.17: ALL cycles emit (including zero-row) because poll latency is
                // the signal itself - a slow ClaimNextAsync is operator-actionable
                // regardless of whether the result was empty.
                OrionPatchDiagnostics.PollDuration.Record(pollSw.Elapsed.TotalMilliseconds);

                if (batch.Count == 0)
                {
                    await clock.DelayAsync(opts.PollingInterval, stoppingToken).ConfigureAwait(false);
                    continue;
                }
                // v0.2.16 batch-size histogram: zero-row cycles do NOT emit so the
                // histogram tail reflects actual produced batches, not idle polling.
                OrionPatchDiagnostics.BatchSize.Record(batch.Count);

                foreach (var row in batch)
                {
                    if (stoppingToken.IsCancellationRequested)
                    {
                        break;
                    }
                    await DispatchOneAsync(row, opts, stoppingToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                LogDispatcherLoopFailure(logger, ex);
                try
                {
                    await clock.DelayAsync(opts.PollingInterval, stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
    }

    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Error,
        Message = "OrionPatch dispatcher loop failure; backing off")]
    private static partial void LogDispatcherLoopFailure(ILogger logger, Exception exception);

    private async Task DispatchOneAsync(OutboxRow row, OrionPatchOptions opts, CancellationToken cancellationToken)
    {
        var attempt = row.AttemptCount + 1;
        OrionPatchDiagnostics.Attempts.Add(1);
        var sw = Stopwatch.StartNew();
        using var activity = OrionPatchDiagnostics.ActivitySource.StartActivity("OrionPatch.Dispatch");
        activity?.SetTag("orionpatch.message.type", row.MessageType);
        activity?.SetTag("orionpatch.attempt", attempt);

        try
        {
            IReadOnlyDictionary<string, string>? headers = null;
            if (!string.IsNullOrEmpty(row.HeadersJson))
            {
                headers = JsonSerializer.Deserialize<Dictionary<string, string>>(row.HeadersJson, opts.JsonOptions);
            }

            var envelope = new OutboxEnvelope(
                row.Id,
                row.MessageType,
                row.Payload,
                headers,
                row.CorrelationId,
                row.OccurredAtUtc,
                attempt);

            await sink.SendAsync(envelope, cancellationToken).ConfigureAwait(false);
            await storage.CompleteAsync(row.Id, clock.UtcNow, cancellationToken).ConfigureAwait(false);
            OrionPatchDiagnostics.Dispatched.Add(1);
            OrionPatchDiagnostics.DispatchDuration.Record(sw.Elapsed.TotalMilliseconds);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            var truncated = Truncate(ex.ToString(), 4000);
            try
            {
                if (attempt >= opts.MaxAttempts)
                {
                    await storage.DeadLetterAsync(row.Id, truncated, cancellationToken).ConfigureAwait(false);
                    OrionPatchDiagnostics.DeadLettered.Add(1);
                    // v0.2.18 IDeadLetterSink: notify the consumer-registered sink AFTER
                    // the storage state is updated. A throwing sink does NOT roll the
                    // dead-letter back; sink exceptions are logged and swallowed because
                    // the sink is observability, not a load-bearing dispatch step.
                    var sinkRef = deadLetterSink;
                    if (sinkRef is not null and not NullDeadLetterSink)
                    {
                        try
                        {
                            await sinkRef.OnDeadLetteredAsync(
                                row.Id, TryBuildEnvelope(row, opts), truncated, attempt, cancellationToken)
                                .ConfigureAwait(false);
                        }
                        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                        {
                            throw;
                        }
#pragma warning disable CA1031
                        catch (Exception sinkEx)
#pragma warning restore CA1031
                        {
                            LogDeadLetterSinkFailed(row.Id, sinkEx);
                        }
                    }
                }
                else
                {
                    var nextAttempt = clock.UtcNow.Add(opts.BackoffStrategy(attempt));
                    await storage.FailAsync(row.Id, truncated, nextAttempt, cancellationToken).ConfigureAwait(false);
                    OrionPatchDiagnostics.Failed.Add(1);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception storageEx)
            {
                LogStorageFailureWhileRecordingDispatchFailure(logger, ex.ToString(), storageEx);
                // Re-throw so the outer ExecuteAsync catch triggers the dispatcher back-off.
                // Row stays in Claimed; lease expiry will surface it for re-attempt.
                throw;
            }
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
        }
    }

    [LoggerMessage(
        EventId = 2,
        Level = LogLevel.Error,
        Message = "OrionPatch storage failure while recording dispatch failure (original sink error: {SinkError})")]
    private static partial void LogStorageFailureWhileRecordingDispatchFailure(
        ILogger logger, string sinkError, Exception storageException);

    private static string Truncate(string value, int max) => value.Length <= max ? value : value[..max];
}
