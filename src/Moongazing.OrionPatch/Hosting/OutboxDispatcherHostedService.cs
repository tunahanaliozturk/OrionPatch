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
public sealed partial class OutboxDispatcherHostedService : BackgroundService
{
    private readonly IOutboxStorage storage;
    private readonly IOutboxSink sink;
    private readonly IOptions<OrionPatchOptions> options;
    private readonly IOutboxDispatcherClock clock;
    private readonly ILogger<OutboxDispatcherHostedService> logger;

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
    }

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
                var batch = await storage
                    .ClaimNextAsync(opts.BatchSize, identity, opts.LeaseDuration, stoppingToken)
                    .ConfigureAwait(false);

                if (batch.Count == 0)
                {
                    await clock.DelayAsync(opts.PollingInterval, stoppingToken).ConfigureAwait(false);
                    continue;
                }

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
            if (attempt >= opts.MaxAttempts)
            {
                await storage.DeadLetterAsync(row.Id, truncated, cancellationToken).ConfigureAwait(false);
                OrionPatchDiagnostics.DeadLettered.Add(1);
            }
            else
            {
                var nextAttempt = clock.UtcNow.Add(opts.BackoffStrategy(attempt));
                await storage.FailAsync(row.Id, truncated, nextAttempt, cancellationToken).ConfigureAwait(false);
                OrionPatchDiagnostics.Failed.Add(1);
            }
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
        }
    }

    private static string Truncate(string value, int max) => value.Length <= max ? value : value[..max];
}
