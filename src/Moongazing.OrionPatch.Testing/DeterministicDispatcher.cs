namespace Moongazing.OrionPatch.Testing;

using System.Text.Json;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Configuration;
using Moongazing.OrionPatch.Models;

/// <summary>
/// Synchronous-driver test dispatcher. Tests call <see cref="DispatchOnceAsync"/>
/// to process exactly one polling pass against the supplied storage and sink.
/// No background thread, no hosted-service lifecycle, no real wall-clock
/// dependence. Mirrors the per-envelope semantics of the production
/// <see cref="Moongazing.OrionPatch.Hosting.OutboxDispatcherHostedService"/>
/// (claim → send → complete; failure → retry with backoff or dead-letter once
/// <see cref="OrionPatchOptions.MaxAttempts"/> is reached) but is invoked
/// explicitly so tests stay deterministic.
/// </summary>
public sealed class DeterministicDispatcher
{
    private readonly IOutboxStorage storage;
    private readonly IOutboxSink sink;
    private readonly TestClock clock;
    private readonly OrionPatchOptions options;
    private readonly string dispatcherIdentity;

    /// <summary>Construct the dispatcher with explicit dependencies.</summary>
    /// <param name="storage">The storage to claim, complete, fail, and dead-letter rows against; must be non-null.</param>
    /// <param name="sink">The sink that receives each materialized envelope; must be non-null.</param>
    /// <param name="clock">Controllable clock used for <c>ProcessedAtUtc</c> and backoff arithmetic; must be non-null.</param>
    /// <param name="options">
    /// Optional <see cref="OrionPatchOptions"/> snapshot. When omitted, the dispatcher uses
    /// the defaults (<see cref="OrionPatchOptions.BatchSize"/> = 50,
    /// <see cref="OrionPatchOptions.MaxAttempts"/> = 5, exponential backoff).
    /// </param>
    /// <param name="dispatcherIdentity">
    /// Stable identity string stamped onto claimed rows. Defaults to <c>"test-dispatcher"</c>
    /// so tests don't have to think about it.
    /// </param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="storage"/>, <paramref name="sink"/>, or <paramref name="clock"/> is null.</exception>
    public DeterministicDispatcher(
        IOutboxStorage storage,
        IOutboxSink sink,
        TestClock clock,
        OrionPatchOptions? options = null,
        string? dispatcherIdentity = null)
    {
        ArgumentNullException.ThrowIfNull(storage);
        ArgumentNullException.ThrowIfNull(sink);
        ArgumentNullException.ThrowIfNull(clock);
        this.storage = storage;
        this.sink = sink;
        this.clock = clock;
        this.options = options ?? new OrionPatchOptions();
        this.dispatcherIdentity = dispatcherIdentity ?? "test-dispatcher";
    }

    /// <summary>
    /// Process up to <see cref="OrionPatchOptions.BatchSize"/> rows in a single pass.
    /// </summary>
    /// <param name="cancellationToken">Token observed across the claim → send → complete chain.</param>
    /// <returns>The number of rows that were successfully dispatched (sink returned without throwing and storage marked them processed).</returns>
    public async Task<int> DispatchOnceAsync(CancellationToken cancellationToken = default)
    {
        var batch = await storage
            .ClaimNextAsync(options.BatchSize, dispatcherIdentity, options.LeaseDuration, cancellationToken)
            .ConfigureAwait(false);

        var processed = 0;
        foreach (var row in batch)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var attempt = row.AttemptCount + 1;
            try
            {
                IReadOnlyDictionary<string, string>? headers = null;
                if (!string.IsNullOrEmpty(row.HeadersJson))
                {
                    headers = JsonSerializer.Deserialize<Dictionary<string, string>>(row.HeadersJson, options.JsonOptions);
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
                processed++;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                var message = ex.Message;
                if (attempt >= options.MaxAttempts)
                {
                    await storage.DeadLetterAsync(row.Id, message, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    var nextAttempt = clock.UtcNow.Add(options.BackoffStrategy(attempt));
                    await storage.FailAsync(row.Id, message, nextAttempt, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        return processed;
    }
}
