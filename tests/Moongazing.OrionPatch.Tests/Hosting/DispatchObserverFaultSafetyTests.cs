namespace Moongazing.OrionPatch.Tests.Hosting;

using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Configuration;
using Moongazing.OrionPatch.Hosting;
using Moongazing.OrionPatch.Models;
using Xunit;

/// <summary>
/// Behavioral guard for the v0.4.1 convergence onto
/// <c>Moongazing.Orion.Abstractions.Observers.SafeObserverInvoker.InvokeAsync</c>. These tests
/// drive the dispatcher end-to-end and assert that the fault-safe ASYNC observer contract is
/// preserved exactly after the bespoke try/catch was replaced by the shared invoker:
/// a throwing <see cref="IOutboxDispatchObserver.OnDispatchedAsync"/> must NOT abort the
/// dispatch (the row is already completed, the sink already fired), the observer-failure counter
/// must still increment tagged with the exception type, and the same warning must still be logged.
/// A non-throwing observer must still be invoked with the exact envelope / attempt / duration.
/// </summary>
[Collection("DispatcherQueueDepth")]
public sealed class DispatchObserverFaultSafetyTests
{
    [Fact]
    public async Task ThrowingObserver_DoesNotAbortDispatch_AndStillLogsAndCounts()
    {
        var rowId = Guid.NewGuid();
        var storage = new InMemoryStorage();
        await storage.AppendAsync(new[] { NewPendingRow(rowId, "T", "{}") }, default);

        var sink = new CapturingSink();
        var observer = new ThrowingObserver();
        var logger = new CapturingLogger<OutboxDispatcherHostedService>();
        var options = new OrionPatchOptions { PollingInterval = TimeSpan.FromMilliseconds(10) };

        var failureSamples = StartObserverFailureListener(out var listener);
        using (listener)
        {
            var svc = new OutboxDispatcherHostedService(
                storage, sink, Options.Create(options), new SystemClockProxy(),
                logger, deadLetterSink: null, dispatchObserver: observer);

            using var cts = new CancellationTokenSource();
            await svc.StartAsync(cts.Token);

            // Dispatch must complete despite the throwing observer: the sink fired and the row
            // is durably Processed. The observer runs AFTER CompleteAsync, so a fault cannot
            // roll the completion back.
            await WaitFor(() => sink.Dispatched.Count == 1, TimeSpan.FromSeconds(5));
            await WaitFor(() => storage.Rows[rowId].Status == OutboxStatus.Processed, TimeSpan.FromSeconds(5));

            // The fault is swallowed and surfaced as a logged warning (EventId 4002), not rethrown.
            await WaitFor(
                () =>
                {
                    lock (logger.Entries)
                    {
                        return logger.Entries.Any(e => e.EventId == 4002);
                    }
                },
                TimeSpan.FromSeconds(5));

            await cts.CancelAsync();
            await svc.StopAsync(default);
        }

        Assert.Single(sink.Dispatched);
        Assert.Equal(OutboxStatus.Processed, storage.Rows[rowId].Status);
        Assert.True(observer.WasInvoked);

        (LogLevel Level, EventId EventId, string Message, Exception? Exception) entry;
        lock (logger.Entries)
        {
            entry = logger.Entries.First(e => e.EventId == 4002);
        }
        Assert.Equal(LogLevel.Warning, entry.Level);
        Assert.IsType<InvalidOperationException>(entry.Exception);

        // The failure counter incremented, carrying the exception_type tag of the thrown type -
        // the same shape the bespoke catch emitted before the convergence.
        lock (failureSamples)
        {
            Assert.Contains(failureSamples, s => s.ExceptionType == nameof(InvalidOperationException));
        }
    }

    [Fact]
    public async Task NonThrowingObserver_ReceivesEnvelopeAttemptAndDuration()
    {
        var rowId = Guid.NewGuid();
        var storage = new InMemoryStorage();
        await storage.AppendAsync(new[] { NewPendingRow(rowId, "Demo.Event", "{}") }, default);

        var sink = new CapturingSink();
        var observer = new CapturingObserver();
        var options = new OrionPatchOptions { PollingInterval = TimeSpan.FromMilliseconds(10) };

        var svc = new OutboxDispatcherHostedService(
            storage, sink, Options.Create(options), new SystemClockProxy(),
            NullLogger<OutboxDispatcherHostedService>.Instance, deadLetterSink: null, dispatchObserver: observer);

        using var cts = new CancellationTokenSource();
        await svc.StartAsync(cts.Token);

        await WaitFor(() => observer.WasInvoked, TimeSpan.FromSeconds(5));

        await cts.CancelAsync();
        await svc.StopAsync(default);

        Assert.NotNull(observer.CapturedEnvelope);
        Assert.Equal(rowId, observer.CapturedEnvelope!.Id);
        Assert.Equal(1, observer.CapturedAttempt);
        Assert.True(observer.CapturedDuration >= 0d);
    }

    private static System.Collections.Generic.List<(string ExceptionType, long Value)> StartObserverFailureListener(out MeterListener listener)
    {
        var samples = new System.Collections.Generic.List<(string ExceptionType, long Value)>();
        var l = new MeterListener();
        l.InstrumentPublished = (instrument, ml) =>
        {
            if (instrument.Meter.Name == Moongazing.OrionPatch.Telemetry.OrionPatchDiagnostics.SourceName
                && instrument.Name == "orionpatch.outbox.dispatch_observer_failures")
            {
                ml.EnableMeasurementEvents(instrument);
            }
        };
        l.SetMeasurementEventCallback<long>((_, val, tags, _) =>
        {
            var type = string.Empty;
            foreach (var tag in tags)
            {
                if (tag.Key == "exception_type")
                {
                    type = tag.Value as string ?? string.Empty;
                }
            }
            lock (samples) { samples.Add((type, val)); }
        });
        l.Start();
        listener = l;
        return samples;
    }

    private static OutboxRow NewPendingRow(Guid id, string type, string payload) => new()
    {
        Id = id,
        MessageType = type,
        Payload = payload,
        OccurredAtUtc = DateTime.UtcNow,
        EnqueuedAtUtc = DateTime.UtcNow,
        Status = OutboxStatus.Pending,
    };

    private static async Task WaitFor(Func<bool> predicate, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (predicate())
            {
                return;
            }
            await Task.Delay(20);
        }
        throw new TimeoutException("Condition was not satisfied within timeout.");
    }

    private sealed class ThrowingObserver : IOutboxDispatchObserver
    {
        private int invoked;
        public bool WasInvoked => Volatile.Read(ref invoked) > 0;
        public Task OnDispatchedAsync(OutboxEnvelope envelope, int attemptCount, double dispatchDurationMs, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref invoked);
            return Task.FromException(new InvalidOperationException("observer failure for test"));
        }
    }

    private sealed class CapturingObserver : IOutboxDispatchObserver
    {
        private int invoked;
        public bool WasInvoked => Volatile.Read(ref invoked) > 0;
        public OutboxEnvelope? CapturedEnvelope { get; private set; }
        public int CapturedAttempt { get; private set; } = -1;
        public double CapturedDuration { get; private set; } = -1d;

        public Task OnDispatchedAsync(OutboxEnvelope envelope, int attemptCount, double dispatchDurationMs, CancellationToken cancellationToken)
        {
            CapturedEnvelope = envelope;
            CapturedAttempt = attemptCount;
            CapturedDuration = dispatchDurationMs;
            Interlocked.Increment(ref invoked);
            return Task.CompletedTask;
        }
    }

    private sealed class InMemoryStorage : IOutboxStorage
    {
        public ConcurrentDictionary<Guid, OutboxRow> Rows { get; } = new();

        public Task AppendAsync(IReadOnlyList<OutboxRow> rows, CancellationToken cancellationToken = default)
        {
            foreach (var r in rows)
            {
                Rows[r.Id] = r;
            }
            return Task.CompletedTask;
        }

        public Task<IReadOnlyList<OutboxRow>> ClaimNextAsync(int batchSize, string dispatcherIdentity, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
        {
            var now = DateTime.UtcNow;
            var claimed = Rows.Values
                .Where(r => r.Status == OutboxStatus.Pending && (r.NextAttemptAtUtc is null || r.NextAttemptAtUtc <= now))
                .Take(batchSize)
                .ToList();
            foreach (var r in claimed)
            {
                r.Status = OutboxStatus.Claimed;
                r.ClaimedAtUtc = now;
                r.ClaimedBy = dispatcherIdentity;
            }
            return Task.FromResult<IReadOnlyList<OutboxRow>>(claimed);
        }

        public Task CompleteAsync(Guid rowId, DateTime processedAtUtc, CancellationToken cancellationToken = default)
        {
            if (Rows.TryGetValue(rowId, out var r))
            {
                r.Status = OutboxStatus.Processed;
                r.ProcessedAtUtc = processedAtUtc;
            }
            return Task.CompletedTask;
        }

        public Task FailAsync(Guid rowId, string errorMessage, DateTime nextAttemptAtUtc, CancellationToken cancellationToken = default)
        {
            if (Rows.TryGetValue(rowId, out var r))
            {
                r.AttemptCount++;
                r.LastError = errorMessage;
                r.NextAttemptAtUtc = nextAttemptAtUtc;
                r.Status = OutboxStatus.Pending;
                r.ClaimedAtUtc = null;
                r.ClaimedBy = null;
            }
            return Task.CompletedTask;
        }

        public Task DeadLetterAsync(Guid rowId, string errorMessage, CancellationToken cancellationToken = default)
        {
            if (Rows.TryGetValue(rowId, out var r))
            {
                r.AttemptCount++;
                r.LastError = errorMessage;
                r.Status = OutboxStatus.DeadLettered;
                r.ClaimedAtUtc = null;
                r.ClaimedBy = null;
            }
            return Task.CompletedTask;
        }

        public Task<long> QueueDepthAsync(CancellationToken cancellationToken = default) =>
            Task.FromResult((long)Rows.Values.Count(r => r.Status == OutboxStatus.Pending));
    }

    private sealed class CapturingSink : IOutboxSink
    {
        public List<OutboxEnvelope> Dispatched { get; } = new();
        public Task SendAsync(OutboxEnvelope envelope, CancellationToken cancellationToken = default)
        {
            lock (Dispatched)
            {
                Dispatched.Add(envelope);
            }
            return Task.CompletedTask;
        }
    }

    private sealed class SystemClockProxy : IOutboxDispatcherClock
    {
        public DateTime UtcNow => DateTime.UtcNow;
        public Task DelayAsync(TimeSpan duration, CancellationToken cancellationToken = default) =>
            Task.Delay(duration, cancellationToken);
    }

    private sealed class CapturingLogger<T> : ILogger<T>
    {
        public List<(LogLevel Level, EventId EventId, string Message, Exception? Exception)> Entries { get; } = new();

        IDisposable? ILogger.BeginScope<TState>(TState state) => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            var message = formatter(state, exception);
            lock (Entries)
            {
                Entries.Add((logLevel, eventId, message, exception));
            }
        }
    }
}
