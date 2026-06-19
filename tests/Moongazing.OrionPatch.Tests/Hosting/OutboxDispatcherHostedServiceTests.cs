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

[Collection("DispatcherQueueDepth")]
public class OutboxDispatcherHostedServiceTests
{
    [Fact]
    public async Task ExecuteAsync_ShouldClaimDispatchAndComplete_WhenStorageHasPendingRow()
    {
        var storage = new InMemoryStorage();
        await storage.AppendAsync(new[] { NewPendingRow(Guid.NewGuid(), "T", "{}") }, default);

        var sink = new CapturingSink();
        var clock = new SystemClockProxy();
        var options = new OrionPatchOptions { PollingInterval = TimeSpan.FromMilliseconds(10) };

        var svc = new OutboxDispatcherHostedService(
            storage, sink, Options.Create(options), clock,
            NullLogger<OutboxDispatcherHostedService>.Instance);

        using var cts = new CancellationTokenSource();
        await svc.StartAsync(cts.Token);

        await WaitFor(() => sink.Dispatched.Count == 1, TimeSpan.FromSeconds(5));

        await cts.CancelAsync();
        await svc.StopAsync(default);

        Assert.Single(sink.Dispatched);
        Assert.Single(storage.Rows.Values.Where(r => r.Status == OutboxStatus.Processed));
    }

    [Fact]
    public async Task ExecuteAsync_ShouldRetryWithBackoff_WhenSinkThrowsAndAttemptsRemain()
    {
        var rowId = Guid.NewGuid();
        var storage = new InMemoryStorage();
        await storage.AppendAsync(new[] { NewPendingRow(rowId, "T", "{}") }, default);

        var sink = new ThrowingSink();
        var options = new OrionPatchOptions
        {
            PollingInterval = TimeSpan.FromMilliseconds(10),
            MaxAttempts = 3,
            BackoffStrategy = _ => TimeSpan.FromMilliseconds(50),
        };

        var svc = new OutboxDispatcherHostedService(
            storage, sink, Options.Create(options), new SystemClockProxy(),
            NullLogger<OutboxDispatcherHostedService>.Instance);

        using var cts = new CancellationTokenSource();
        await svc.StartAsync(cts.Token);

        await WaitFor(() => storage.Rows[rowId].AttemptCount >= 1, TimeSpan.FromSeconds(5));

        await cts.CancelAsync();
        await svc.StopAsync(default);

        var row = storage.Rows[rowId];
        Assert.True(row.AttemptCount >= 1);
        Assert.False(string.IsNullOrEmpty(row.LastError));
        Assert.NotNull(row.NextAttemptAtUtc);
        Assert.NotEqual(OutboxStatus.DeadLettered, row.Status);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldDeadLetter_WhenAttemptsExceedMax()
    {
        var rowId = Guid.NewGuid();
        var storage = new InMemoryStorage();
        var seedRow = NewPendingRow(rowId, "T", "{}");
        seedRow.AttemptCount = 2;
        await storage.AppendAsync(new[] { seedRow }, default);

        var sink = new ThrowingSink();
        var options = new OrionPatchOptions
        {
            PollingInterval = TimeSpan.FromMilliseconds(10),
            MaxAttempts = 3,
            BackoffStrategy = _ => TimeSpan.FromMilliseconds(5),
        };

        var svc = new OutboxDispatcherHostedService(
            storage, sink, Options.Create(options), new SystemClockProxy(),
            NullLogger<OutboxDispatcherHostedService>.Instance);

        using var cts = new CancellationTokenSource();
        await svc.StartAsync(cts.Token);

        await WaitFor(() => storage.Rows[rowId].Status == OutboxStatus.DeadLettered, TimeSpan.FromSeconds(5));

        await cts.CancelAsync();
        await svc.StopAsync(default);

        Assert.Equal(OutboxStatus.DeadLettered, storage.Rows[rowId].Status);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldPollWhenStorageIsEmpty_AndStopGracefullyOnCancellation()
    {
        var storage = new InMemoryStorage();
        var sink = new CapturingSink();
        var options = new OrionPatchOptions { PollingInterval = TimeSpan.FromMilliseconds(10) };

        var svc = new OutboxDispatcherHostedService(
            storage, sink, Options.Create(options), new SystemClockProxy(),
            NullLogger<OutboxDispatcherHostedService>.Instance);

        using var cts = new CancellationTokenSource();
        await svc.StartAsync(cts.Token);

        await Task.Delay(100);

        await cts.CancelAsync();
        await svc.StopAsync(default);

        Assert.Empty(sink.Dispatched);
        Assert.True(storage.ClaimNextCalls >= 1);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldLogOriginalSinkError_WhenStorageFailsDuringFailureRecovery()
    {
        var rowId = Guid.NewGuid();
        var storage = new FlappingStorage();
        await storage.AppendAsync(new[] { NewPendingRow(rowId, "T", "{}") }, default);

        var sink = new ThrowingSink();
        var capturedLogger = new CapturingLogger<OutboxDispatcherHostedService>();
        var options = new OrionPatchOptions
        {
            PollingInterval = TimeSpan.FromMilliseconds(10),
            MaxAttempts = 3,
            BackoffStrategy = _ => TimeSpan.FromMilliseconds(5),
        };

        var svc = new OutboxDispatcherHostedService(
            storage, sink, Options.Create(options), new SystemClockProxy(), capturedLogger);

        using var cts = new CancellationTokenSource();
        await svc.StartAsync(cts.Token);

        await WaitFor(
            () =>
            {
                lock (capturedLogger.Entries)
                {
                    return capturedLogger.Entries.Any(e =>
                        e.Message.Contains("storage failure while recording dispatch failure", StringComparison.OrdinalIgnoreCase));
                }
            },
            TimeSpan.FromSeconds(5));

        await cts.CancelAsync();
        await svc.StopAsync(default);

        (LogLevel Level, string Message, Exception? Exception) entry;
        lock (capturedLogger.Entries)
        {
            entry = capturedLogger.Entries.First(e =>
                e.Message.Contains("storage failure while recording dispatch failure", StringComparison.OrdinalIgnoreCase));
        }

        // The original sink error is carried as a structured-logging property and rendered into the message.
        Assert.Contains("sink failure for test", entry.Message, StringComparison.OrdinalIgnoreCase);
        // The storage exception is the one bound to the Exception slot.
        Assert.IsType<InvalidOperationException>(entry.Exception);
        Assert.Equal("storage flap", entry.Exception!.Message);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldRecordPickupLag_OnFirstAttempt()
    {
        // Seed the row's enqueue time 2s in the past so its first-pickup lag is ~2000ms - a
        // magnitude no other (fresh-row) test in the suite can produce, which keeps this
        // assertion immune to the shared static Meter being written by parallel test classes.
        var row = NewPendingRow(Guid.NewGuid(), "T", "{}");
        var staleRow = new OutboxRow
        {
            Id = row.Id,
            MessageType = row.MessageType,
            Payload = row.Payload,
            OccurredAtUtc = DateTime.UtcNow - TimeSpan.FromSeconds(2),
            EnqueuedAtUtc = DateTime.UtcNow - TimeSpan.FromSeconds(2),
            Status = OutboxStatus.Pending,
        };
        var storage = new InMemoryStorage();
        await storage.AppendAsync(new[] { staleRow }, default);

        var samples = StartPickupLagListener(out var listener);
        using (listener)
        {
            var sink = new CapturingSink();
            var options = new OrionPatchOptions { PollingInterval = TimeSpan.FromMilliseconds(10) };
            var svc = new OutboxDispatcherHostedService(
                storage, sink, Options.Create(options), new SystemClockProxy(),
                NullLogger<OutboxDispatcherHostedService>.Instance);

            using var cts = new CancellationTokenSource();
            await svc.StartAsync(cts.Token);
            await WaitFor(() => sink.Dispatched.Count == 1, TimeSpan.FromSeconds(5));
            await cts.CancelAsync();
            await svc.StopAsync(default);
        }

        lock (samples)
        {
            Assert.Contains(samples, s => s >= 1500.0);
        }
    }

    [Fact]
    public async Task ExecuteAsync_ShouldNotRecordPickupLag_OnRetryAttempt()
    {
        // A row whose AttemptCount is already 1 is dispatched as attempt #2, so pickup lag - which
        // is the time to the FIRST attempt - must NOT be re-recorded. The stale 2s enqueue time
        // means a (wrong) recording would surface as a ~2000ms sample; assert none appears.
        var retryRow = new OutboxRow
        {
            Id = Guid.NewGuid(),
            MessageType = "T",
            Payload = "{}",
            OccurredAtUtc = DateTime.UtcNow - TimeSpan.FromSeconds(2),
            EnqueuedAtUtc = DateTime.UtcNow - TimeSpan.FromSeconds(2),
            AttemptCount = 1,
            Status = OutboxStatus.Pending,
        };
        var storage = new InMemoryStorage();
        await storage.AppendAsync(new[] { retryRow }, default);

        var samples = StartPickupLagListener(out var listener);
        using (listener)
        {
            var sink = new CapturingSink();
            var options = new OrionPatchOptions { PollingInterval = TimeSpan.FromMilliseconds(10) };
            var svc = new OutboxDispatcherHostedService(
                storage, sink, Options.Create(options), new SystemClockProxy(),
                NullLogger<OutboxDispatcherHostedService>.Instance);

            using var cts = new CancellationTokenSource();
            await svc.StartAsync(cts.Token);
            await WaitFor(() => sink.Dispatched.Count == 1, TimeSpan.FromSeconds(5));
            await cts.CancelAsync();
            await svc.StopAsync(default);
        }

        lock (samples)
        {
            Assert.DoesNotContain(samples, s => s >= 1500.0);
        }
    }

    [Fact]
    public async Task ExecuteAsync_ShouldSkipDeadLetterSideEffects_WhenStoreReportsNoOp()
    {
        // codex P2: when IDeadLetterStore.DeadLetterAsync returns false for an idempotent replay
        // (two dispatchers handling the same exhausted row after lease expiry), the post-dead-letter
        // side effects - deadlettered telemetry increment + IDeadLetterSink notification - must NOT
        // run. Otherwise the dead_letter metric double-counts and triage alerts fire twice for a row
        // that was already routed. Seed a row whose FIRST dispatch is terminal (AttemptCount == MaxAttempts - 1)
        // and back it with a store that always reports a no-op (false).
        var rowId = Guid.NewGuid();
        var storage = new NoOpDeadLetterStorage();
        var seedRow = NewPendingRow(rowId, "T", "{}");
        seedRow.AttemptCount = 2;
        await storage.AppendAsync(new[] { seedRow }, default);

        var sink = new ThrowingSink();
        var deadLetterSink = new CountingDeadLetterSink();
        var options = new OrionPatchOptions
        {
            PollingInterval = TimeSpan.FromMilliseconds(10),
            MaxAttempts = 3,
            BackoffStrategy = _ => TimeSpan.FromMilliseconds(5),
        };

        var deadLetterSamples = StartDeadLetteredCounterListener(out var listener);
        using (listener)
        {
            var svc = new OutboxDispatcherHostedService(
                storage, sink, Options.Create(options), new SystemClockProxy(),
                NullLogger<OutboxDispatcherHostedService>.Instance, deadLetterSink);

            using var cts = new CancellationTokenSource();
            await svc.StartAsync(cts.Token);

            // The terminal path runs once the (single) attempt exhausts MaxAttempts and calls the store,
            // which reports a no-op. Wait until the store has actually been asked to dead-letter.
            await WaitFor(() => storage.DeadLetterCalls >= 1, TimeSpan.FromSeconds(5));
            // Give the loop a beat to (incorrectly) fire side effects if the gate were missing.
            await Task.Delay(100);

            await cts.CancelAsync();
            await svc.StopAsync(default);
        }

        // Store was asked to route, reported no-op -> NO deadlettered metric increment, NO sink call.
        Assert.True(storage.DeadLetterCalls >= 1);
        lock (deadLetterSamples)
        {
            Assert.Empty(deadLetterSamples);
        }
        Assert.Equal(0, deadLetterSink.Notifications);
    }

    private static System.Collections.Generic.List<long> StartDeadLetteredCounterListener(out MeterListener listener)
    {
        var samples = new System.Collections.Generic.List<long>();
        var l = new MeterListener();
        l.InstrumentPublished = (instrument, ml) =>
        {
            if (instrument.Meter.Name == Moongazing.OrionPatch.Telemetry.OrionPatchDiagnostics.SourceName
                && instrument.Name == "orionpatch.outbox.deadlettered")
            {
                ml.EnableMeasurementEvents(instrument);
            }
        };
        l.SetMeasurementEventCallback<long>((_, val, _, _) =>
        {
            lock (samples) { samples.Add(val); }
        });
        l.Start();
        listener = l;
        return samples;
    }

    private static System.Collections.Generic.List<double> StartPickupLagListener(out MeterListener listener)
    {
        var samples = new System.Collections.Generic.List<double>();
        var l = new MeterListener();
        l.InstrumentPublished = (instrument, ml) =>
        {
            if (instrument.Meter.Name == Moongazing.OrionPatch.Telemetry.OrionPatchDiagnostics.SourceName
                && instrument.Name == "orionpatch.outbox.dispatch.pickup_lag_ms")
            {
                ml.EnableMeasurementEvents(instrument);
            }
        };
        l.SetMeasurementEventCallback<double>((_, val, _, _) =>
        {
            lock (samples) { samples.Add(val); }
        });
        l.Start();
        listener = l;
        return samples;
    }

    [Fact]
    public void Constructor_ShouldThrow_WhenAnyDependencyIsNull()
    {
        var storage = new InMemoryStorage();
        var sink = new CapturingSink();
        var options = Options.Create(new OrionPatchOptions());
        var clock = new SystemClockProxy();
        var logger = NullLogger<OutboxDispatcherHostedService>.Instance;

        Assert.Throws<ArgumentNullException>(() => new OutboxDispatcherHostedService(null!, sink, options, clock, logger));
        Assert.Throws<ArgumentNullException>(() => new OutboxDispatcherHostedService(storage, null!, options, clock, logger));
        Assert.Throws<ArgumentNullException>(() => new OutboxDispatcherHostedService(storage, sink, null!, clock, logger));
        Assert.Throws<ArgumentNullException>(() => new OutboxDispatcherHostedService(storage, sink, options, null!, logger));
        Assert.Throws<ArgumentNullException>(() => new OutboxDispatcherHostedService(storage, sink, options, clock, null!));
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

    // Private in-file fakes. The formal Moongazing.OrionPatch.Testing package lands in Task 8;
    // for Task 4 we co-locate minimal versions to keep this PR self-contained.
    private sealed class InMemoryStorage : IOutboxStorage
    {
        public ConcurrentDictionary<Guid, OutboxRow> Rows { get; } = new();
        public int ClaimNextCalls;

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
            Interlocked.Increment(ref ClaimNextCalls);
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

    // Storage that implements IDeadLetterStore and ALWAYS reports a no-op (false) from
    // DeadLetterAsync, simulating an idempotent replay of an already-routed exhausted row.
    private sealed class NoOpDeadLetterStorage : IOutboxStorage, IDeadLetterStore
    {
        private readonly InMemoryStorage inner = new();
        public int DeadLetterCalls;

        public Task AppendAsync(IReadOnlyList<OutboxRow> rows, CancellationToken cancellationToken = default) =>
            inner.AppendAsync(rows, cancellationToken);

        public Task<IReadOnlyList<OutboxRow>> ClaimNextAsync(int batchSize, string dispatcherIdentity, TimeSpan leaseDuration, CancellationToken cancellationToken = default) =>
            inner.ClaimNextAsync(batchSize, dispatcherIdentity, leaseDuration, cancellationToken);

        public Task CompleteAsync(Guid rowId, DateTime processedAtUtc, CancellationToken cancellationToken = default) =>
            inner.CompleteAsync(rowId, processedAtUtc, cancellationToken);

        public Task FailAsync(Guid rowId, string errorMessage, DateTime nextAttemptAtUtc, CancellationToken cancellationToken = default) =>
            inner.FailAsync(rowId, errorMessage, nextAttemptAtUtc, cancellationToken);

        // Legacy in-place flip - not exercised by this fake's tests but required by the interface.
        public Task DeadLetterAsync(Guid rowId, string errorMessage, CancellationToken cancellationToken = default) =>
            inner.DeadLetterAsync(rowId, errorMessage, cancellationToken);

        public Task<long> QueueDepthAsync(CancellationToken cancellationToken = default) =>
            inner.QueueDepthAsync(cancellationToken);

        // Always a no-op: the row is treated as already dead-lettered by a peer dispatcher. Remove it
        // from the active outbox so the dispatcher loop does not re-claim and re-attempt it forever.
        public Task<bool> DeadLetterAsync(Guid rowId, DeadLetterContext context, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref DeadLetterCalls);
            inner.Rows.TryRemove(rowId, out _);
            return Task.FromResult(false);
        }

        public Task<IReadOnlyList<DeadLetteredMessage>> GetDeadLetteredAsync(CancellationToken cancellationToken = default) =>
            Task.FromResult<IReadOnlyList<DeadLetteredMessage>>(Array.Empty<DeadLetteredMessage>());
    }

    private sealed class CountingDeadLetterSink : IDeadLetterSink
    {
        private int notifications;
        public int Notifications => Volatile.Read(ref notifications);
        public Task OnDeadLetteredAsync(Guid rowId, OutboxEnvelope? envelope, string errorMessage, int attemptCount, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref notifications);
            return Task.CompletedTask;
        }
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

    private sealed class ThrowingSink : IOutboxSink
    {
        public Task SendAsync(OutboxEnvelope envelope, CancellationToken cancellationToken = default) =>
            Task.FromException(new InvalidOperationException("sink failure for test"));
    }

    private sealed class SystemClockProxy : IOutboxDispatcherClock
    {
        public DateTime UtcNow => DateTime.UtcNow;
        public Task DelayAsync(TimeSpan duration, CancellationToken cancellationToken = default) =>
            Task.Delay(duration, cancellationToken);
    }

    // Storage decorator that succeeds on Append/Claim/Complete (so the dispatcher can claim and
    // attempt) but throws on FailAsync/DeadLetterAsync to simulate a storage flap during the
    // sink-failure recovery path.
    private sealed class FlappingStorage : IOutboxStorage
    {
        private readonly InMemoryStorage inner = new();

        public Task AppendAsync(IReadOnlyList<OutboxRow> rows, CancellationToken cancellationToken = default) =>
            inner.AppendAsync(rows, cancellationToken);

        public Task<IReadOnlyList<OutboxRow>> ClaimNextAsync(int batchSize, string dispatcherIdentity, TimeSpan leaseDuration, CancellationToken cancellationToken = default) =>
            inner.ClaimNextAsync(batchSize, dispatcherIdentity, leaseDuration, cancellationToken);

        public Task CompleteAsync(Guid rowId, DateTime processedAtUtc, CancellationToken cancellationToken = default) =>
            inner.CompleteAsync(rowId, processedAtUtc, cancellationToken);

        public Task FailAsync(Guid rowId, string errorMessage, DateTime nextAttemptAtUtc, CancellationToken cancellationToken = default) =>
            Task.FromException(new InvalidOperationException("storage flap"));

        public Task DeadLetterAsync(Guid rowId, string errorMessage, CancellationToken cancellationToken = default) =>
            Task.FromException(new InvalidOperationException("storage flap"));

        public Task<long> QueueDepthAsync(CancellationToken cancellationToken = default) =>
            inner.QueueDepthAsync(cancellationToken);
    }

    private sealed class CapturingLogger<T> : ILogger<T>
    {
        public List<(LogLevel Level, string Message, Exception? Exception)> Entries { get; } = new();

        IDisposable? ILogger.BeginScope<TState>(TState state) => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            var message = formatter(state, exception);
            lock (Entries)
            {
                Entries.Add((logLevel, message, exception));
            }
        }
    }
}
