namespace Moongazing.OrionPatch.Tests.Hosting;

using System.Collections.Concurrent;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Configuration;
using Moongazing.OrionPatch.Hosting;
using Moongazing.OrionPatch.Models;
using Xunit;

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
}
