namespace Moongazing.OrionPatch.Testing;

using System.Collections.Concurrent;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Models;

/// <summary>
/// Thread-safe in-memory <see cref="IOutboxStorage"/> for use in tests.
/// Stores rows in a <see cref="ConcurrentDictionary{TKey, TValue}"/>; mutations
/// are serialized through a per-instance lock to keep claim/complete/fail/
/// dead-letter transitions atomic. Lease-expiry and FIFO claim semantics
/// match the EF Core
/// <see cref="Moongazing.OrionPatch.Models.OutboxRow"/>-backed
/// <c>CompareAndSwapClaimStrategy</c>. Not intended for production use.
/// </summary>
public sealed class InMemoryOutboxStorage : IOutboxStorage
{
    private readonly object syncRoot = new();
    private readonly ConcurrentDictionary<Guid, OutboxRow> rows = new();

    /// <summary>Snapshot of all stored rows for test inspection.</summary>
    public IReadOnlyCollection<OutboxRow> Rows
    {
        get
        {
            lock (syncRoot)
            {
                return rows.Values.ToList();
            }
        }
    }

    /// <inheritdoc/>
    public Task AppendAsync(IReadOnlyList<OutboxRow> rows, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rows);
        lock (syncRoot)
        {
            foreach (var row in rows)
            {
                this.rows[row.Id] = row;
            }
        }
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task<IReadOnlyList<OutboxRow>> ClaimNextAsync(
        int batchSize,
        string dispatcherIdentity,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(dispatcherIdentity);
        if (batchSize <= 0)
        {
            return Task.FromResult<IReadOnlyList<OutboxRow>>(Array.Empty<OutboxRow>());
        }

        var now = DateTime.UtcNow;
        var leaseExpiry = now - leaseDuration;

        lock (syncRoot)
        {
            var claimed = rows.Values
                .Where(r =>
                    (r.Status == OutboxStatus.Pending && (r.NextAttemptAtUtc is null || r.NextAttemptAtUtc <= now)) ||
                    (r.Status == OutboxStatus.Claimed && r.ClaimedAtUtc is not null && r.ClaimedAtUtc < leaseExpiry))
                .OrderBy(r => r.EnqueuedAtUtc)
                .Take(batchSize)
                .ToList();

            foreach (var row in claimed)
            {
                row.Status = OutboxStatus.Claimed;
                row.ClaimedAtUtc = now;
                row.ClaimedBy = dispatcherIdentity;
            }

            return Task.FromResult<IReadOnlyList<OutboxRow>>(claimed);
        }
    }

    /// <inheritdoc/>
    public Task CompleteAsync(Guid rowId, DateTime processedAtUtc, CancellationToken cancellationToken = default)
    {
        lock (syncRoot)
        {
            if (rows.TryGetValue(rowId, out var row))
            {
                row.Status = OutboxStatus.Processed;
                row.ProcessedAtUtc = processedAtUtc;
                row.ClaimedAtUtc = null;
                row.ClaimedBy = null;
            }
        }
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task FailAsync(Guid rowId, string errorMessage, DateTime nextAttemptAtUtc, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(errorMessage);
        lock (syncRoot)
        {
            if (rows.TryGetValue(rowId, out var row))
            {
                row.AttemptCount++;
                row.LastError = errorMessage;
                row.NextAttemptAtUtc = nextAttemptAtUtc;
                row.Status = OutboxStatus.Pending;
                row.ClaimedAtUtc = null;
                row.ClaimedBy = null;
            }
        }
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task DeadLetterAsync(Guid rowId, string errorMessage, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(errorMessage);
        lock (syncRoot)
        {
            if (rows.TryGetValue(rowId, out var row))
            {
                row.AttemptCount++;
                row.LastError = errorMessage;
                row.Status = OutboxStatus.DeadLettered;
                row.ClaimedAtUtc = null;
                row.ClaimedBy = null;
            }
        }
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task<long> QueueDepthAsync(CancellationToken cancellationToken = default)
    {
        lock (syncRoot)
        {
            return Task.FromResult((long)rows.Values.Count(r => r.Status == OutboxStatus.Pending));
        }
    }
}
