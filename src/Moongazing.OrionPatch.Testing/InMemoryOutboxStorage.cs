namespace Moongazing.OrionPatch.Testing;

using System.Collections.Concurrent;
using System.Text.Json;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Models;
using Moongazing.OrionPatch.Telemetry;

/// <summary>
/// Thread-safe in-memory <see cref="IOutboxStorage"/> for use in tests.
/// Stores rows in a <see cref="ConcurrentDictionary{TKey, TValue}"/>; mutations
/// are serialized through a per-instance lock to keep claim/complete/fail/
/// dead-letter transitions atomic. Lease-expiry and FIFO claim semantics
/// match the EF Core
/// <see cref="Moongazing.OrionPatch.Models.OutboxRow"/>-backed
/// <c>CompareAndSwapClaimStrategy</c>. Not intended for production use.
/// </summary>
/// <remarks>
/// v0.3.0: also implements <see cref="IDeadLetterStore"/> (route exhausted rows out of the hot
/// outbox into a dedicated dead-letter store exactly once) and <see cref="IOutboxArchivalStore"/>
/// (reap processed rows past the retention window, either archiving or purging them).
/// v0.3.3: also implements <see cref="IDeadLetterReplayStore"/> (re-enqueue a dead-lettered
/// message back into the active outbox as a fresh pending row, atomically, idempotently).
/// </remarks>
public sealed class InMemoryOutboxStorage : IOutboxStorage, IDeadLetterStore, IOutboxArchivalStore, IDeadLetterReplayStore
{
    private readonly object syncRoot = new();
    private readonly ConcurrentDictionary<Guid, OutboxRow> rows = new();
    private readonly ConcurrentDictionary<Guid, DeadLetteredMessage> deadLettered = new();
    private readonly List<OutboxRow> archived = new();
    private readonly bool purgeOnArchive;

    /// <summary>
    /// Create the in-memory storage in archive mode (reaped rows are retained in the in-memory archive).
    /// </summary>
    /// <remarks>
    /// This explicit zero-argument overload exists for BINARY compatibility. v0.2.x assemblies were
    /// compiled against a compiler-generated <c>.ctor()</c>; the v0.3.0 <c>bool</c>-parameter
    /// constructor below has an optional default but is a DIFFERENT metadata signature
    /// (<c>.ctor(System.Boolean)</c>). An optional default only preserves SOURCE compatibility, so
    /// callers compiled against v0.2.x that invoke <c>new InMemoryOutboxStorage()</c> would bind to a
    /// missing <c>.ctor()</c> and throw <see cref="MissingMethodException"/> at runtime under 0.3.0.
    /// This overload preserves that metadata signature and delegates to the new constructor.
    /// </remarks>
    public InMemoryOutboxStorage()
        : this(purgeOnArchive: false)
    {
    }

    /// <summary>
    /// Create the in-memory storage.
    /// </summary>
    /// <param name="purgeOnArchive">
    /// When <see langword="false"/> (default), reaped processed rows are moved into an in-memory
    /// archive observable via <see cref="GetArchivedAsync"/>. When <see langword="true"/>, reaped
    /// rows are discarded (purge mode) and <see cref="GetArchivedAsync"/> stays empty.
    /// </param>
    public InMemoryOutboxStorage(bool purgeOnArchive = false)
    {
        this.purgeOnArchive = purgeOnArchive;
    }

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

    /// <summary>Snapshot of the dead-letter store for test inspection.</summary>
    public IReadOnlyCollection<DeadLetteredMessage> DeadLetteredMessages
    {
        get
        {
            lock (syncRoot)
            {
                return deadLettered.Values.ToList();
            }
        }
    }

    /// <summary>Snapshot of the archive for test inspection.</summary>
    public IReadOnlyCollection<OutboxRow> ArchivedRows
    {
        get
        {
            lock (syncRoot)
            {
                return archived.ToList();
            }
        }
    }

    /// <inheritdoc/>
    public Task<bool> DeadLetterAsync(Guid rowId, DeadLetterContext context, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(context.FinalError);
        lock (syncRoot)
        {
            // Idempotency: if this row is already in the dead-letter store, the move has
            // already happened. Treat a redelivered terminal-path call as a no-op so the
            // message lands exactly once even under lease-expiry / crash-replay.
            if (deadLettered.ContainsKey(rowId))
            {
                return Task.FromResult(false);
            }

            if (!rows.TryRemove(rowId, out var row))
            {
                // Source row already gone (e.g. archived/purged) and not yet recorded as
                // dead-lettered: nothing to route.
                return Task.FromResult(false);
            }

            deadLettered[rowId] = new DeadLetteredMessage
            {
                Id = row.Id,
                MessageType = row.MessageType,
                Payload = row.Payload,
                HeadersJson = row.HeadersJson,
                CorrelationId = row.CorrelationId,
                OccurredAtUtc = row.OccurredAtUtc,
                EnqueuedAtUtc = row.EnqueuedAtUtc,
                AttemptCount = context.AttemptCount,
                FinalError = context.FinalError,
                DeadLetteredAtUtc = context.DeadLetteredAtUtc,
            };
            return Task.FromResult(true);
        }
    }

    /// <inheritdoc/>
    public Task<IReadOnlyList<DeadLetteredMessage>> GetDeadLetteredAsync(CancellationToken cancellationToken = default)
    {
        lock (syncRoot)
        {
            return Task.FromResult<IReadOnlyList<DeadLetteredMessage>>(deadLettered.Values.ToList());
        }
    }

    /// <inheritdoc/>
    public Task<int> ArchiveProcessedAsync(TimeSpan retention, DateTime nowUtc, CancellationToken cancellationToken = default)
    {
        if (retention < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(retention), retention, "Retention must be non-negative.");
        }

        var cutoff = nowUtc - retention;
        lock (syncRoot)
        {
            var reapable = rows.Values
                .Where(r => r.Status == OutboxStatus.Processed
                    && r.ProcessedAtUtc is not null
                    && r.ProcessedAtUtc <= cutoff)
                .ToList();

            foreach (var row in reapable)
            {
                if (rows.TryRemove(row.Id, out var removed) && !purgeOnArchive)
                {
                    archived.Add(removed);
                }
            }

            return Task.FromResult(reapable.Count);
        }
    }

    /// <inheritdoc/>
    public Task<IReadOnlyList<OutboxRow>> GetArchivedAsync(CancellationToken cancellationToken = default)
    {
        lock (syncRoot)
        {
            return Task.FromResult<IReadOnlyList<OutboxRow>>(archived.ToList());
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// The move (remove the dead-letter record, append a fresh pending row) is performed under the
    /// per-instance lock so it is atomic: the message is never simultaneously dead-lettered and
    /// live. Idempotency is anchored on the dead-letter id: a second call for a message already
    /// redriven (or never present) finds nothing to remove and is a no-op.
    /// </remarks>
    public Task<bool> RedriveAsync(Guid messageId, CancellationToken cancellationToken = default)
    {
        lock (syncRoot)
        {
            if (!deadLettered.TryRemove(messageId, out var message))
            {
                return Task.FromResult(false);
            }

            rows[message.Id] = ToPendingRow(message);
            OrionPatchDiagnostics.RecordRedriven(1);
            return Task.FromResult(true);
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Snapshots the matching dead-letter ids under the lock, then redrives them in batches of
    /// <paramref name="batchSize"/>, each batch taken under the lock so individual moves stay
    /// atomic. A message that is concurrently redriven or removed between the snapshot and its
    /// batch is counted as skipped, not re-enqueued twice.
    /// </remarks>
    public Task<RedriveResult> RedriveAsync(RedriveFilter filter, int batchSize, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);

        List<Guid> candidates;
        lock (syncRoot)
        {
            candidates = deadLettered.Values
                .Where(filter.Matches)
                .Select(m => m.Id)
                .ToList();
        }

        var result = RedriveResult.Empty;
        for (var i = 0; i < candidates.Count; i += batchSize)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var redriven = 0;
            var skipped = 0;
            lock (syncRoot)
            {
                var end = Math.Min(i + batchSize, candidates.Count);
                for (var j = i; j < end; j++)
                {
                    if (deadLettered.TryRemove(candidates[j], out var message))
                    {
                        rows[message.Id] = ToPendingRow(message);
                        redriven++;
                    }
                    else
                    {
                        skipped++;
                    }
                }
            }

            OrionPatchDiagnostics.RecordRedriven(redriven);
            result += new RedriveResult(redriven, skipped);
        }

        return Task.FromResult(result);
    }

    /// <summary>
    /// Build the fresh pending outbox row a redrive re-enqueues from a dead-lettered message:
    /// original payload / correlation id / occurrence time preserved, attempt count reset to zero,
    /// failure context cleared, and a <see cref="IDeadLetterReplayStore.RedrivenFromHeader"/> header
    /// stamped with the source id. <see cref="OutboxRow.EnqueuedAtUtc"/> is the redrive instant so
    /// FIFO claim ordering and pickup-lag telemetry treat it as a newly enqueued row.
    /// </summary>
    private static OutboxRow ToPendingRow(DeadLetteredMessage message)
    {
        var headersJson = StampRedrivenFrom(message.HeadersJson, message.Id);
        var now = DateTime.UtcNow;
        return new OutboxRow
        {
            Id = message.Id,
            MessageType = message.MessageType,
            Payload = message.Payload,
            HeadersJson = headersJson,
            CorrelationId = message.CorrelationId,
            OccurredAtUtc = message.OccurredAtUtc,
            EnqueuedAtUtc = now,
            Status = OutboxStatus.Pending,
            AttemptCount = 0,
            ClaimedAtUtc = null,
            ClaimedBy = null,
            LastError = null,
            ProcessedAtUtc = null,
            NextAttemptAtUtc = now,
        };
    }

    /// <summary>
    /// Merge the <see cref="IDeadLetterReplayStore.RedrivenFromHeader"/> header (value = the source
    /// dead-letter id in Guid "N" format) into the message's JSON-serialized header map, preserving
    /// any existing headers. A caller-supplied header of the same key is overwritten so the stamp is
    /// authoritative.
    /// </summary>
    private static string StampRedrivenFrom(string? headersJson, Guid sourceId)
    {
        var headers = string.IsNullOrEmpty(headersJson)
            ? new Dictionary<string, string>(StringComparer.Ordinal)
            : JsonSerializer.Deserialize<Dictionary<string, string>>(headersJson) ?? new Dictionary<string, string>(StringComparer.Ordinal);

        headers[IDeadLetterReplayStore.RedrivenFromHeader] = sourceId.ToString("N");
        return JsonSerializer.Serialize(headers);
    }
}
