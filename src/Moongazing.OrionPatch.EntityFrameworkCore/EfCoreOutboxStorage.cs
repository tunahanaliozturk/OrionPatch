namespace Moongazing.OrionPatch.EntityFrameworkCore;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.EntityFrameworkCore.Claims;
using Moongazing.OrionPatch.Models;

/// <summary>
/// EF Core-backed <see cref="IOutboxStorage"/>. Claim semantics are delegated to a
/// provider-aware <see cref="IClaimStrategy"/> picked via
/// <see cref="ProviderClaimStrategy.For"/>; Complete / Fail / DeadLetter mutations run
/// through <see cref="EntityFrameworkQueryableExtensions"/>'s
/// <c>ExecuteUpdateAsync</c> for single-round-trip atomicity. <c>QueueDepthAsync</c>
/// is a no-tracking <c>COUNT</c>.
/// </summary>
/// <remarks>
/// <para>
/// This type is registered per-scope alongside the consumer's <see cref="DbContext"/>;
/// the dispatcher hosted service resolves a fresh storage per claim cycle. EF Core's
/// thread-affinity restriction applies — do not invoke any of the lifecycle methods
/// concurrently on the same <see cref="DbContext"/>.
/// </para>
/// <para>
/// Complete/Fail/DeadLetter mutations use <c>ExecuteUpdateAsync</c>, which bypasses
/// EF Core's change tracker. The storage class itself never tracks <see cref="Models.OutboxRow"/>
/// (every read uses <c>AsNoTracking()</c>), so the library is self-consistent. If a consumer
/// mixes their own tracked <c>OutboxRow</c> queries against the same DbContext, those entities
/// will become stale after any storage write.
/// </para>
/// <para>
/// v0.3.2: this type also implements <see cref="IDeadLetterStore"/> and
/// <see cref="IOutboxArchivalStore"/>, bringing the durable dead-letter destination and the
/// retention reaper to the production EF Core backend. The dead-letter move (delete the source
/// outbox row, insert a <see cref="DeadLetterRow"/>) runs inside an explicit transaction so it
/// is atomic and exactly once; the archival reap moves processed rows past the retention cutoff
/// out of the active outbox in bounded batches so a large backlog never takes one long lock.
/// </para>
/// <para>
/// This type does not emit telemetry. <see cref="Hosting.OutboxDispatcherHostedService"/>
/// instruments <see cref="Abstractions.IOutboxStorage"/> calls externally; storage stays
/// transparent to keep the per-operation cost predictable and to avoid double-counting.
/// </para>
/// </remarks>
public sealed class EfCoreOutboxStorage : IOutboxStorage, IDeadLetterStore, IOutboxArchivalStore
{
    /// <summary>
    /// Default number of rows moved out of the active outbox per archival batch. Bounds the
    /// per-statement work so a large processed backlog is reaped incrementally rather than under
    /// one long-held lock. The reap loops until a batch comes back short, so the cap only affects
    /// batch granularity, never the total reaped.
    /// </summary>
    private const int DefaultArchiveBatchSize = 500;

    /// <summary>
    /// Upper bound on the rows returned by <see cref="GetDeadLetteredAsync(CancellationToken)"/>.
    /// The SPI signature returns a single snapshot rather than a page; on a relational store the
    /// dead-letter table can grow without bound, so the unbounded read is capped to the most recent
    /// rows to keep a triage call from materializing an arbitrarily large result set. Use
    /// <see cref="GetDeadLetteredAsync(int, int, CancellationToken)"/> to page beyond this window.
    /// </summary>
    private const int DeadLetterSnapshotCap = 1000;

    private readonly DbContext db;
    private readonly IClaimStrategy claimStrategy;
    private readonly bool purgeOnArchive;

    /// <summary>
    /// Create the storage bound to a specific <see cref="DbContext"/> in archive mode. The claim
    /// strategy is auto-selected by <see cref="ProviderClaimStrategy.For"/> from the DbContext's
    /// database provider.
    /// </summary>
    /// <param name="db">DbContext used for all storage operations; must be non-null.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="db"/> is null.</exception>
    public EfCoreOutboxStorage(DbContext db)
        : this(db, ProviderClaimStrategy.For(db ?? throw new ArgumentNullException(nameof(db))), purgeOnArchive: false)
    {
    }

    /// <summary>
    /// Create the storage bound to a specific <see cref="DbContext"/>, choosing the archival mode.
    /// The claim strategy is auto-selected by <see cref="ProviderClaimStrategy.For"/>.
    /// </summary>
    /// <param name="db">DbContext used for all storage operations; must be non-null.</param>
    /// <param name="purgeOnArchive">
    /// When <see langword="false"/>, <see cref="ArchiveProcessedAsync"/> copies reaped rows into the
    /// <c>OrionPatch_OutboxArchive</c> table before deleting them from the active outbox. When
    /// <see langword="true"/>, reaped rows are deleted outright (purge mode) and the archive stays empty.
    /// </param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="db"/> is null.</exception>
    public EfCoreOutboxStorage(DbContext db, bool purgeOnArchive)
        : this(db, ProviderClaimStrategy.For(db ?? throw new ArgumentNullException(nameof(db))), purgeOnArchive)
    {
    }

    /// <summary>
    /// Test-seam constructor that takes an explicit claim strategy (archive mode). Used by the
    /// unit-test project to inject the same strategy <see cref="ProviderClaimStrategy.For"/> would
    /// have resolved for the bound DbContext, without going through the static resolver.
    /// </summary>
    /// <param name="db">DbContext used for all storage operations; must be non-null.</param>
    /// <param name="claimStrategy">Provider-aware claim strategy; must be non-null.</param>
    /// <exception cref="ArgumentNullException">Thrown when any argument is null.</exception>
    internal EfCoreOutboxStorage(DbContext db, IClaimStrategy claimStrategy)
        : this(db, claimStrategy, purgeOnArchive: false)
    {
    }

    /// <summary>
    /// Test-seam constructor that takes an explicit claim strategy and archival mode.
    /// </summary>
    /// <param name="db">DbContext used for all storage operations; must be non-null.</param>
    /// <param name="claimStrategy">Provider-aware claim strategy; must be non-null.</param>
    /// <param name="purgeOnArchive">See <see cref="EfCoreOutboxStorage(DbContext, bool)"/>.</param>
    /// <exception cref="ArgumentNullException">Thrown when any argument is null.</exception>
    internal EfCoreOutboxStorage(DbContext db, IClaimStrategy claimStrategy, bool purgeOnArchive)
    {
        ArgumentNullException.ThrowIfNull(db);
        ArgumentNullException.ThrowIfNull(claimStrategy);
        this.db = db;
        this.claimStrategy = claimStrategy;
        this.purgeOnArchive = purgeOnArchive;
    }

    /// <inheritdoc/>
    public async Task AppendAsync(IReadOnlyList<OutboxRow> rows, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rows);
        if (rows.Count == 0)
        {
            return;
        }
        db.AddRange(rows);
        await db.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public Task<IReadOnlyList<OutboxRow>> ClaimNextAsync(int batchSize, string dispatcherIdentity, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(dispatcherIdentity);
        return claimStrategy.ClaimNextAsync(db, batchSize, dispatcherIdentity, leaseDuration, DateTime.UtcNow, cancellationToken);
    }

    /// <inheritdoc/>
    public async Task CompleteAsync(Guid rowId, DateTime processedAtUtc, CancellationToken cancellationToken = default)
    {
        await db.Set<OutboxRow>()
            .Where(r => r.Id == rowId)
            .ExecuteUpdateAsync(s => s
                .SetProperty(r => r.Status, OutboxStatus.Processed)
                .SetProperty(r => r.ProcessedAtUtc, (DateTime?)processedAtUtc)
                .SetProperty(r => r.ClaimedAtUtc, (DateTime?)null)
                .SetProperty(r => r.ClaimedBy, (string?)null),
                cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task FailAsync(Guid rowId, string errorMessage, DateTime nextAttemptAtUtc, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(errorMessage);
        await db.Set<OutboxRow>()
            .Where(r => r.Id == rowId)
            .ExecuteUpdateAsync(s => s
                .SetProperty(r => r.Status, OutboxStatus.Pending)
                .SetProperty(r => r.AttemptCount, r => r.AttemptCount + 1)
                .SetProperty(r => r.LastError, (string?)errorMessage)
                .SetProperty(r => r.NextAttemptAtUtc, (DateTime?)nextAttemptAtUtc)
                .SetProperty(r => r.ClaimedAtUtc, (DateTime?)null)
                .SetProperty(r => r.ClaimedBy, (string?)null),
                cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task DeadLetterAsync(Guid rowId, string errorMessage, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(errorMessage);
        await db.Set<OutboxRow>()
            .Where(r => r.Id == rowId)
            .ExecuteUpdateAsync(s => s
                .SetProperty(r => r.Status, OutboxStatus.DeadLettered)
                .SetProperty(r => r.AttemptCount, r => r.AttemptCount + 1)
                .SetProperty(r => r.LastError, (string?)errorMessage)
                .SetProperty(r => r.ClaimedAtUtc, (DateTime?)null)
                .SetProperty(r => r.ClaimedBy, (string?)null),
                cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task<long> QueueDepthAsync(CancellationToken cancellationToken = default)
    {
        return await db.Set<OutboxRow>()
            .AsNoTracking()
            .Where(r => r.Status == OutboxStatus.Pending)
            .LongCountAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Runs the source-row delete and the <see cref="DeadLetterRow"/> insert inside a single
    /// explicit transaction so the move is atomic: a crash between the two never leaves the row in
    /// both tables or neither. Idempotency is anchored on the dead-letter primary key (the source
    /// row id): a replayed terminal path either finds the dead-letter row already present (the
    /// pre-check) or loses the race and trips the primary-key constraint on insert (the catch),
    /// and both are reported as the no-op (<see langword="false"/>). When this storage is already
    /// enlisted in an ambient transaction (the caller opened one), it joins that transaction and
    /// leaves commit/rollback to the owner rather than nesting.
    /// </remarks>
    public async Task<bool> DeadLetterAsync(Guid rowId, DeadLetterContext context, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(context.FinalError);

        // Join an ambient transaction if the caller already opened one; otherwise own a fresh one.
        var ownsTransaction = db.Database.CurrentTransaction is null;
        IDbContextTransaction? transaction = ownsTransaction
            ? await db.Database.BeginTransactionAsync(cancellationToken).ConfigureAwait(false)
            : null;
        try
        {
            // Idempotency pre-check: a row already routed is an exactly-once no-op.
            var alreadyRouted = await db.Set<DeadLetterRow>()
                .AsNoTracking()
                .AnyAsync(d => d.Id == rowId, cancellationToken).ConfigureAwait(false);
            if (alreadyRouted)
            {
                return false;
            }

            // Read the source row's enqueue-time columns to copy into the snapshot. AsNoTracking so
            // the subsequent ExecuteDelete is the only write touching the row.
            var source = await db.Set<OutboxRow>()
                .AsNoTracking()
                .Where(r => r.Id == rowId)
                .Select(r => new
                {
                    r.MessageType,
                    r.Payload,
                    r.HeadersJson,
                    r.CorrelationId,
                    r.OccurredAtUtc,
                    r.EnqueuedAtUtc,
                })
                .FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            if (source is null)
            {
                // Source row already gone and not yet recorded as dead-lettered: nothing to route.
                return false;
            }

            db.Add(new DeadLetterRow
            {
                Id = rowId,
                MessageType = source.MessageType,
                Payload = source.Payload,
                HeadersJson = source.HeadersJson,
                CorrelationId = source.CorrelationId,
                OccurredAtUtc = source.OccurredAtUtc,
                EnqueuedAtUtc = source.EnqueuedAtUtc,
                AttemptCount = context.AttemptCount,
                FinalError = context.FinalError,
                DeadLetteredAtUtc = context.DeadLetteredAtUtc,
            });

            try
            {
                await db.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (DbUpdateException)
            {
                // Lost the race to a concurrent terminal path that inserted the same id first.
                // Treat the primary-key violation as the idempotent no-op. Detach the rejected
                // entity so a later SaveChanges on this context does not retry the insert.
                db.ChangeTracker.Clear();
                if (ownsTransaction && transaction is not null)
                {
                    await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                }
                return false;
            }

            await db.Set<OutboxRow>()
                .Where(r => r.Id == rowId)
                .ExecuteDeleteAsync(cancellationToken).ConfigureAwait(false);

            if (ownsTransaction && transaction is not null)
            {
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            }

            return true;
        }
        finally
        {
            if (transaction is not null)
            {
                await transaction.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Returns the most recent <see cref="DeadLetterSnapshotCap"/> rows, newest first. The SPI
    /// hands back a single snapshot rather than a page, so on a relational store - where the
    /// dead-letter table can grow without bound - the read is capped to avoid materializing an
    /// arbitrarily large result set during triage. Use
    /// <see cref="GetDeadLetteredAsync(int, int, CancellationToken)"/> to page beyond the cap.
    /// </remarks>
    public async Task<IReadOnlyList<DeadLetteredMessage>> GetDeadLetteredAsync(CancellationToken cancellationToken = default)
    {
        return await GetDeadLetteredAsync(skip: 0, take: DeadLetterSnapshotCap, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Read a bounded page of dead-lettered messages, ordered newest first by
    /// <see cref="DeadLetterRow.DeadLetteredAtUtc"/>. The relational read the SPI's unbounded
    /// snapshot cannot express: paging tooling that triages a large backlog calls this directly.
    /// </summary>
    /// <param name="skip">Number of rows to skip from the newest; must be non-negative.</param>
    /// <param name="take">Maximum rows to return; must be positive.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Up to <paramref name="take"/> dead-lettered messages, newest first.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="skip"/> is negative or <paramref name="take"/> is not positive.</exception>
    public async Task<IReadOnlyList<DeadLetteredMessage>> GetDeadLetteredAsync(int skip, int take, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(skip);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(take);

        var rows = await db.Set<DeadLetterRow>()
            .AsNoTracking()
            .OrderByDescending(d => d.DeadLetteredAtUtc)
            .ThenBy(d => d.Id)
            .Skip(skip)
            .Take(take)
            .ToListAsync(cancellationToken).ConfigureAwait(false);

        return rows.Select(static d => d.ToMessage()).ToList();
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Reaps in bounded batches of <see cref="DefaultArchiveBatchSize"/>: each pass shortlists the
    /// oldest processed ids past the cutoff, then in archive mode copies them into
    /// <c>OrionPatch_OutboxArchive</c> and deletes them from the active outbox inside one
    /// transaction, or in purge mode deletes them outright. The loop continues until a batch comes
    /// back short, so a large backlog drains over several small statements rather than one long
    /// lock. Deletes target an explicit id set (not <c>DELETE ... LIMIT</c>) so the reap is portable
    /// across providers. The cutoff is inclusive (<c>ProcessedAtUtc &lt;= nowUtc - retention</c>),
    /// matching the in-memory store.
    /// </remarks>
    public async Task<int> ArchiveProcessedAsync(TimeSpan retention, DateTime nowUtc, CancellationToken cancellationToken = default)
    {
        if (retention < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(retention), retention, "Retention must be non-negative.");
        }

        var cutoff = nowUtc - retention;
        var total = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var batchIds = await db.Set<OutboxRow>()
                .AsNoTracking()
                .Where(r => r.Status == OutboxStatus.Processed
                    && r.ProcessedAtUtc != null
                    && r.ProcessedAtUtc <= cutoff)
                .OrderBy(r => r.ProcessedAtUtc)
                .Take(DefaultArchiveBatchSize)
                .Select(r => r.Id)
                .ToListAsync(cancellationToken).ConfigureAwait(false);

            if (batchIds.Count == 0)
            {
                break;
            }

            if (purgeOnArchive)
            {
                await db.Set<OutboxRow>()
                    .Where(r => batchIds.Contains(r.Id))
                    .ExecuteDeleteAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await ArchiveBatchAsync(batchIds, cancellationToken).ConfigureAwait(false);
            }

            total += batchIds.Count;

            // A short batch means the cutoff set is exhausted; avoid an extra empty round-trip.
            if (batchIds.Count < DefaultArchiveBatchSize)
            {
                break;
            }
        }

        return total;
    }

    /// <summary>
    /// Copy one batch of processed rows into the archive table and delete them from the active
    /// outbox inside a single transaction so the move is atomic. Joins an ambient transaction when
    /// the caller already owns one rather than nesting.
    /// </summary>
    private async Task ArchiveBatchAsync(IReadOnlyList<Guid> batchIds, CancellationToken cancellationToken)
    {
        var rows = await db.Set<OutboxRow>()
            .AsNoTracking()
            .Where(r => batchIds.Contains(r.Id))
            .ToListAsync(cancellationToken).ConfigureAwait(false);

        if (rows.Count == 0)
        {
            return;
        }

        var ownsTransaction = db.Database.CurrentTransaction is null;
        IDbContextTransaction? transaction = ownsTransaction
            ? await db.Database.BeginTransactionAsync(cancellationToken).ConfigureAwait(false)
            : null;
        try
        {
            db.AddRange(rows.Select(static r => new OutboxArchiveRow
            {
                Id = r.Id,
                MessageType = r.MessageType,
                Payload = r.Payload,
                HeadersJson = r.HeadersJson,
                CorrelationId = r.CorrelationId,
                OccurredAtUtc = r.OccurredAtUtc,
                EnqueuedAtUtc = r.EnqueuedAtUtc,
                AttemptCount = r.AttemptCount,
                LastError = r.LastError,
                ProcessedAtUtc = r.ProcessedAtUtc,
            }));
            await db.SaveChangesAsync(cancellationToken).ConfigureAwait(false);

            await db.Set<OutboxRow>()
                .Where(r => batchIds.Contains(r.Id))
                .ExecuteDeleteAsync(cancellationToken).ConfigureAwait(false);

            if (ownsTransaction && transaction is not null)
            {
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            if (transaction is not null)
            {
                await transaction.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// In purge mode the archive table is never written, so this returns an empty list. In archive
    /// mode it reads back every <see cref="OutboxArchiveRow"/>, newest first by
    /// <see cref="OutboxArchiveRow.ProcessedAtUtc"/>. Like the dead-letter snapshot this can grow
    /// large on a long-lived store; callers that need to bound the read should query the archive
    /// table directly.
    /// </remarks>
    public async Task<IReadOnlyList<OutboxRow>> GetArchivedAsync(CancellationToken cancellationToken = default)
    {
        if (purgeOnArchive)
        {
            return Array.Empty<OutboxRow>();
        }

        var rows = await db.Set<OutboxArchiveRow>()
            .AsNoTracking()
            .OrderByDescending(a => a.ProcessedAtUtc)
            .ThenBy(a => a.Id)
            .ToListAsync(cancellationToken).ConfigureAwait(false);

        return rows.Select(static a => a.ToRow()).ToList();
    }
}
