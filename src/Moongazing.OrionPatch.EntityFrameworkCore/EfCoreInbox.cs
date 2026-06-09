namespace Moongazing.OrionPatch.EntityFrameworkCore;

using Microsoft.EntityFrameworkCore;
using Moongazing.OrionPatch.Abstractions;

/// <summary>
/// EF Core-backed <see cref="IInbox"/>. Persists accepted message ids in the
/// <see cref="InboxRow"/> table; consumer-side dedup survives process restart.
/// </summary>
/// <remarks>
/// Concurrency: <see cref="TryAcceptAsync"/> attempts an insert and treats a unique-constraint
/// violation as "duplicate, return false". This relies on the storage backend's primary-key
/// enforcement. SQL Server, Postgres, MySQL, and SQLite all raise the same EF Core
/// <see cref="DbUpdateException"/> shape on a duplicate insert; the implementation inspects
/// the inner exception once and treats it as a duplicate regardless of the provider error code.
/// </remarks>
public sealed class EfCoreInbox : IInbox
{
    private readonly DbContext db;
    private readonly string? consumer;
    private readonly TimeProvider clock;

    /// <summary>
    /// Bind the inbox to a specific <see cref="DbContext"/>. Optional <paramref name="consumer"/>
    /// scopes the dedup state so two consumers can share the same table without colliding.
    /// </summary>
    public EfCoreInbox(DbContext db, string? consumer = null, TimeProvider? clock = null)
    {
        ArgumentNullException.ThrowIfNull(db);
        this.db = db;
        this.consumer = consumer;
        this.clock = clock ?? TimeProvider.System;
    }

    /// <inheritdoc />
    public async ValueTask<bool> TryAcceptAsync(Guid messageId, CancellationToken cancellationToken)
    {
        var consumerKey = consumer ?? string.Empty;

        // Fast-path: a previous call within the same DbContext lifetime tracked the row.
        // Looking it up in the change tracker is cheap and avoids a duplicate-insert exception
        // (the change tracker enforces the composite-key uniqueness BEFORE SaveChanges runs).
        var local = db.Set<InboxRow>().Local.FirstOrDefault(
            r => r.MessageId == messageId && r.Consumer == consumerKey);
        if (local is not null)
        {
            return false;
        }

        var row = new InboxRow
        {
            MessageId = messageId,
            Consumer = consumerKey,
            AcceptedAtUtc = clock.GetUtcNow().UtcDateTime,
        };

        try
        {
            db.Set<InboxRow>().Add(row);
            await db.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (DbUpdateException)
        {
            // Detach the failed insert so the change tracker does not retry it on subsequent
            // SaveChanges calls.
            db.Entry(row).State = EntityState.Detached;

            // Determine whether this was actually a duplicate-key conflict or a different
            // persistence failure (missing table, schema mismatch, connection error). We
            // query for the row's existence: if it now exists, the failure was a duplicate
            // and we report "already seen". Otherwise it was a genuine storage failure and
            // the caller MUST see the exception so the message is not silently dropped.
            var exists = await db.Set<InboxRow>()
                .AsNoTracking()
                .AnyAsync(
                    r => r.MessageId == messageId && r.Consumer == consumerKey,
                    cancellationToken)
                .ConfigureAwait(false);

            if (exists)
            {
                return false;
            }

            throw;
        }
    }
}
