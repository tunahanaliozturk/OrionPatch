namespace Moongazing.OrionPatch.EntityFrameworkCore;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Moongazing.OrionPatch.Models;

/// <summary>
/// EF Core <see cref="SaveChangesInterceptor"/> that flushes buffered messages from
/// the per-DbContext <see cref="EfCoreOutbox"/> into the DbContext's change tracker
/// during <c>SavingChanges</c>/<c>SavingChangesAsync</c>, so the resulting
/// <see cref="OutboxRow"/> rows commit atomically with the consumer's other entity changes.
/// </summary>
/// <remarks>
/// <para>
/// The interceptor is stateless and safe to register as a singleton on
/// <see cref="DbContextOptionsBuilder.AddInterceptors(System.Collections.Generic.IEnumerable{Microsoft.EntityFrameworkCore.Diagnostics.IInterceptor})"/>.
/// It locates the bound outbox via <see cref="EfCoreOutbox.Find(DbContext)"/>, which is populated
/// when <see cref="EfCoreOutbox"/> is constructed against a DbContext — there is no application
/// service-provider hop, which keeps the interceptor usable in both DI and manually-wired scenarios.
/// </para>
/// <para>
/// Flush is three-phase to preserve at-least-once on save failure:
/// <list type="number">
/// <item><c>SavingChanges*</c> moves rows from <see cref="EfCoreOutbox.Buffer"/> to
/// <see cref="EfCoreOutbox.PendingFlush"/> and attaches them to the change tracker.</item>
/// <item><c>SavedChanges*</c> commits by clearing <see cref="EfCoreOutbox.PendingFlush"/>.</item>
/// <item><c>SaveChangesFailed*</c> reverts by detaching the attached entries and
/// returning the rows to <see cref="EfCoreOutbox.Buffer"/> so a subsequent successful
/// <c>SaveChanges</c> still persists them.</item>
/// </list>
/// </para>
/// </remarks>
public sealed class OrionPatchSaveChangesInterceptor : SaveChangesInterceptor
{
    /// <inheritdoc/>
    public override InterceptionResult<int> SavingChanges(
        DbContextEventData eventData,
        InterceptionResult<int> result)
    {
        ArgumentNullException.ThrowIfNull(eventData);
        Flush(eventData.Context);
        return base.SavingChanges(eventData, result);
    }

    /// <inheritdoc/>
    public override ValueTask<InterceptionResult<int>> SavingChangesAsync(
        DbContextEventData eventData,
        InterceptionResult<int> result,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(eventData);
        Flush(eventData.Context);
        return base.SavingChangesAsync(eventData, result, cancellationToken);
    }

    /// <inheritdoc/>
    public override int SavedChanges(SaveChangesCompletedEventData eventData, int result)
    {
        ArgumentNullException.ThrowIfNull(eventData);
        Commit(eventData.Context);
        return base.SavedChanges(eventData, result);
    }

    /// <inheritdoc/>
    public override ValueTask<int> SavedChangesAsync(
        SaveChangesCompletedEventData eventData,
        int result,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(eventData);
        Commit(eventData.Context);
        return base.SavedChangesAsync(eventData, result, cancellationToken);
    }

    /// <inheritdoc/>
    public override void SaveChangesFailed(DbContextErrorEventData eventData)
    {
        ArgumentNullException.ThrowIfNull(eventData);
        Revert(eventData.Context);
        base.SaveChangesFailed(eventData);
    }

    /// <inheritdoc/>
    public override Task SaveChangesFailedAsync(
        DbContextErrorEventData eventData,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(eventData);
        Revert(eventData.Context);
        return base.SaveChangesFailedAsync(eventData, cancellationToken);
    }

    private static void Flush(DbContext? db)
    {
        if (db is null)
        {
            return;
        }
        var outbox = EfCoreOutbox.Find(db);
        if (outbox is null || outbox.Buffer.Count == 0)
        {
            return;
        }

        // Move rows from Buffer to PendingFlush and attach to the change tracker.
        // Buffer is drained here; PendingFlush is the durable record until Commit/Revert.
        foreach (var row in outbox.Buffer)
        {
            outbox.PendingFlush.Add(row);
            db.Add(row);
        }
        outbox.Buffer.Clear();
    }

    private static void Commit(DbContext? db)
    {
        if (db is null)
        {
            return;
        }
        var outbox = EfCoreOutbox.Find(db);
        if (outbox is null)
        {
            return;
        }
        // Save succeeded — drop pending entries; they are now persisted.
        outbox.PendingFlush.Clear();
    }

    private static void Revert(DbContext? db)
    {
        if (db is null)
        {
            return;
        }
        var outbox = EfCoreOutbox.Find(db);
        if (outbox is null || outbox.PendingFlush.Count == 0)
        {
            return;
        }

        // Save failed. Detach the entries we added (so a retry doesn't double-insert)
        // and move them back into Buffer so a subsequent successful SaveChanges still
        // persists them.
        foreach (var row in outbox.PendingFlush)
        {
            db.Entry(row).State = EntityState.Detached;
            outbox.Buffer.Add(row);
        }
        outbox.PendingFlush.Clear();
    }
}
