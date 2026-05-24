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
/// The interceptor is stateless and safe to register as a singleton on
/// <see cref="DbContextOptionsBuilder.AddInterceptors(System.Collections.Generic.IEnumerable{Microsoft.EntityFrameworkCore.Diagnostics.IInterceptor})"/>.
/// It locates the bound outbox via <see cref="EfCoreOutbox.Find(DbContext)"/>, which is populated
/// when <see cref="EfCoreOutbox"/> is constructed against a DbContext — there is no application
/// service-provider hop, which keeps the interceptor usable in both DI and manually-wired scenarios.
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
        db.AddRange(outbox.Buffer);
        outbox.Buffer.Clear();
    }
}
