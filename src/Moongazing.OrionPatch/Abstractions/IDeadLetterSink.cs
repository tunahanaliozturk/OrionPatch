namespace Moongazing.OrionPatch.Abstractions;

using System.Threading;
using System.Threading.Tasks;
using Moongazing.OrionPatch.Models;

/// <summary>
/// v0.2.18 consumer-supplied observer invoked when an outbox row is dead-lettered.
/// Useful for routing the envelope to an external triage system (Slack notification,
/// PagerDuty alert, follow-up review queue) without baking the routing into the
/// dispatcher. Registered via DI; the dispatcher resolves it per-cycle.
/// </summary>
/// <remarks>
/// <para>
/// The sink runs AFTER <see cref="IOutboxStorage.DeadLetterAsync"/> succeeds so a
/// throwing sink does NOT affect the database state (the row is already dead-lettered).
/// Sink exceptions are caught and logged; they do not surface as dispatch failures.
/// </para>
/// <para>
/// No sink is registered by default. Consumers wire one via
/// <c>services.AddSingleton&lt;IDeadLetterSink, MySink&gt;()</c> or via the keyed-singleton
/// shortcut <c>services.AddOrionPatchDeadLetterSink&lt;MySink&gt;()</c> (v0.2.18+).
/// </para>
/// </remarks>
public interface IDeadLetterSink
{
    /// <summary>Notify the sink that a row has been dead-lettered.</summary>
    /// <param name="rowId">Outbox row id (Guid).</param>
    /// <param name="envelope">The envelope payload as-stored (may be null for legacy rows).</param>
    /// <param name="errorMessage">Truncated error text from the final dispatch attempt.</param>
    /// <param name="attemptCount">Number of attempts that preceded the dead-letter.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task OnDeadLetteredAsync(
        System.Guid rowId,
        OutboxEnvelope? envelope,
        string errorMessage,
        int attemptCount,
        CancellationToken cancellationToken);
}

/// <summary>Default no-op sink used when no consumer-registered sink is present.</summary>
public sealed class NullDeadLetterSink : IDeadLetterSink
{
    /// <inheritdoc />
    public Task OnDeadLetteredAsync(
        System.Guid rowId,
        OutboxEnvelope? envelope,
        string errorMessage,
        int attemptCount,
        CancellationToken cancellationToken) => Task.CompletedTask;
}
