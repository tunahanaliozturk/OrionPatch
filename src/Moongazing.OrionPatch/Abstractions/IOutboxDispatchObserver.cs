namespace Moongazing.OrionPatch.Abstractions;

using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// v0.2.20 consumer-supplied observer invoked when an outbox row is successfully
/// dispatched. Mirror of v0.2.18 <see cref="IDeadLetterSink"/> on the success path.
/// Useful for emitting application-side audit trails (per-message acknowledgements,
/// downstream system fan-out for confirmed deliveries) without tangling that logic
/// into the dispatcher's load-bearing transactional path.
/// </summary>
/// <remarks>
/// <para>
/// The observer runs AFTER <see cref="IOutboxStorage.CompleteAsync"/> succeeds so a
/// throwing observer does NOT affect the database state (the row is already completed).
/// Observer exceptions are caught and logged; they do not surface as dispatch failures.
/// </para>
/// <para>
/// No observer is registered by default. Consumers wire one via
/// <c>services.AddSingleton&lt;IOutboxDispatchObserver, MyObserver&gt;()</c>.
/// </para>
/// </remarks>
public interface IOutboxDispatchObserver
{
    /// <summary>Notify the observer that a row was successfully dispatched.</summary>
    /// <param name="envelope">The envelope that was dispatched.</param>
    /// <param name="attemptCount">Number of attempts that preceded the success (1 = first try).</param>
    /// <param name="dispatchDurationMs">Sink call wall-clock in milliseconds.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task OnDispatchedAsync(
        Models.OutboxEnvelope envelope,
        int attemptCount,
        double dispatchDurationMs,
        CancellationToken cancellationToken);
}

/// <summary>Default no-op observer used when no consumer-registered observer is present.</summary>
public sealed class NullOutboxDispatchObserver : IOutboxDispatchObserver
{
    /// <inheritdoc />
    public Task OnDispatchedAsync(
        Models.OutboxEnvelope envelope,
        int attemptCount,
        double dispatchDurationMs,
        CancellationToken cancellationToken) => Task.CompletedTask;
}
