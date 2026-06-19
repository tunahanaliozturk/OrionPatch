namespace Moongazing.OrionPatch.Abstractions;

using Moongazing.OrionPatch.Models;

/// <summary>
/// v0.3.0 dead-letter <em>store</em> capability. Where <see cref="IDeadLetterSink"/> is a
/// fire-and-forget observer (Slack, PagerDuty, a triage queue) notified after a row is
/// abandoned, this is a durable store that the outbox storage routes the exhausted message
/// <em>into</em>, carrying its final failure context. Routing a message here is the alternative
/// to retrying forever or silently dropping it once <c>MaxAttempts</c> is reached.
/// </summary>
/// <remarks>
/// <para>
/// Implementations are expected to be co-located with <see cref="IOutboxStorage"/> so the
/// move (remove from the hot outbox, append to the dead-letter store) can be made atomic and
/// <em>exactly once</em>. The bundled in-memory storage implements both interfaces on one type;
/// a relational backend would typically write a sibling <c>dead_letter</c> table inside the
/// same transaction as the source-row delete.
/// </para>
/// <para>
/// Routing MUST be idempotent on <see cref="DeadLetteredMessage.Id"/>: a redelivered or
/// retried dead-letter for an already-routed row is a no-op, so a message lands in the store
/// exactly once even if the dispatcher's terminal path runs twice (lease expiry, crash-replay).
/// </para>
/// </remarks>
public interface IDeadLetterStore
{
    /// <summary>
    /// Route an exhausted outbox row into the dead-letter store exactly once. Implementations
    /// remove the source row from the active outbox and append a <see cref="DeadLetteredMessage"/>
    /// capturing the final failure context. A second call for the same <paramref name="rowId"/>
    /// after the message is already present is a no-op.
    /// </summary>
    /// <param name="rowId">Id of the row that exhausted its delivery budget.</param>
    /// <param name="context">Final failure context (error text, attempt count, dead-letter instant).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// <see langword="true"/> when this call performed the move (the message was newly routed);
    /// <see langword="false"/> when the row was already dead-lettered (idempotent no-op) or the
    /// source row no longer exists.
    /// </returns>
    Task<bool> DeadLetterAsync(Guid rowId, DeadLetterContext context, CancellationToken cancellationToken = default);

    /// <summary>Snapshot the messages currently held in the dead-letter store. Primarily for inspection, triage, and replay tooling.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<IReadOnlyList<DeadLetteredMessage>> GetDeadLetteredAsync(CancellationToken cancellationToken = default);
}
