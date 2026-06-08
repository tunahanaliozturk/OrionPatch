namespace Moongazing.OrionPatch.Abstractions;

/// <summary>
/// Consumer-side dedup primitive. Wraps a message handler so repeated deliveries of the same
/// message id become observable no-ops without forcing every handler to write its own dedup
/// store.
/// </summary>
/// <remarks>
/// The interface is intentionally narrow: a single try-once method that returns whether the
/// message was a first delivery. Storage shape (in-memory dictionary, EF Core table, Redis,
/// etc.) is the implementation's concern; v0.2.2 ships the contract and the
/// in-memory <see cref="Channels.InMemoryInbox"/> implementation. EF Core storage ships in
/// v0.2.3 alongside the broker sink work.
/// </remarks>
public interface IInbox
{
    /// <summary>
    /// Atomically record a first delivery of <paramref name="messageId"/>. Returns
    /// <see langword="true"/> if this caller observed the first delivery; subsequent calls
    /// return <see langword="false"/>. The implementation MUST be safe under concurrent
    /// callers for the same id - at most one caller sees <see langword="true"/>.
    /// </summary>
    /// <param name="messageId">Stable id of the message under dedup. Typically the outbox row
    /// <c>OutboxEnvelope.Id</c> or a broker-supplied message id.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true"/> on first delivery, <see langword="false"/> on duplicate.</returns>
    ValueTask<bool> TryAcceptAsync(Guid messageId, CancellationToken cancellationToken);
}
