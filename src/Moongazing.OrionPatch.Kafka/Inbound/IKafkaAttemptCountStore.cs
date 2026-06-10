namespace Moongazing.OrionPatch.Kafka.Inbound;

/// <summary>
/// Persistence hook for the inbound consumer's per-envelope attempt counter. v0.2.9 kept
/// the counter in memory only - a consumer restart reset the count and a poison-pill
/// envelope could escape DLQ routing by surviving across restarts. v0.2.10 lets the
/// consumer register a persistent store (EF Core table, Redis hash, Cosmos document) so
/// the attempt counter survives restarts and the DLQ promise becomes transactional.
/// </summary>
/// <remarks>
/// <para>
/// The store is consulted ONCE per handler attempt at the start of <c>HandleAsync</c>
/// (after the inbox accept) to seed the in-memory counter, and after each handler
/// failure / DLQ routing to persist the updated count. Implementations MUST be
/// idempotent for the persisted shape - a transient store error should not corrupt the
/// counter or accidentally trigger DLQ routing.
/// </para>
/// <para>
/// When no <see cref="IKafkaAttemptCountStore"/> is registered, the inbound service
/// falls back to the v0.2.9 in-memory <see cref="System.Collections.Concurrent.ConcurrentDictionary{TKey, TValue}"/>
/// behaviour. The default in-memory implementation is provided as
/// <see cref="InMemoryKafkaAttemptCountStore"/> for parity with v0.2.9.
/// </para>
/// </remarks>
public interface IKafkaAttemptCountStore
{
    /// <summary>Read the persisted attempt count for <paramref name="envelopeId"/>. Returns 0 when never persisted.</summary>
    ValueTask<int> GetAsync(Guid envelopeId, CancellationToken cancellationToken);

    /// <summary>Persist <paramref name="attemptCount"/> for <paramref name="envelopeId"/>.</summary>
    ValueTask SetAsync(Guid envelopeId, int attemptCount, CancellationToken cancellationToken);

    /// <summary>Forget the attempt record for <paramref name="envelopeId"/> (call after successful handle / DLQ).</summary>
    ValueTask ClearAsync(Guid envelopeId, CancellationToken cancellationToken);
}
