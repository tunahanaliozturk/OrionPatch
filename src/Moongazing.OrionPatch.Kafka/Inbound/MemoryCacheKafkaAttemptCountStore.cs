namespace Moongazing.OrionPatch.Kafka.Inbound;

using System.Collections.Concurrent;

/// <summary>
/// <see cref="IKafkaAttemptCountStore"/> decorator that fronts a slower persistent inner
/// store with an in-memory write-through cache. Useful when the inner store is a
/// transactional database (v0.2.11 EF Core) or a remote KV (planned Redis impl): the
/// hot-path read on every failure no longer round-trips to the database, but the count
/// stays restart-survivable because the inner store sees every write.
/// </summary>
/// <remarks>
/// <para>
/// Write semantics: <see cref="SetAsync"/> writes to BOTH the inner store and the cache
/// (cache writes are after the inner write succeeds so a transient inner failure does
/// not leave the cache ahead of the truth). <see cref="ClearAsync"/> evicts the cache
/// entry first, then forwards to the inner so a redelivery that arrives between the
/// cache evict and the inner clear sees the inner's truth rather than a stale cache hit.
/// </para>
/// <para>
/// Read semantics: <see cref="GetAsync"/> reads the cache first; on miss it forwards to
/// the inner and populates the cache with the result. A cache TTL is intentionally NOT
/// implemented because attempt counts are write-driven (every failure / success ticks
/// the value) - stale entries get refreshed naturally by the next failure or get cleared
/// by the success path.
/// </para>
/// </remarks>
public sealed class MemoryCacheKafkaAttemptCountStore : IKafkaAttemptCountStore
{
    private readonly IKafkaAttemptCountStore inner;
    private readonly ConcurrentDictionary<Guid, int> cache = new();

    /// <summary>Construct over a persistent inner store.</summary>
    public MemoryCacheKafkaAttemptCountStore(IKafkaAttemptCountStore inner)
    {
        ArgumentNullException.ThrowIfNull(inner);
        this.inner = inner;
    }

    /// <inheritdoc />
    public async ValueTask<int> GetAsync(Guid envelopeId, CancellationToken cancellationToken)
    {
        if (cache.TryGetValue(envelopeId, out var cached))
        {
            return cached;
        }
        var fromInner = await inner.GetAsync(envelopeId, cancellationToken).ConfigureAwait(false);
        // Only populate the cache when the inner store has a non-default value; caching
        // a 0 for an unseen envelope would prevent the next failure from observing a
        // 1-count miss if a concurrent writer set the inner side.
        if (fromInner > 0)
        {
            cache[envelopeId] = fromInner;
        }
        return fromInner;
    }

    /// <inheritdoc />
    public async ValueTask SetAsync(Guid envelopeId, int attemptCount, CancellationToken cancellationToken)
    {
        await inner.SetAsync(envelopeId, attemptCount, cancellationToken).ConfigureAwait(false);
        cache[envelopeId] = attemptCount;
    }

    /// <inheritdoc />
    public async ValueTask ClearAsync(Guid envelopeId, CancellationToken cancellationToken)
    {
        cache.TryRemove(envelopeId, out _);
        await inner.ClearAsync(envelopeId, cancellationToken).ConfigureAwait(false);
    }
}
