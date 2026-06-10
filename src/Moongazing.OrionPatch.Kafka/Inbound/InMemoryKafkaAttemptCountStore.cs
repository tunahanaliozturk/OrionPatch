namespace Moongazing.OrionPatch.Kafka.Inbound;

using System.Collections.Concurrent;

/// <summary>
/// Default <see cref="IKafkaAttemptCountStore"/> backed by a process-local
/// <see cref="ConcurrentDictionary{TKey, TValue}"/>. Preserves the v0.2.9 best-effort
/// poison-pill behaviour - consumer restarts reset the counter. Consumers needing
/// restart-survivable counts register an EF Core / Redis-backed store instead.
/// </summary>
public sealed class InMemoryKafkaAttemptCountStore : IKafkaAttemptCountStore
{
    private readonly ConcurrentDictionary<Guid, int> attempts = new();

    /// <inheritdoc />
    public ValueTask<int> GetAsync(Guid envelopeId, CancellationToken cancellationToken)
        => ValueTask.FromResult(attempts.TryGetValue(envelopeId, out var count) ? count : 0);

    /// <inheritdoc />
    public ValueTask SetAsync(Guid envelopeId, int attemptCount, CancellationToken cancellationToken)
    {
        attempts[envelopeId] = attemptCount;
        return default;
    }

    /// <inheritdoc />
    public ValueTask ClearAsync(Guid envelopeId, CancellationToken cancellationToken)
    {
        attempts.TryRemove(envelopeId, out _);
        return default;
    }
}
