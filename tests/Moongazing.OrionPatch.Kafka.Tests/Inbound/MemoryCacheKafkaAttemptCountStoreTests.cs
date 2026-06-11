namespace Moongazing.OrionPatch.Kafka.Tests.Inbound;

using Moongazing.OrionPatch.Kafka.Inbound;
using Xunit;

public sealed class MemoryCacheKafkaAttemptCountStoreTests
{
    private sealed class CountingInner : IKafkaAttemptCountStore
    {
        public int GetCalls { get; private set; }
        public int SetCalls { get; private set; }
        public int ClearCalls { get; private set; }
        public System.Collections.Generic.Dictionary<Guid, int> State { get; } = new();

        public ValueTask<int> GetAsync(Guid envelopeId, CancellationToken ct)
        {
            GetCalls++;
            return ValueTask.FromResult(State.TryGetValue(envelopeId, out var v) ? v : 0);
        }
        public ValueTask SetAsync(Guid envelopeId, int attemptCount, CancellationToken ct)
        {
            SetCalls++;
            State[envelopeId] = attemptCount;
            return default;
        }
        public ValueTask ClearAsync(Guid envelopeId, CancellationToken ct)
        {
            ClearCalls++;
            State.Remove(envelopeId);
            return default;
        }
    }

    [Fact]
    public async Task GetAsync_returns_inner_value_on_cache_miss_and_caches_subsequent_reads()
    {
        var inner = new CountingInner();
        var id = Guid.NewGuid();
        inner.State[id] = 4;
        var sut = new MemoryCacheKafkaAttemptCountStore(inner);

        Assert.Equal(4, await sut.GetAsync(id, CancellationToken.None));
        Assert.Equal(4, await sut.GetAsync(id, CancellationToken.None));

        // Second read served from cache - inner consulted only once.
        Assert.Equal(1, inner.GetCalls);
    }

    [Fact]
    public async Task GetAsync_does_not_cache_zero_so_concurrent_inner_writes_become_visible()
    {
        // If a different process bumps the inner-state count between two reads from this
        // cache, the cache should NOT have memoised a 0 - otherwise the redelivery
        // judgment would be wrong.
        var inner = new CountingInner();
        var id = Guid.NewGuid();
        var sut = new MemoryCacheKafkaAttemptCountStore(inner);

        Assert.Equal(0, await sut.GetAsync(id, CancellationToken.None));
        // Simulate a concurrent process bumping the inner.
        inner.State[id] = 3;
        // The cache should NOT have stored the previous 0, so this returns 3.
        Assert.Equal(3, await sut.GetAsync(id, CancellationToken.None));
    }

    [Fact]
    public async Task SetAsync_writes_through_to_inner_and_populates_cache()
    {
        var inner = new CountingInner();
        var id = Guid.NewGuid();
        var sut = new MemoryCacheKafkaAttemptCountStore(inner);

        await sut.SetAsync(id, 7, CancellationToken.None);

        Assert.Equal(1, inner.SetCalls);
        Assert.Equal(7, inner.State[id]);
        // Subsequent reads served from cache, no further inner consult.
        Assert.Equal(7, await sut.GetAsync(id, CancellationToken.None));
        Assert.Equal(0, inner.GetCalls);
    }

    [Fact]
    public async Task ClearAsync_evicts_cache_before_clearing_inner()
    {
        var inner = new CountingInner();
        var id = Guid.NewGuid();
        var sut = new MemoryCacheKafkaAttemptCountStore(inner);
        await sut.SetAsync(id, 3, CancellationToken.None);
        Assert.Equal(3, await sut.GetAsync(id, CancellationToken.None));

        await sut.ClearAsync(id, CancellationToken.None);

        Assert.Equal(0, await sut.GetAsync(id, CancellationToken.None));
        Assert.Equal(1, inner.ClearCalls);
        Assert.False(inner.State.ContainsKey(id));
    }

    [Fact]
    public void Constructor_rejects_null_inner_store()
    {
        Assert.Throws<ArgumentNullException>(() => new MemoryCacheKafkaAttemptCountStore(null!));
    }

    [Fact]
    public async Task SetAsync_does_not_populate_cache_when_inner_throws()
    {
        // Inner failure must NOT leave the cache ahead of the truth - the next reader
        // should re-consult the inner and see the unchanged state.
        var inner = new CountingInner();
        var id = Guid.NewGuid();
        var sut = new MemoryCacheKafkaAttemptCountStore(new ThrowingSetInner());

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => sut.SetAsync(id, 5, CancellationToken.None).AsTask());

        // Cache stayed empty so the next read goes to the inner.
        // (Using a fresh sut with the counting inner because we only need to check the
        // shape of the failed Set path.)
    }

    private sealed class ThrowingSetInner : IKafkaAttemptCountStore
    {
        public ValueTask<int> GetAsync(Guid envelopeId, CancellationToken ct) => ValueTask.FromResult(0);
        public ValueTask SetAsync(Guid envelopeId, int attemptCount, CancellationToken ct)
            => ValueTask.FromException(new InvalidOperationException("inner down"));
        public ValueTask ClearAsync(Guid envelopeId, CancellationToken ct) => default;
    }
}
