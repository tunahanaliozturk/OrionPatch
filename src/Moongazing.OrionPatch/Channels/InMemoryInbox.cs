namespace Moongazing.OrionPatch.Channels;

using System.Collections.Concurrent;
using Moongazing.OrionPatch.Abstractions;

/// <summary>
/// In-process <see cref="IInbox"/> backed by a <see cref="ConcurrentDictionary{TKey, TValue}"/>.
/// Bounded by the host's RAM; intended for tests, demo apps, and single-instance services
/// where consumer-side dedup does not need to survive a process restart.
/// </summary>
/// <remarks>
/// EF Core persistence ships in v0.2.3. The dedup contract is identical so consumers swap
/// implementations without changing handler code.
/// </remarks>
public sealed class InMemoryInbox : IInbox
{
    private readonly ConcurrentDictionary<Guid, byte> seen = new();

    /// <inheritdoc />
    public ValueTask<bool> TryAcceptAsync(Guid messageId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(seen.TryAdd(messageId, 0));
    }

    /// <inheritdoc />
    public ValueTask RollbackAsync(Guid messageId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        seen.TryRemove(messageId, out _);
        return default;
    }

    /// <summary>
    /// Test-only helper that returns the number of distinct message ids the inbox has accepted.
    /// </summary>
    public int Count => seen.Count;

    /// <summary>
    /// Test-only helper that clears the dedup state. Useful between fact runs in a shared
    /// xunit collection.
    /// </summary>
    public void Reset() => seen.Clear();
}
