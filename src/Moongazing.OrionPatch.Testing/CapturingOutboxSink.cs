namespace Moongazing.OrionPatch.Testing;

using System.Collections.Concurrent;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Models;

/// <summary>
/// Thread-safe <see cref="IOutboxSink"/> that records every dispatched envelope
/// into an in-memory list for later inspection by tests. Never throws; never
/// drops; preserves dispatch order under single-threaded test drivers.
/// </summary>
public sealed class CapturingOutboxSink : IOutboxSink
{
    private readonly ConcurrentQueue<OutboxEnvelope> received = new();

    /// <summary>Snapshot of envelopes that have been sent to this sink, in dispatch order.</summary>
    public IReadOnlyList<OutboxEnvelope> Sent => received.ToArray();

    /// <inheritdoc/>
    public Task SendAsync(OutboxEnvelope envelope, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        received.Enqueue(envelope);
        return Task.CompletedTask;
    }

    /// <summary>Reset the captured-envelope list. Safe to call between scenarios.</summary>
    public void Clear()
    {
#if NET9_0_OR_GREATER
        received.Clear();
#else
        while (received.TryDequeue(out _)) { }
#endif
    }
}
