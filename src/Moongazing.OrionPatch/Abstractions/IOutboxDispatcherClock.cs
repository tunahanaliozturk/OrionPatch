namespace Moongazing.OrionPatch.Abstractions;

/// <summary>
/// Time + delay abstraction so tests can drive the dispatcher deterministically.
/// </summary>
public interface IOutboxDispatcherClock
{
    /// <summary>Current UTC time.</summary>
    DateTime UtcNow { get; }

    /// <summary>Asynchronously wait for the given duration.</summary>
    /// <param name="duration">How long to wait.</param>
    /// <param name="cancellationToken">Cancellation token observed during the wait.</param>
    Task DelayAsync(TimeSpan duration, CancellationToken cancellationToken = default);
}
