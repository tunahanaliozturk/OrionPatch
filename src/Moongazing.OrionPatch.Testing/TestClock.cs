namespace Moongazing.OrionPatch.Testing;

using Moongazing.OrionPatch.Abstractions;

/// <summary>
/// Controllable <see cref="IOutboxDispatcherClock"/> for tests. UTC time
/// advances only when <see cref="Advance"/> or <see cref="Set"/> is called;
/// <see cref="DelayAsync"/> completes immediately so the dispatcher loop
/// doesn't spin against real wall-clock seconds. Tests that need to verify
/// post-delay behaviour must call <see cref="Advance"/> explicitly.
/// </summary>
public sealed class TestClock : IOutboxDispatcherClock
{
    private DateTime utcNow;

    /// <summary>Construct the clock at the supplied initial UTC time.</summary>
    /// <param name="initialUtc">
    /// Initial value for <see cref="UtcNow"/>. Defaults to <see cref="DateTime.UtcNow"/>
    /// at construction time.
    /// </param>
    public TestClock(DateTime? initialUtc = null)
    {
        utcNow = initialUtc ?? DateTime.UtcNow;
    }

    /// <inheritdoc/>
    public DateTime UtcNow => utcNow;

    /// <summary>Shift the clock forward by the supplied non-negative duration.</summary>
    /// <param name="duration">Duration to add; must be non-negative.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="duration"/> is negative.</exception>
    public void Advance(TimeSpan duration)
    {
        if (duration < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(duration), "Cannot advance the clock by a negative duration.");
        }
        utcNow = utcNow.Add(duration);
    }

    /// <summary>Set the clock to a specific UTC time.</summary>
    /// <param name="newUtcNow">The new value of <see cref="UtcNow"/>.</param>
    public void Set(DateTime newUtcNow) => utcNow = newUtcNow;

    /// <inheritdoc/>
    public Task DelayAsync(TimeSpan duration, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.CompletedTask;
    }
}
