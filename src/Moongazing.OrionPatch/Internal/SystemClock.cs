namespace Moongazing.OrionPatch.Internal;

using Moongazing.OrionPatch.Abstractions;

/// <summary>
/// Wall-clock <see cref="IOutboxDispatcherClock"/> backed by <see cref="DateTime.UtcNow"/>
/// and <see cref="Task.Delay(TimeSpan, CancellationToken)"/>. Registered by default.
/// </summary>
internal sealed class SystemClock : IOutboxDispatcherClock
{
    /// <inheritdoc />
    public DateTime UtcNow => DateTime.UtcNow;

    /// <inheritdoc />
    public Task DelayAsync(TimeSpan duration, CancellationToken cancellationToken = default) =>
        Task.Delay(duration, cancellationToken);
}
