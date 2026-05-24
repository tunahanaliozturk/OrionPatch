namespace Moongazing.OrionPatch.Configuration;

/// <summary>
/// Built-in factories for retry backoff strategies used by the dispatcher
/// (<see cref="OrionPatchOptions.BackoffStrategy"/>).
/// </summary>
public static class BackoffStrategy
{
    /// <summary>
    /// Exponential doubling: attempt 1 = <paramref name="initial"/>, attempt 2 = initial*2,
    /// attempt 3 = initial*4, capped at <paramref name="max"/>.
    /// Non-positive attempts yield <see cref="TimeSpan.Zero"/>.
    /// </summary>
    /// <param name="initial">Delay returned for attempt 1.</param>
    /// <param name="max">Upper bound on the returned delay regardless of attempt number.</param>
    /// <returns>A delegate mapping a 1-based attempt number to a delay.</returns>
    public static Func<int, TimeSpan> Exponential(TimeSpan initial, TimeSpan max) =>
        attempt =>
        {
            if (attempt <= 0)
            {
                return TimeSpan.Zero;
            }

            long ticks = initial.Ticks * (1L << Math.Min(attempt - 1, 30));
            return TimeSpan.FromTicks(Math.Min(ticks, max.Ticks));
        };

    /// <summary>
    /// Constant delay regardless of attempt number.
    /// </summary>
    /// <param name="delay">The delay returned for every attempt.</param>
    /// <returns>A delegate that ignores the attempt number and always returns <paramref name="delay"/>.</returns>
    public static Func<int, TimeSpan> Fixed(TimeSpan delay) => _ => delay;
}
