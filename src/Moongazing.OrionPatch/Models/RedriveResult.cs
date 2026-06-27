namespace Moongazing.OrionPatch.Models;

/// <summary>
/// The outcome of a redrive (replay) operation: how many dead-lettered messages were
/// re-enqueued into the active outbox and how many were skipped as a clean no-op. Returned by
/// <see cref="Moongazing.OrionPatch.Abstractions.IDeadLetterReplayStore"/>.
/// </summary>
/// <remarks>
/// <para>
/// A single-id redrive reports either <c>(1, 0)</c> when it re-enqueued the message or
/// <c>(0, 1)</c> when the message was already redriven or absent (the idempotent no-op). A bulk
/// redrive sums the per-message outcomes across every batch it processed.
/// </para>
/// <para>
/// <see cref="Skipped"/> counts only ids the operation considered but did not move (already gone
/// from the dead-letter store). It is NOT an error count: a redrive that touches no rows because
/// the filter matched nothing returns <see cref="Empty"/> (<c>0, 0</c>), not a skip.
/// </para>
/// </remarks>
/// <param name="Redriven">Number of dead-lettered messages re-enqueued into the active outbox as fresh pending rows.</param>
/// <param name="Skipped">Number of ids considered but not moved because they were already redriven or no longer present (idempotent no-op).</param>
public readonly record struct RedriveResult(int Redriven, int Skipped)
{
    /// <summary>A result that moved and skipped nothing. The identity element for <see cref="op_Addition"/>.</summary>
    public static RedriveResult Empty => default;

    /// <summary>Total ids the operation considered (<see cref="Redriven"/> + <see cref="Skipped"/>).</summary>
    public int Total => Redriven + Skipped;

    /// <summary>Sum two results component-wise so a bulk redrive can fold per-batch outcomes into one total.</summary>
    /// <param name="left">First result.</param>
    /// <param name="right">Second result.</param>
    /// <returns>A result whose counts are the component-wise sums.</returns>
    public static RedriveResult operator +(RedriveResult left, RedriveResult right)
        => new(left.Redriven + right.Redriven, left.Skipped + right.Skipped);

    /// <summary>Named alternative to <see cref="op_Addition"/> for languages without operator support.</summary>
    /// <param name="left">First result.</param>
    /// <param name="right">Second result.</param>
    /// <returns>A result whose counts are the component-wise sums.</returns>
    public static RedriveResult Add(RedriveResult left, RedriveResult right) => left + right;
}
