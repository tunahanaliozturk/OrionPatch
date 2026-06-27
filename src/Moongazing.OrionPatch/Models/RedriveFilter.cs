namespace Moongazing.OrionPatch.Models;

/// <summary>
/// Selects which dead-lettered messages a bulk redrive
/// (<see cref="Moongazing.OrionPatch.Abstractions.IDeadLetterReplayStore.RedriveAsync(RedriveFilter, int, System.Threading.CancellationToken)"/>)
/// re-enqueues. All set facets are combined with AND; an unset facet (<see langword="null"/>)
/// places no constraint. The default value (<see cref="All"/>) matches every dead-lettered message.
/// </summary>
/// <remarks>
/// <para>
/// The typical operator workflow is "a downstream was out from 14:00 to 15:00 and every
/// <c>OrderShipped</c> in that window dead-lettered; redrive that whole class now that it is back".
/// <see cref="MessageType"/> narrows by logical type, and
/// <see cref="DeadLetteredAtOrAfterUtc"/> / <see cref="DeadLetteredBeforeUtc"/> bound the dead-letter
/// window (half-open: <c>[from, to)</c>) so a re-run with a moved <c>from</c> does not re-touch
/// rows already handled.
/// </para>
/// <para>
/// The window is matched against
/// <see cref="DeadLetteredMessage.DeadLetteredAtUtc"/> (when the message entered the dead-letter
/// store), not its original enqueue or occurrence time, because that is the axis an operator
/// reasons about when recovering from a dated outage.
/// </para>
/// </remarks>
/// <param name="MessageType">
/// When set, only messages whose logical <see cref="DeadLetteredMessage.MessageType"/> equals this
/// value are redriven. The comparison is an exact, case-sensitive, ordinal match (no normalization,
/// trimming, or culture folding) and is identical across every store: the in-memory store evaluates
/// it via <see cref="Matches"/> using <see cref="System.StringComparison.Ordinal"/>, and the
/// relational stores translate it to a binary-collation <c>WHERE MessageType = @value</c>, so the
/// same filter selects the same set on either backend.
/// </param>
/// <param name="DeadLetteredAtOrAfterUtc">When set, only messages dead-lettered at or after this UTC instant are redriven (inclusive lower bound).</param>
/// <param name="DeadLetteredBeforeUtc">When set, only messages dead-lettered strictly before this UTC instant are redriven (exclusive upper bound).</param>
public readonly record struct RedriveFilter(
    string? MessageType = null,
    DateTime? DeadLetteredAtOrAfterUtc = null,
    DateTime? DeadLetteredBeforeUtc = null)
{
    /// <summary>A filter with no constraints: matches every dead-lettered message.</summary>
    public static RedriveFilter All => default;

    /// <summary>Evaluate this filter against a candidate message. Used by stores whose backing collection cannot push the predicate to a query.</summary>
    /// <param name="message">Candidate dead-lettered message; must be non-null.</param>
    /// <returns><see langword="true"/> when the message satisfies every set facet.</returns>
    /// <exception cref="System.ArgumentNullException">Thrown when <paramref name="message"/> is null.</exception>
    public bool Matches(DeadLetteredMessage message)
    {
        System.ArgumentNullException.ThrowIfNull(message);

        if (MessageType is not null && !string.Equals(message.MessageType, MessageType, System.StringComparison.Ordinal))
        {
            return false;
        }

        if (DeadLetteredAtOrAfterUtc is { } from && message.DeadLetteredAtUtc < from)
        {
            return false;
        }

        if (DeadLetteredBeforeUtc is { } to && message.DeadLetteredAtUtc >= to)
        {
            return false;
        }

        return true;
    }
}
