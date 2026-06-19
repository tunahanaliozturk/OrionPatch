namespace Moongazing.OrionPatch.Models;

/// <summary>
/// The final failure context captured when an outbox row is routed to the dead-letter store.
/// Passed to <see cref="Moongazing.OrionPatch.Abstractions.IDeadLetterStore.DeadLetterAsync"/>
/// so the resulting <see cref="DeadLetteredMessage"/> records why and when the row was abandoned.
/// </summary>
/// <param name="FinalError">Truncated error text from the final dispatch attempt.</param>
/// <param name="AttemptCount">Total number of attempts the row accumulated before it was abandoned.</param>
/// <param name="DeadLetteredAtUtc">UTC instant at which the row is being routed to the dead-letter store.</param>
public readonly record struct DeadLetterContext(string FinalError, int AttemptCount, DateTime DeadLetteredAtUtc);
