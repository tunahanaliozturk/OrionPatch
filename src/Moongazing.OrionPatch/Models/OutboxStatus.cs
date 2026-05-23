namespace Moongazing.OrionPatch.Models;

/// <summary>
/// Lifecycle state of an outbox row.
/// </summary>
public enum OutboxStatus : byte
{
    /// <summary>Row is waiting to be claimed by a dispatcher.</summary>
    Pending = 0,
    /// <summary>Row has been claimed by a dispatcher under a lease.</summary>
    Claimed = 1,
    /// <summary>Row has been successfully dispatched to the sink.</summary>
    Processed = 2,
    /// <summary>Row exceeded MaxAttempts and was abandoned.</summary>
    DeadLettered = 3,
}
