namespace Moongazing.OrionPatch.EntityFrameworkCore;

/// <summary>
/// EF Core-backed inbox row. One row per accepted message id; persists the v0.2.2
/// <see cref="Abstractions.IInbox"/> contract across process restarts.
/// </summary>
public sealed class InboxRow
{
    /// <summary>Stable message id under dedup.</summary>
    public Guid MessageId { get; set; }

    /// <summary>UTC timestamp when the message was first accepted.</summary>
    public DateTime AcceptedAtUtc { get; set; }

    /// <summary>
    /// Optional consumer name. Useful when one inbox table is shared across multiple
    /// consumers and each one needs to dedup independently. The default
    /// <see cref="EfCoreInbox"/> registration does not populate this; consumers wire it
    /// via the <c>UseInbox</c> options.
    /// </summary>
    public string? Consumer { get; set; }
}
