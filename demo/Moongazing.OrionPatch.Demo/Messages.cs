namespace Moongazing.OrionPatch.Demo;

/// <summary>
/// Domain messages used across the demo scenarios. These are plain records that get
/// JSON-serialized into outbox rows and round-tripped through the sink.
/// </summary>
public sealed record OrderConfirmed(Guid OrderId, int TotalCents);

/// <summary>Original V1 shape of the shipped event, kept around for the message-type registry rename demo.</summary>
public sealed record OrderShipped(Guid OrderId, string Carrier);

/// <summary>V2 shape that replaces <see cref="OrderShipped"/> after a rename, mapped to a versioned logical name.</summary>
public sealed record OrderShippedV2(Guid OrderId, string Carrier, string TrackingNumber);

/// <summary>Event used by the channel-sink fan-out demo.</summary>
public sealed record PaymentCaptured(Guid PaymentId, int AmountCents);
