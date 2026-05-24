namespace Moongazing.OrionPatch.Sample;

/// <summary>Sample domain event: an order has been confirmed.</summary>
/// <param name="Id">Order identifier.</param>
/// <param name="TotalCents">Order total in minor currency units.</param>
internal sealed record OrderConfirmed(Guid Id, int TotalCents);
