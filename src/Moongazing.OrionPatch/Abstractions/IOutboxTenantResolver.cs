namespace Moongazing.OrionPatch.Abstractions;

/// <summary>
/// Resolves the current tenant identifier at the moment <see cref="IOutbox.Enqueue{T}"/> is
/// called. Used by the enqueue path to stamp a standard header
/// (<see cref="TenantHeaderName"/>) on every outbox row without the caller having to remember
/// to pass <see cref="Models.OutboxEnqueueOptions.Headers"/> by hand.
/// </summary>
/// <remarks>
/// The default registration is <see cref="NullOutboxTenantResolver"/>, which always returns
/// <see langword="null"/> and preserves the v0.2.0 behaviour: callers who already pass a
/// tenant header via <c>OutboxEnqueueOptions.Headers["tenant-id"]</c> continue to work
/// unchanged. Replace the registration to opt in to ambient tenant capture.
/// </remarks>
public interface IOutboxTenantResolver
{
    /// <summary>The standard header key used for tenant attribution: <c>"tenant-id"</c>.</summary>
    public const string TenantHeaderName = "tenant-id";

    /// <summary>
    /// Resolve the current ambient tenant identifier, or <see langword="null"/> when no
    /// tenant is in scope. Called synchronously on the enqueue thread; implementations must
    /// not block.
    /// </summary>
    /// <returns>The current tenant identifier, or <see langword="null"/>.</returns>
    string? Resolve();
}
