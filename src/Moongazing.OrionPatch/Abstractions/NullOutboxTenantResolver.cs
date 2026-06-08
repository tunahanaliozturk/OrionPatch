namespace Moongazing.OrionPatch.Abstractions;

/// <summary>
/// Default <see cref="IOutboxTenantResolver"/> registration. Always returns
/// <see langword="null"/>, so the enqueue path falls through to the caller-supplied
/// <c>OutboxEnqueueOptions.Headers["tenant-id"]</c> if any. Behaviour is byte-for-byte
/// identical to the v0.2.0 enqueue path.
/// </summary>
public sealed class NullOutboxTenantResolver : IOutboxTenantResolver
{
    /// <inheritdoc />
    public string? Resolve() => null;
}
