namespace Moongazing.OrionPatch.Abstractions;

/// <summary>
/// Delegate-backed <see cref="IOutboxTenantResolver"/> for consumers who already have a
/// resolution function (a closure over <c>IHttpContextAccessor</c>, an ambient AsyncLocal,
/// etc.) and do not want to declare a one-off class.
/// </summary>
public sealed class DelegateOutboxTenantResolver : IOutboxTenantResolver
{
    private readonly Func<string?> resolver;

    /// <summary>Wraps the supplied delegate.</summary>
    /// <param name="resolver">A non-null delegate returning the current tenant or <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="resolver"/> is <see langword="null"/>.</exception>
    public DelegateOutboxTenantResolver(Func<string?> resolver)
    {
        ArgumentNullException.ThrowIfNull(resolver);
        this.resolver = resolver;
    }

    /// <inheritdoc />
    public string? Resolve() => resolver();
}
