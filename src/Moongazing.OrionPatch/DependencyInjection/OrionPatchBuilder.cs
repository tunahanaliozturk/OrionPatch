namespace Moongazing.OrionPatch.DependencyInjection;

using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Fluent surface returned from <see cref="OrionPatchServiceCollectionExtensions.AddOrionPatch"/>.
/// Carries the <see cref="IServiceCollection"/> so sink and storage registration extensions
/// can chain off it.
/// </summary>
public sealed class OrionPatchBuilder
{
    /// <summary>Create a builder around the supplied service collection.</summary>
    /// <param name="services">The host service collection; must be non-null.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="services"/> is null.</exception>
    public OrionPatchBuilder(IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        Services = services;
    }

    /// <summary>The underlying service collection. Use this to add services directly when an extension method does not yet exist.</summary>
    public IServiceCollection Services { get; }
}
