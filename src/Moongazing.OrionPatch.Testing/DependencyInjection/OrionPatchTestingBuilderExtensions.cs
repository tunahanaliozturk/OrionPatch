namespace Moongazing.OrionPatch.Testing.DependencyInjection;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.DependencyInjection;

/// <summary>
/// <see cref="OrionPatchBuilder"/> extensions for wiring the in-memory test
/// backend (<see cref="InMemoryOutboxStorage"/> + <see cref="InMemoryOutbox"/>)
/// into a host's service collection.
/// </summary>
public static class OrionPatchTestingBuilderExtensions
{
    /// <summary>
    /// Register <see cref="InMemoryOutboxStorage"/> and <see cref="InMemoryOutbox"/>
    /// as singletons, replacing any previously-registered <see cref="IOutboxStorage"/>
    /// or <see cref="IOutbox"/> descriptors so the resulting <see cref="IServiceProvider"/>
    /// resolves exactly one storage and one outbox.
    /// </summary>
    /// <param name="builder">Builder returned from <c>AddOrionPatch</c>; must be non-null.</param>
    /// <returns>The same <paramref name="builder"/> for fluent chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="builder"/> is null.</exception>
    public static OrionPatchBuilder UseInMemory(this OrionPatchBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Services.RemoveAll<IOutbox>();
        builder.Services.RemoveAll<IOutboxStorage>();
        builder.Services.RemoveAll<InMemoryOutboxStorage>();
        builder.Services.RemoveAll<InMemoryOutbox>();

        builder.Services.AddSingleton<InMemoryOutboxStorage>();
        builder.Services.AddSingleton<IOutboxStorage>(sp => sp.GetRequiredService<InMemoryOutboxStorage>());

        builder.Services.AddSingleton(sp => new InMemoryOutbox(sp.GetRequiredService<InMemoryOutboxStorage>()));
        builder.Services.AddSingleton<IOutbox>(sp => sp.GetRequiredService<InMemoryOutbox>());

        return builder;
    }
}
