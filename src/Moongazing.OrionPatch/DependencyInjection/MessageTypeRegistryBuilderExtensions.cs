namespace Moongazing.OrionPatch.DependencyInjection;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Moongazing.OrionPatch.Configuration;

/// <summary>
/// Fluent registration of <see cref="MessageTypeRegistry"/> on the <see cref="OrionPatchBuilder"/>.
/// </summary>
public static class MessageTypeRegistryBuilderExtensions
{
    /// <summary>
    /// Register a <see cref="MessageTypeRegistry"/> built from the supplied configuration delegate.
    /// Replaces the default empty registry so the enqueue path and dispatcher consult the user-supplied
    /// logical names. Multiple calls are not additive; the last call wins.
    /// </summary>
    /// <param name="builder">The OrionPatch DI builder.</param>
    /// <param name="configure">Builder callback registering type maps and options.</param>
    /// <returns>The same builder for chaining.</returns>
    public static OrionPatchBuilder UseMessageTypeRegistry(
        this OrionPatchBuilder builder,
        Action<MessageTypeRegistryBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        var registryBuilder = new MessageTypeRegistryBuilder();
        configure(registryBuilder);
        var registry = registryBuilder.Build();

        builder.Services.RemoveAll<MessageTypeRegistry>();
        builder.Services.AddSingleton(registry);

        return builder;
    }
}
