namespace Moongazing.OrionPatch.Internal;

using Moongazing.OrionPatch.Configuration;
using Moongazing.OrionPatch.Models;

/// <summary>
/// Resolves the <c>MessageType</c> string for an outbox row. Precedence:
/// <list type="number">
///   <item>Per-enqueue override on <see cref="OutboxEnqueueOptions.MessageType"/>.</item>
///   <item>Logical name registered in the <see cref="MessageTypeRegistry"/>.</item>
///   <item>Fallback to <see cref="Type.FullName"/> (or <c>Type.Name</c> for open generics),
///     gated by <see cref="MessageTypeRegistryOptions.AllowAssemblyQualifiedNameFallback"/>.</item>
/// </list>
/// </summary>
internal sealed class MessageTypeNameResolver
{
    private readonly MessageTypeRegistry registry;

    public MessageTypeNameResolver(MessageTypeRegistry? registry = null)
    {
        this.registry = registry ?? MessageTypeRegistry.Empty;
    }

    /// <summary>Resolve the message-type name to stamp on the envelope.</summary>
    /// <param name="type">The runtime type of the message.</param>
    /// <param name="options">Optional per-enqueue overrides; may be <see langword="null"/>.</param>
    /// <returns>The override if set; otherwise the registered logical name; otherwise the
    /// CLR-derived name when fallback is enabled.</returns>
    /// <exception cref="InvalidOperationException">Thrown when no mapping is registered and
    /// fallback is disabled via <see cref="MessageTypeRegistryOptions.AllowAssemblyQualifiedNameFallback"/>.</exception>
    public string Resolve(Type type, OutboxEnqueueOptions? options)
    {
        if (!string.IsNullOrEmpty(options?.MessageType))
        {
            return options.MessageType!;
        }

        var registered = registry.ResolveLogicalName(type);
        if (registered is not null)
        {
            return registered;
        }

        if (!registry.AllowAssemblyQualifiedNameFallback)
        {
            throw new InvalidOperationException(
                $"No MessageTypeRegistry mapping is registered for {type.FullName ?? type.Name} " +
                "and fallback to the CLR-derived name is disabled. " +
                "Add a .Map<T>(\"logical-name\") entry or set " +
                "MessageTypeRegistryOptions.AllowAssemblyQualifiedNameFallback to true.");
        }

        return type.FullName ?? type.Name;
    }
}
