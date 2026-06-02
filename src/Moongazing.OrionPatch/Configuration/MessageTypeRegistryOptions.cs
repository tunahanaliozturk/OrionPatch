namespace Moongazing.OrionPatch.Configuration;

/// <summary>
/// Options that govern <see cref="MessageTypeRegistry"/> behaviour at registry build time.
/// </summary>
public sealed class MessageTypeRegistryOptions
{
    /// <summary>
    /// When <see langword="true"/> (default), the enqueue path falls back to
    /// <c>type.FullName ?? type.Name</c> for unregistered CLR types and the dispatcher
    /// resolves unknown logical names by attempting <see cref="System.Type.GetType(string)"/>
    /// against the assembly-qualified form. Set <see langword="false"/> to require explicit
    /// mapping for every type that flows through the outbox; missing mappings then throw
    /// <see cref="System.InvalidOperationException"/> at enqueue or dispatch time.
    /// </summary>
    public bool AllowAssemblyQualifiedNameFallback { get; set; } = true;
}
