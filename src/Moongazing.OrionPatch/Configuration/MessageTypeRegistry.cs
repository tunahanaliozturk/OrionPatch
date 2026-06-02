namespace Moongazing.OrionPatch.Configuration;

using System.Collections.Frozen;

/// <summary>
/// Bidirectional mapping between logical message-type names (e.g. <c>"OrderShipped"</c>,
/// <c>"OrderShipped.V2"</c>) and their CLR types. Lets consumers rename or refactor message
/// types without breaking in-flight outbox rows: the row keeps the logical name, and the
/// registry resolves it back to a CLR type at dispatch time.
/// </summary>
/// <remarks>
/// <para>
/// Build via <see cref="MessageTypeRegistryBuilder"/> off the DI surface
/// (<c>services.AddOrionPatch().UseMessageTypeRegistry(...)</c>). The built registry is
/// immutable and thread-safe.
/// </para>
/// <para>
/// If no mapping is registered for a given type, the enqueue path falls back to
/// <see cref="System.Type.FullName"/> (or assembly-qualified name if
/// <see cref="MessageTypeRegistryOptions.AllowAssemblyQualifiedNameFallback"/> is
/// <see langword="true"/>, the default). A registered mapping always wins over the fallback.
/// </para>
/// </remarks>
public sealed class MessageTypeRegistry
{
    private readonly FrozenDictionary<Type, string> clrToLogical;
    private readonly FrozenDictionary<string, Type> logicalToClr;

    /// <summary>
    /// Whether the enqueue path may fall back to <see cref="Type.FullName"/> for unmapped
    /// types. Snapshotted from <see cref="MessageTypeRegistryOptions.AllowAssemblyQualifiedNameFallback"/>
    /// at construction time; cannot be mutated after the registry is built.
    /// </summary>
    public bool AllowAssemblyQualifiedNameFallback { get; }

    internal MessageTypeRegistry(
        IReadOnlyDictionary<Type, string> clrToLogical,
        IReadOnlyDictionary<string, Type> logicalToClr,
        MessageTypeRegistryOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        this.clrToLogical = clrToLogical.ToFrozenDictionary();
        this.logicalToClr = logicalToClr.ToFrozenDictionary(StringComparer.Ordinal);
        AllowAssemblyQualifiedNameFallback = options.AllowAssemblyQualifiedNameFallback;
    }

    /// <summary>
    /// Look up the registered logical name for a CLR message type, or <see langword="null"/>
    /// if no mapping was registered. The enqueue path uses this; a null result means the
    /// caller should fall back to the type's own name.
    /// </summary>
    public string? ResolveLogicalName(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        return clrToLogical.TryGetValue(messageType, out var name) ? name : null;
    }

    /// <summary>
    /// Look up the CLR type registered under a logical name, or <see langword="null"/> if no
    /// mapping is registered. The dispatcher and broker sinks use this to round-trip an
    /// outbox row's <c>MessageType</c> back to a CLR type for deserialisation.
    /// </summary>
    public Type? ResolveClrType(string logicalName)
    {
        ArgumentNullException.ThrowIfNull(logicalName);
        return logicalToClr.TryGetValue(logicalName, out var type) ? type : null;
    }

    /// <summary>
    /// Empty registry. Equivalent to "every type uses its CLR-derived name" - useful as a
    /// non-null default when DI has not registered an explicit registry yet.
    /// </summary>
    public static MessageTypeRegistry Empty { get; } = new(
        new Dictionary<Type, string>(),
        new Dictionary<string, Type>(StringComparer.Ordinal),
        new MessageTypeRegistryOptions());
}
