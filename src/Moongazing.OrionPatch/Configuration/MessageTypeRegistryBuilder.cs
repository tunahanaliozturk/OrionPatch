namespace Moongazing.OrionPatch.Configuration;

/// <summary>
/// Fluent builder for <see cref="MessageTypeRegistry"/>. Surfaces <c>Map</c> for individual
/// type entries and <c>MapVersion</c> for documenting renamed-type chains.
/// </summary>
public sealed class MessageTypeRegistryBuilder
{
    private readonly Dictionary<Type, string> clrToLogical = new();
    private readonly Dictionary<string, Type> logicalToClr = new(StringComparer.Ordinal);
    private readonly MessageTypeRegistryOptions options = new();

    /// <summary>
    /// Map a CLR message type to a logical wire name.
    /// </summary>
    /// <typeparam name="T">The message CLR type.</typeparam>
    /// <param name="logicalName">The wire-stable logical name to use on enqueue and dispatch.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="logicalName"/> is null or whitespace.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the type or the logical name is already mapped.</exception>
    /// <returns>The builder, for fluent chaining.</returns>
    public MessageTypeRegistryBuilder Map<T>(string logicalName) where T : class
    {
        if (string.IsNullOrWhiteSpace(logicalName))
        {
            throw new ArgumentException("Logical name must be non-empty.", nameof(logicalName));
        }

        var type = typeof(T);
        if (clrToLogical.TryGetValue(type, out var existingName))
        {
            throw new InvalidOperationException(
                $"Type {type.FullName} is already mapped to '{existingName}'.");
        }
        if (logicalToClr.TryGetValue(logicalName, out var existingType))
        {
            throw new InvalidOperationException(
                $"Logical name '{logicalName}' is already mapped to {existingType.FullName}.");
        }

        clrToLogical[type] = logicalName;
        logicalToClr[logicalName] = type;
        return this;
    }

    /// <summary>
    /// Document a rename-with-version chain: <typeparamref name="TOld"/> is replaced by
    /// <typeparamref name="TNew"/> at the supplied versioned logical name. The new type is
    /// registered with the supplied <paramref name="newLogicalName"/>. The old type stays
    /// resolvable only if it was previously registered via <see cref="Map{T}(string)"/>.
    /// </summary>
    public MessageTypeRegistryBuilder MapVersion<TOld, TNew>(string newLogicalName)
        where TOld : class
        where TNew : class
    {
        return Map<TNew>(newLogicalName);
    }

    /// <summary>
    /// Configure registry-wide behaviour (fallback policy etc.).
    /// </summary>
    public MessageTypeRegistryBuilder Configure(Action<MessageTypeRegistryOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        configure(options);
        return this;
    }

    /// <summary>
    /// Build the immutable registry. The builder is single-use; further mutations would race
    /// with consumers but the API does not enforce that today.
    /// </summary>
    public MessageTypeRegistry Build() => new(clrToLogical, logicalToClr, options);
}
