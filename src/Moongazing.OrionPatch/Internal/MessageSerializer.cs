namespace Moongazing.OrionPatch.Internal;

using System.Text.Json;

/// <summary>
/// Wraps <see cref="JsonSerializer"/> with the configured <see cref="JsonSerializerOptions"/>.
/// Serializes against the runtime type so polymorphic payloads round-trip correctly.
/// </summary>
internal sealed class MessageSerializer
{
    /// <summary>Create a serializer bound to the given options.</summary>
    /// <param name="jsonOptions">JSON options carried on <see cref="Configuration.OrionPatchOptions.JsonOptions"/>; must be non-null.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="jsonOptions"/> is <see langword="null"/>.</exception>
    public MessageSerializer(JsonSerializerOptions jsonOptions)
    {
        Options = jsonOptions ?? throw new ArgumentNullException(nameof(jsonOptions));
    }

    /// <summary>
    /// The configured options instance; exposed so callers that need to serialize related
    /// metadata (e.g., header dictionaries) honor the same JSON pipeline as the payload.
    /// </summary>
    public JsonSerializerOptions Options { get; }

    /// <summary>Serialize <paramref name="value"/> using its runtime type.</summary>
    /// <typeparam name="T">Compile-time message type; serialization uses <c>value.GetType()</c>.</typeparam>
    /// <param name="value">The message instance; must be non-null.</param>
    /// <returns>JSON payload string.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="value"/> is <see langword="null"/>.</exception>
    public string Serialize<T>(T value) where T : class
    {
        ArgumentNullException.ThrowIfNull(value);
        return JsonSerializer.Serialize(value, value.GetType(), Options);
    }
}
