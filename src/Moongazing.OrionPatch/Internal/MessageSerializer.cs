namespace Moongazing.OrionPatch.Internal;

using System.Text.Json;

/// <summary>
/// Wraps <see cref="JsonSerializer"/> with the configured <see cref="JsonSerializerOptions"/>.
/// Serializes against the runtime type so polymorphic payloads round-trip correctly.
/// </summary>
internal sealed class MessageSerializer
{
    private readonly JsonSerializerOptions jsonOptions;

    /// <summary>Create a serializer bound to the given options.</summary>
    /// <param name="jsonOptions">JSON options carried on <see cref="Configuration.OrionPatchOptions.JsonOptions"/>.</param>
    public MessageSerializer(JsonSerializerOptions jsonOptions)
    {
        this.jsonOptions = jsonOptions;
    }

    /// <summary>Serialize <paramref name="value"/> using its runtime type.</summary>
    /// <typeparam name="T">Compile-time message type; serialization uses <c>value.GetType()</c>.</typeparam>
    /// <param name="value">Message instance.</param>
    /// <returns>JSON payload string.</returns>
    public string Serialize<T>(T value) where T : class =>
        JsonSerializer.Serialize(value, value.GetType(), jsonOptions);
}
