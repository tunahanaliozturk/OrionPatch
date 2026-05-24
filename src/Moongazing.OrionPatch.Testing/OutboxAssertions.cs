namespace Moongazing.OrionPatch.Testing;

using System.Text.Json;
using Moongazing.OrionPatch.Models;

/// <summary>
/// Fluent assertion helpers over <see cref="CapturingOutboxSink"/> and
/// <see cref="InMemoryOutboxStorage"/> for OrionPatch tests. All helpers
/// throw <see cref="InvalidOperationException"/> on miss so they integrate
/// cleanly with xUnit / NUnit / MSTest without taking a dependency on any
/// specific assertion library.
/// </summary>
public static class OutboxAssertions
{
    /// <summary>
    /// Assert at least one envelope of type <typeparamref name="T"/> was dispatched and
    /// (optionally) matches the supplied predicate. Returns the matched envelope so the
    /// caller can chain further assertions.
    /// </summary>
    /// <typeparam name="T">Expected payload CLR type; matched against
    /// <see cref="OutboxEnvelope.MessageType"/> via <c>typeof(T).FullName</c>.</typeparam>
    /// <param name="sink">The capturing sink whose <see cref="CapturingOutboxSink.Sent"/>
    /// list will be scanned; must be non-null.</param>
    /// <param name="predicate">Optional predicate evaluated against the deserialized payload.</param>
    /// <param name="jsonOptions">
    /// Optional JSON options used to deserialize payloads. Defaults to a fresh
    /// <see cref="JsonSerializerOptions"/> seeded from <see cref="JsonSerializerDefaults.Web"/>.
    /// </param>
    /// <returns>The first envelope whose message type and predicate both match.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="sink"/> is null.</exception>
    /// <exception cref="InvalidOperationException">Thrown when no matching envelope was found.</exception>
    public static OutboxEnvelope AssertDispatched<T>(
        this CapturingOutboxSink sink,
        Func<T, bool>? predicate = null,
        JsonSerializerOptions? jsonOptions = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(sink);

        var typeName = typeof(T).FullName ?? typeof(T).Name;
        var serializerOptions = jsonOptions ?? new JsonSerializerOptions(JsonSerializerDefaults.Web);

        foreach (var envelope in sink.Sent)
        {
            if (!string.Equals(envelope.MessageType, typeName, StringComparison.Ordinal))
            {
                continue;
            }

            if (predicate is null)
            {
                return envelope;
            }

            var deserialized = JsonSerializer.Deserialize<T>(envelope.Payload, serializerOptions);
            if (deserialized is not null && predicate(deserialized))
            {
                return envelope;
            }
        }

        throw new InvalidOperationException(
            $"No dispatched envelope of type '{typeName}' matched the predicate. " +
            $"Captured envelope count: {sink.Sent.Count}.");
    }

    /// <summary>
    /// Assert at least one row in <paramref name="storage"/> is in
    /// <see cref="OutboxStatus.DeadLettered"/> state and (optionally) matches the
    /// supplied predicate. Returns the matched row.
    /// </summary>
    /// <param name="storage">The in-memory storage to scan; must be non-null.</param>
    /// <param name="predicate">Optional predicate evaluated against the dead-lettered row.</param>
    /// <returns>The first dead-lettered row that matches the predicate (or any dead-lettered row when no predicate is supplied).</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="storage"/> is null.</exception>
    /// <exception cref="InvalidOperationException">Thrown when no matching dead-lettered row was found.</exception>
    public static OutboxRow AssertDeadLettered(
        this InMemoryOutboxStorage storage,
        Func<OutboxRow, bool>? predicate = null)
    {
        ArgumentNullException.ThrowIfNull(storage);

        foreach (var row in storage.Rows)
        {
            if (row.Status != OutboxStatus.DeadLettered)
            {
                continue;
            }
            if (predicate is null || predicate(row))
            {
                return row;
            }
        }

        throw new InvalidOperationException("No dead-lettered row matched the predicate.");
    }
}
