namespace Moongazing.OrionPatch.Testing;

using System.Text.Json;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Models;

/// <summary>
/// <see cref="IOutbox"/> companion for tests that don't need an EF Core
/// <c>DbContext</c>. Each <see cref="Enqueue{T}"/> call serializes the message
/// to JSON and writes the resulting row directly into the supplied
/// <see cref="InMemoryOutboxStorage"/>. There is no transactional buffering —
/// the message is persisted immediately, which matches the "test seam" mental
/// model where the caller controls when the dispatcher runs via
/// <see cref="DeterministicDispatcher.DispatchOnceAsync"/>.
/// </summary>
public sealed class InMemoryOutbox : IOutbox
{
    private readonly InMemoryOutboxStorage storage;
    private readonly JsonSerializerOptions jsonOptions;

    /// <summary>Construct an in-memory outbox over the supplied storage.</summary>
    /// <param name="storage">The in-memory storage to write to; must be non-null.</param>
    /// <param name="jsonOptions">
    /// JSON options used to serialize payloads and headers. Defaults to a fresh
    /// <see cref="JsonSerializerOptions"/> seeded from <see cref="JsonSerializerDefaults.Web"/>.
    /// </param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="storage"/> is null.</exception>
    public InMemoryOutbox(InMemoryOutboxStorage storage, JsonSerializerOptions? jsonOptions = null)
    {
        ArgumentNullException.ThrowIfNull(storage);
        this.storage = storage;
        this.jsonOptions = jsonOptions ?? new JsonSerializerOptions(JsonSerializerDefaults.Web);
    }

    /// <inheritdoc/>
    public void Enqueue<T>(T message, OutboxEnqueueOptions? options = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(message);

        var occurred = options?.OccurredAtUtc ?? DateTime.UtcNow;
        var row = new OutboxRow
        {
            Id = Guid.NewGuid(),
            MessageType = options?.MessageType ?? typeof(T).FullName ?? typeof(T).Name,
            Payload = JsonSerializer.Serialize(message, message.GetType(), jsonOptions),
            HeadersJson = options?.Headers is null
                ? null
                : JsonSerializer.Serialize(options.Headers, jsonOptions),
            CorrelationId = options?.CorrelationId,
            OccurredAtUtc = occurred,
            EnqueuedAtUtc = occurred,
            Status = OutboxStatus.Pending,
            AttemptCount = 0,
            NextAttemptAtUtc = occurred,
        };

        // Fire-and-forget on the synchronous storage path — InMemoryOutboxStorage.AppendAsync
        // is implemented synchronously and returns a completed task, so this never blocks.
        storage.AppendAsync(new[] { row }).GetAwaiter().GetResult();
    }
}
