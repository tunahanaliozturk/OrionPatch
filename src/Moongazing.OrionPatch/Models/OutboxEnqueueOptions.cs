namespace Moongazing.OrionPatch.Models;

/// <summary>
/// Per-enqueue overrides for <see cref="Moongazing.OrionPatch.Abstractions.IOutbox.Enqueue{T}"/>.
/// </summary>
public sealed class OutboxEnqueueOptions
{
    /// <summary>Override the resolved <see cref="OutboxEnvelope.MessageType"/>; default is <c>typeof(T).FullName</c>.</summary>
    public string? MessageType { get; init; }

    /// <summary>Override the correlation id picked up from ambient context.</summary>
    public string? CorrelationId { get; init; }

    /// <summary>Optional headers serialized into the row's HeadersJson column.</summary>
    public IReadOnlyDictionary<string, string>? Headers { get; init; }

    /// <summary>
    /// Override the "when did this happen" timestamp; default is <see cref="DateTime.UtcNow"/> at enqueue time.
    /// </summary>
    /// <remarks>
    /// Treated as UTC by convention; the enqueue boundary (Task 5) will enforce <c>Kind == DateTimeKind.Utc</c>.
    /// </remarks>
    public DateTime? OccurredAtUtc { get; init; }
}
