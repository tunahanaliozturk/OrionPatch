using System;
using System.Collections.Generic;

namespace Moongazing.OrionPatch.Models;

/// <summary>
/// The sink-facing view of an outbox message ready for dispatch.
/// Immutable; constructed by the dispatcher just before invoking the sink.
/// </summary>
/// <param name="Id">Unique id of the outbox row this envelope was materialized from.</param>
/// <param name="MessageType">Logical type name; defaults to the CLR FullName of T, or the override from <see cref="OutboxEnqueueOptions.MessageType"/>.</param>
/// <param name="Payload">JSON payload as serialized at enqueue time.</param>
/// <param name="Headers">Optional caller-supplied string headers (correlation, tenant, etc.).</param>
/// <param name="CorrelationId">Optional correlation id; picks up ambient correlation at enqueue time when not overridden.</param>
/// <param name="OccurredAtUtc">When the domain event happened (caller-supplied or enqueue time).</param>
/// <param name="AttemptNumber">1-based attempt counter; equals the row's AttemptCount + 1 at the time of this dispatch.</param>
public sealed record OutboxEnvelope(
    Guid Id,
    string MessageType,
    string Payload,
    IReadOnlyDictionary<string, string>? Headers,
    string? CorrelationId,
    DateTime OccurredAtUtc,
    int AttemptNumber);
