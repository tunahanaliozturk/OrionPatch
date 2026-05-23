using Moongazing.OrionPatch.Models;

namespace Moongazing.OrionPatch.Abstractions;

/// <summary>
/// Scope-bound enqueue API. The implementation buffers messages and flushes them
/// into the consumer's transaction during EF Core SaveChanges.
/// </summary>
public interface IOutbox
{
    /// <summary>
    /// Queue a message for at-least-once dispatch. The row is persisted atomically
    /// with the consumer's other entity changes when <c>SaveChanges</c> commits.
    /// </summary>
    /// <remarks>
    /// The message is buffered in-memory and persisted only when the bound DbContext's
    /// <c>SaveChangesAsync</c> succeeds. If the unit of work rolls back, the message is
    /// silently discarded along with the rest of the transaction — which is the intended
    /// atomic behaviour. If <c>SaveChangesAsync</c> is never called, the message is
    /// never persisted and therefore never dispatched.
    /// </remarks>
    /// <typeparam name="T">Message type; serialized to JSON.</typeparam>
    /// <param name="message">The message instance.</param>
    /// <param name="options">Optional per-enqueue overrides.</param>
    void Enqueue<T>(T message, OutboxEnqueueOptions? options = null) where T : class;
}
