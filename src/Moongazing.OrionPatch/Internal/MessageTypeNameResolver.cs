namespace Moongazing.OrionPatch.Internal;

using Moongazing.OrionPatch.Models;

/// <summary>
/// Resolves the <c>MessageType</c> string for an outbox row. Honors the optional
/// override on <see cref="OutboxEnqueueOptions"/>; otherwise defaults to the type's
/// <see cref="Type.FullName"/>, falling back to <c>Type.Name</c> for open
/// generics (which have a null <see cref="Type.FullName"/>).
/// </summary>
internal sealed class MessageTypeNameResolver
{
    /// <summary>Resolve the message-type name to stamp on the envelope.</summary>
    /// <param name="type">The runtime type of the message.</param>
    /// <param name="options">Optional per-enqueue overrides; may be <see langword="null"/>.</param>
    /// <returns>The override if set; otherwise <c>type.FullName</c>; otherwise <c>type.Name</c>.</returns>
    /// <remarks>
    /// Kept as an instance method (rather than static) so it can be registered in DI as a singleton and
    /// later evolve to carry caching or configuration state without a breaking API change.
    /// </remarks>
#pragma warning disable CA1822 // see remarks: instance-by-design for forward-compatible DI registration
    public string Resolve(Type type, OutboxEnqueueOptions? options) =>
        options?.MessageType ?? type.FullName ?? type.Name;
#pragma warning restore CA1822
}
