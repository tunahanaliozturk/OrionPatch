namespace Moongazing.OrionPatch.Abstractions;

using Moongazing.OrionPatch.Models;

/// <summary>
/// Convenience wrapper that runs a handler delegate only on first delivery, returning the
/// matching <see cref="OutboxEnvelope"/> outcome. Lets consumer broker pipelines plug
/// dedup in with a single decorator instead of branching on <see cref="IInbox.TryAcceptAsync"/>
/// at every handler.
/// </summary>
/// <example>
/// <code>
/// var filter = new InboxFilter(inbox);
/// await filter.InvokeIfFirstAsync(envelope,
///     (env, ct) => handler.HandleAsync(env.Payload, ct),
///     cancellationToken);
/// </code>
/// </example>
public sealed class InboxFilter
{
    private readonly IInbox inbox;

    /// <summary>Constructor. Stores the inbox to consult on every envelope.</summary>
    public InboxFilter(IInbox inbox)
    {
        ArgumentNullException.ThrowIfNull(inbox);
        this.inbox = inbox;
    }

    /// <summary>
    /// Invoke <paramref name="handler"/> only when this is the first observed delivery of
    /// <paramref name="envelope"/>. Duplicates are silently accepted; the broker should treat
    /// the dispatch as successful so the row is not retried.
    /// </summary>
    /// <returns><see langword="true"/> when the handler ran (first delivery),
    /// <see langword="false"/> on duplicate.</returns>
    public async ValueTask<bool> InvokeIfFirstAsync(
        OutboxEnvelope envelope,
        Func<OutboxEnvelope, CancellationToken, ValueTask> handler,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        ArgumentNullException.ThrowIfNull(handler);

        if (!await inbox.TryAcceptAsync(envelope.Id, cancellationToken).ConfigureAwait(false))
        {
            return false;
        }
        await handler(envelope, cancellationToken).ConfigureAwait(false);
        return true;
    }
}
