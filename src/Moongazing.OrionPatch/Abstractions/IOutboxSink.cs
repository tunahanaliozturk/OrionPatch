using System.Threading;
using System.Threading.Tasks;
using Moongazing.OrionPatch.Models;

namespace Moongazing.OrionPatch.Abstractions;

/// <summary>
/// Pluggable destination for dispatched outbox envelopes. Exactly one sink is
/// registered per service; broker-specific implementations live in opt-in
/// sub-packages (e.g. OrionPatch.RabbitMQ, OrionPatch.AzureServiceBus).
/// </summary>
public interface IOutboxSink
{
    /// <summary>
    /// Send an envelope to the external destination. Must be idempotent at the
    /// envelope level — under at-least-once semantics, the dispatcher may re-deliver
    /// after a sink/process failure that occurred between "send succeeded" and
    /// "row marked Processed". Best practice: keep the external publish the last
    /// statement of this method.
    /// </summary>
    /// <param name="envelope">The envelope to dispatch.</param>
    /// <param name="ct">Cancellation token observed for the duration of the send.</param>
    Task SendAsync(OutboxEnvelope envelope, CancellationToken ct);
}
