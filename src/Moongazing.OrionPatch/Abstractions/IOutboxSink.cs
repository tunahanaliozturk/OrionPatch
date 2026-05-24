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
    /// <remarks>
    /// Best practice: deduplicate at the destination on <see cref="Models.OutboxEnvelope.Id"/>,
    /// or perform an upsert. The dispatcher may re-invoke this method with the same envelope id
    /// after a failure between successful send and successful storage acknowledgement, or after
    /// a lease-expiry race when the sink runtime exceeds
    /// <see cref="Configuration.OrionPatchOptions.LeaseDuration"/>. Keep the external publish the
    /// last statement of the implementation so a failure after publish does not silently lose
    /// acknowledgement.
    /// </remarks>
    /// <param name="envelope">The envelope to dispatch.</param>
    /// <param name="cancellationToken">Cancellation token observed for the duration of the send.</param>
    Task SendAsync(OutboxEnvelope envelope, CancellationToken cancellationToken = default);
}
