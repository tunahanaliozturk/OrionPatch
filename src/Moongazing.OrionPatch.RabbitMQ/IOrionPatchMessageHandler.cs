namespace Moongazing.OrionPatch.RabbitMQ;

using Moongazing.OrionPatch.Models;

/// <summary>
/// Consumer-supplied delegate invoked for every first-delivery envelope drained from the
/// RabbitMQ queue. Implementations are resolved scoped per delivery so they can take EF
/// Core / scoped repositories as constructor parameters.
/// </summary>
public interface IOrionPatchMessageHandler
{
    /// <summary>
    /// Handle the envelope. Throwing surfaces as a NACK on the AMQP delivery; returning
    /// successfully ACKs the message. The envelope id is already deduplicated against the
    /// registered <see cref="Abstractions.IInbox"/>; this method is invoked at most once
    /// per id (per process) for the lifetime of the inbox storage.
    /// </summary>
    Task HandleAsync(OutboxEnvelope envelope, CancellationToken cancellationToken);
}
