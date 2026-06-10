using System.Text;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Models;

namespace Moongazing.OrionPatch.AzureServiceBus;

/// <summary>
/// Azure Service Bus-backed <see cref="IOutboxSink"/>. Publishes each envelope to the
/// configured entity path (queue or topic). The Service Bus SDK already handles
/// publisher acknowledgement; the sink waits for the SDK's SendMessageAsync to complete
/// before returning so a transient failure surfaces as a re-deliverable outbox row.
/// </summary>
/// <remarks>
/// <para>
/// Stamped on every outgoing <see cref="ServiceBusMessage"/>:
/// <list type="bullet">
///   <item><description><see cref="ServiceBusMessage.MessageId"/> = envelope id (Guid N format) so Service Bus' built-in duplicate detection (if enabled on the entity) absorbs broker-side retries.</description></item>
///   <item><description><see cref="ServiceBusMessage.Subject"/> = subject selected via <see cref="AzureServiceBusOutboxSinkOptions.SubjectSelector"/>; defaults to <see cref="OutboxEnvelope.MessageType"/>.</description></item>
///   <item><description><see cref="ServiceBusMessage.CorrelationId"/> = the envelope's correlation id, when present.</description></item>
///   <item><description><see cref="ServiceBusMessage.ContentType"/> = the configured content type (default <c>application/json</c>).</description></item>
///   <item><description><c>ApplicationProperties["orionpatch-envelope-id"]</c> + <c>orionpatch-message-type</c> + caller-supplied envelope <see cref="OutboxEnvelope.Headers"/> (W3C traceparent / tracestate, tenant id) flow through verbatim. Reserved <c>orionpatch-*</c> keys win over consumer overrides.</description></item>
/// </list>
/// </para>
/// </remarks>
public sealed partial class AzureServiceBusOutboxSink : IOutboxSink
{
    [LoggerMessage(EventId = 1, Level = LogLevel.Debug,
        Message = "Published envelope {EnvelopeId} of type {MessageType} to Service Bus entity '{Entity}' with subject '{Subject}'")]
    private partial void LogPublished(Guid envelopeId, string messageType, string entity, string subject);

    private readonly IServiceBusSenderFactory factory;
    private readonly AzureServiceBusOutboxSinkOptions options;
    private readonly ILogger<AzureServiceBusOutboxSink> logger;

    /// <summary>Construct with the configured sender factory.</summary>
    public AzureServiceBusOutboxSink(
        IServiceBusSenderFactory factory,
        IOptions<AzureServiceBusOutboxSinkOptions> options,
        ILogger<AzureServiceBusOutboxSink>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentNullException.ThrowIfNull(options);
        this.factory = factory;
        this.options = options.Value;
        this.logger = logger ?? NullLogger<AzureServiceBusOutboxSink>.Instance;
    }

    /// <inheritdoc />
    public async Task SendAsync(OutboxEnvelope envelope, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        var sender = factory.CreateSender(options.EntityPath);
        var subject = options.SubjectSelector(envelope);
        var message = new ServiceBusMessage(Encoding.UTF8.GetBytes(envelope.Payload))
        {
            MessageId = envelope.Id.ToString("N"),
            Subject = subject,
            ContentType = options.ContentType,
        };
        if (!string.IsNullOrEmpty(envelope.CorrelationId))
        {
            message.CorrelationId = envelope.CorrelationId;
            message.ApplicationProperties["orionpatch-correlation-id"] = envelope.CorrelationId;
        }
        message.ApplicationProperties["orionpatch-envelope-id"] = envelope.Id.ToString("N");
        message.ApplicationProperties["orionpatch-message-type"] = envelope.MessageType;

        // Caller-supplied envelope headers (W3C traceparent/tracestate, tenant id, etc.)
        // flow through as ApplicationProperties verbatim. Reserved orionpatch-* keys win
        // over consumer overrides so a malicious caller cannot hijack the envelope id.
        if (envelope.Headers is { Count: > 0 })
        {
            foreach (var (k, v) in envelope.Headers)
            {
                if (k.StartsWith("orionpatch-", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }
                message.ApplicationProperties[k] = v;
            }
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(options.SendTimeout);
        await sender.SendMessageAsync(message, cts.Token).ConfigureAwait(false);
        LogPublished(envelope.Id, envelope.MessageType, options.EntityPath, subject);
    }
}
