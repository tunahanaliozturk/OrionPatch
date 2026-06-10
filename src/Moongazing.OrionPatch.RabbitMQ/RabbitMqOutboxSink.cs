using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Models;
using RabbitMQ.Client;

namespace Moongazing.OrionPatch.RabbitMQ;

/// <summary>
/// RabbitMQ-backed <see cref="IOutboxSink"/>. Publishes each envelope to the configured
/// exchange with the routing key selected by
/// <see cref="RabbitMqOutboxSinkOptions.RoutingKeySelector"/>. Publisher confirms are
/// enabled by default so a broker that does not ack within the configured timeout makes
/// the sink throw, which keeps the outbox row unprocessed for re-delivery.
/// </summary>
/// <remarks>
/// <para>
/// One <see cref="IModel"/> per sink instance; the channel is opened lazily on the first
/// <see cref="SendAsync"/> and reused for the lifetime of the sink. Disposing the sink
/// closes the channel; the underlying <see cref="IConnection"/> is owned by DI (the sink
/// does NOT dispose it) so multiple sinks can share one connection.
/// </para>
/// <para>
/// Headers stamped on every outgoing message:
/// <list type="bullet">
///   <item><description><c>orionpatch-envelope-id</c> - the envelope's stable id (Guid) for deduplication at the consumer.</description></item>
///   <item><description><c>orionpatch-message-type</c> - the envelope's logical message type.</description></item>
///   <item><description><c>orionpatch-correlation-id</c> - the envelope's correlation id, when present.</description></item>
///   <item><description>Caller-supplied envelope <see cref="OutboxEnvelope.Headers"/> entries (e.g. W3C <c>traceparent</c> / <c>tracestate</c>, tenant id) flow through verbatim. Reserved <c>orionpatch-*</c> keys win over consumer overrides.</description></item>
/// </list>
/// </para>
/// </remarks>
public sealed partial class RabbitMqOutboxSink : IOutboxSink, IDisposable
{
    [LoggerMessage(EventId = 1, Level = LogLevel.Debug,
        Message = "Published envelope {EnvelopeId} of type {MessageType} to exchange '{Exchange}' with routing key '{RoutingKey}'")]
    private partial void LogPublished(Guid envelopeId, string messageType, string exchange, string routingKey);

    [LoggerMessage(EventId = 2, Level = LogLevel.Warning,
        Message = "Failed to dispose RabbitMQ channel during sink Dispose.")]
    private partial void LogChannelDisposeFailed(Exception ex);

    private readonly IConnection connection;
    private readonly RabbitMqOutboxSinkOptions options;
    private readonly ILogger<RabbitMqOutboxSink> logger;
    private readonly object channelLock = new();
    private IModel? channel;
    private bool disposed;

    /// <summary>Construct with an already-resolved <see cref="IConnection"/>.</summary>
    public RabbitMqOutboxSink(
        IConnection connection,
        IOptions<RabbitMqOutboxSinkOptions> options,
        ILogger<RabbitMqOutboxSink>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(options);
        this.connection = connection;
        this.options = options.Value;
        this.logger = logger ?? NullLogger<RabbitMqOutboxSink>.Instance;
    }

    /// <inheritdoc />
    public Task SendAsync(OutboxEnvelope envelope, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        ObjectDisposedException.ThrowIf(disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        // RabbitMQ.Client 6.x is synchronous; wrap so the IOutboxSink contract is honoured.
        return Task.Run(() => PublishCore(envelope), cancellationToken);
    }

    private void PublishCore(OutboxEnvelope envelope)
    {
        var ch = GetOrOpenChannel();
        var routingKey = options.RoutingKeySelector(envelope);

        var props = ch.CreateBasicProperties();
        props.ContentType = options.ContentType;
        props.MessageId = envelope.Id.ToString("N");
        props.Type = envelope.MessageType;
        if (options.PersistentDelivery)
        {
            props.DeliveryMode = 2; // persistent
        }

        var headers = new Dictionary<string, object>(StringComparer.Ordinal)
        {
            ["orionpatch-envelope-id"] = envelope.Id.ToString("N"),
            ["orionpatch-message-type"] = envelope.MessageType,
        };
        if (!string.IsNullOrEmpty(envelope.CorrelationId))
        {
            headers["orionpatch-correlation-id"] = envelope.CorrelationId;
            props.CorrelationId = envelope.CorrelationId;
        }
        // Caller-supplied headers (W3C traceparent/tracestate, tenant id, etc.) flow through
        // as AMQP headers verbatim. Reserved orionpatch-* keys win over consumer overrides.
        if (envelope.Headers is { Count: > 0 })
        {
            foreach (var (k, v) in envelope.Headers)
            {
                if (k.StartsWith("orionpatch-", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }
                headers[k] = v;
            }
        }
        props.Headers = headers;

        var body = Encoding.UTF8.GetBytes(envelope.Payload);

        ch.BasicPublish(
            exchange: options.ExchangeName,
            routingKey: routingKey,
            mandatory: false,
            basicProperties: props,
            body: body);

        if (options.UsePublisherConfirms)
        {
            if (!ch.WaitForConfirms(options.ConfirmTimeout))
            {
                throw new InvalidOperationException(
                    $"RabbitMQ broker did not acknowledge envelope {envelope.Id:N} within {options.ConfirmTimeout}. " +
                    "The outbox row remains unprocessed and will be re-delivered on the next dispatch cycle.");
            }
        }

        LogPublished(envelope.Id, envelope.MessageType, options.ExchangeName, routingKey);
    }

    private IModel GetOrOpenChannel()
    {
        if (channel is { IsOpen: true })
        {
            return channel;
        }

        lock (channelLock)
        {
            if (channel is { IsOpen: true })
            {
                return channel;
            }

            channel?.Dispose();
            channel = connection.CreateModel();
            if (options.UsePublisherConfirms)
            {
                channel.ConfirmSelect();
            }
            return channel;
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (disposed)
        {
            return;
        }
        disposed = true;
        lock (channelLock)
        {
            try { channel?.Dispose(); }
            catch (Exception ex) { LogChannelDisposeFailed(ex); }
            channel = null;
        }
    }
}
