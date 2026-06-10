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

    [LoggerMessage(EventId = 3, Level = LogLevel.Warning,
        Message = "RabbitMQ returned envelope {EnvelopeId} as unroutable (exchange='{Exchange}', routingKey='{RoutingKey}', replyCode={ReplyCode}, replyText='{ReplyText}'). The outbox row will be re-delivered.")]
    private partial void LogUnroutable(string envelopeId, string exchange, string routingKey, int replyCode, string replyText);

    private readonly IConnection connection;
    private readonly RabbitMqOutboxSinkOptions options;
    private readonly ILogger<RabbitMqOutboxSink> logger;
    private readonly object channelLock = new();
    private readonly SemaphoreSlim publishGate = new(initialCount: 1, maxCount: 1);
    private IModel? channel;
    private string? lastUnroutableEnvelopeId;
    private string? lastUnroutableReplyText;
    private int lastUnroutableReplyCode;
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
    public async Task SendAsync(OutboxEnvelope envelope, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        ObjectDisposedException.ThrowIf(disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        // The RabbitMQ .NET client guide is explicit: concurrent BasicPublish on a shared
        // channel can interleave frames and tear the connection down. Serialise per-sink so
        // the dispatcher can call SendAsync from multiple workers without violating the
        // single-publisher invariant. The semaphore also bounds the WaitForConfirms window
        // so a slow broker does not stall a different in-flight envelope.
        await publishGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // RabbitMQ.Client 6.x is synchronous; wrap so the IOutboxSink contract is honoured.
            await Task.Run(() => PublishCore(envelope), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            publishGate.Release();
        }
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

        // Reset the per-publish unroutable bookkeeping so we only react to a return that
        // belongs to THIS message. The BasicReturn handler is registered once when the
        // channel is opened.
        lastUnroutableEnvelopeId = null;
        lastUnroutableReplyText = null;
        lastUnroutableReplyCode = 0;

        // mandatory:true makes the broker call BasicReturn when no queue is bound to the
        // (exchange, routingKey). Without this, a misconfigured topology silently drops the
        // message AND the publisher-confirm still acks (the broker confirms the EXCHANGE
        // received the message, not that any QUEUE did) - the outbox row would be marked
        // processed even though no consumer can ever see it. Returning the message gives us
        // a deterministic signal to throw so the dispatcher re-delivers on the next cycle.
        ch.BasicPublish(
            exchange: options.ExchangeName,
            routingKey: routingKey,
            mandatory: true,
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

        // BasicReturn for an unroutable message arrives BEFORE the publisher-confirm ack on
        // the same channel, so by the time WaitForConfirms returns we know whether the
        // message was returned. Throwing here keeps the outbox row unprocessed so the next
        // dispatch cycle re-delivers (or the operator fixes the topology).
        if (lastUnroutableEnvelopeId == envelope.Id.ToString("N"))
        {
            var replyCode = lastUnroutableReplyCode;
            var replyText = lastUnroutableReplyText;
            LogUnroutable(envelope.Id.ToString("N"), options.ExchangeName, routingKey, replyCode, replyText ?? string.Empty);
            throw new InvalidOperationException(
                $"RabbitMQ returned envelope {envelope.Id:N} as unroutable (replyCode={replyCode}, replyText='{replyText}'). " +
                "The exchange / routing key has no matching queue binding; the outbox row remains unprocessed.");
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
            // Single BasicReturn handler bound to the channel. The publish-side state set in
            // PublishCore (envelope id, reply code/text) is what makes the latest publish
            // attribute the return correctly; the publishGate ensures only one publish is in
            // flight at a time so there is no cross-message confusion.
            channel.BasicReturn += OnBasicReturn;
            return channel;
        }
    }

    private void OnBasicReturn(object? sender, global::RabbitMQ.Client.Events.BasicReturnEventArgs e)
    {
        // The broker echoes the message id we stamped at publish time. We use it to confirm
        // the return belongs to the in-flight envelope (defensive; the publishGate already
        // prevents interleaving).
        lastUnroutableEnvelopeId = e.BasicProperties?.MessageId;
        lastUnroutableReplyCode = e.ReplyCode;
        lastUnroutableReplyText = e.ReplyText;
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
            try
            {
                if (channel is not null)
                {
                    channel.BasicReturn -= OnBasicReturn;
                    channel.Dispose();
                }
            }
            catch (Exception ex) { LogChannelDisposeFailed(ex); }
            channel = null;
        }
        publishGate.Dispose();
    }
}
