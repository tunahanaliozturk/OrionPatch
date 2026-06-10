using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Models;

namespace Moongazing.OrionPatch.Kafka;

/// <summary>
/// Apache Kafka-backed <see cref="IOutboxSink"/>. Produces each envelope to the configured
/// topic with the envelope id as the message key (partition affinity for ordering) and
/// Kafka headers carrying the OrionPatch metadata. Idempotent producer mode is enabled by
/// default so broker-side retries do not duplicate messages.
/// </summary>
/// <remarks>
/// <para>
/// Stamped on every outgoing <see cref="Message{TKey, TValue}"/>:
/// <list type="bullet">
///   <item><description>Key = <see cref="KafkaOutboxSinkOptions.KeySelector"/> result (default = envelope id Guid N).</description></item>
///   <item><description>Value = UTF-8 bytes of <see cref="OutboxEnvelope.Payload"/>.</description></item>
///   <item><description>Header <c>orionpatch-envelope-id</c> (UTF-8 Guid N).</description></item>
///   <item><description>Header <c>orionpatch-message-type</c> (UTF-8).</description></item>
///   <item><description>Header <c>orionpatch-correlation-id</c> when present.</description></item>
///   <item><description>Caller-supplied envelope <see cref="OutboxEnvelope.Headers"/> entries (W3C traceparent / tracestate, tenant id) flow through verbatim. Reserved <c>orionpatch-*</c> keys win over consumer overrides.</description></item>
/// </list>
/// </para>
/// </remarks>
public sealed partial class KafkaOutboxSink : IOutboxSink
{
    [LoggerMessage(EventId = 1, Level = LogLevel.Debug,
        Message = "Produced envelope {EnvelopeId} of type {MessageType} to Kafka topic '{Topic}' partition {Partition} offset {Offset}")]
    private partial void LogProduced(Guid envelopeId, string messageType, string topic, int partition, long offset);

    private readonly IKafkaProducerFactory factory;
    private readonly KafkaOutboxSinkOptions options;
    private readonly ILogger<KafkaOutboxSink> logger;

    /// <summary>Construct with the configured producer factory.</summary>
    public KafkaOutboxSink(
        IKafkaProducerFactory factory,
        IOptions<KafkaOutboxSinkOptions> options,
        ILogger<KafkaOutboxSink>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentNullException.ThrowIfNull(options);
        this.factory = factory;
        this.options = options.Value;
        this.logger = logger ?? NullLogger<KafkaOutboxSink>.Instance;
    }

    /// <inheritdoc />
    public async Task SendAsync(OutboxEnvelope envelope, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        var producer = factory.GetProducer();
        var topic = options.TopicSelector?.Invoke(envelope) ?? options.Topic;
        var key = options.KeySelector(envelope);

        var headers = new Headers
        {
            { "orionpatch-envelope-id", Encoding.UTF8.GetBytes(envelope.Id.ToString("N")) },
            { "orionpatch-message-type", Encoding.UTF8.GetBytes(envelope.MessageType) },
        };
        if (!string.IsNullOrEmpty(envelope.CorrelationId))
        {
            headers.Add("orionpatch-correlation-id", Encoding.UTF8.GetBytes(envelope.CorrelationId));
        }
        if (envelope.Headers is { Count: > 0 })
        {
            foreach (var (k, v) in envelope.Headers)
            {
                if (k.StartsWith("orionpatch-", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }
                headers.Add(k, Encoding.UTF8.GetBytes(v));
            }
        }

        var message = new Message<string, byte[]>
        {
            Key = key,
            Value = Encoding.UTF8.GetBytes(envelope.Payload),
            Headers = headers,
        };

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(options.SendTimeout);
        var result = await producer.ProduceAsync(topic, message, cts.Token).ConfigureAwait(false);
        LogProduced(envelope.Id, envelope.MessageType, topic, result.Partition.Value, result.Offset.Value);
    }
}
