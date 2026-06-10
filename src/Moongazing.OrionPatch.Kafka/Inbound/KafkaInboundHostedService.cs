using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moongazing.OrionPatch.Abstractions;

namespace Moongazing.OrionPatch.Kafka.Inbound;

/// <summary>
/// Hosted service that consumes Kafka records, dedups via <see cref="IInbox"/>, and
/// dispatches to the registered <see cref="IKafkaInboundHandler"/>. Commits the offset
/// only after the handler returns successfully so a crash mid-handler is replayed.
/// </summary>
public sealed partial class KafkaInboundHostedService : BackgroundService
{
    [LoggerMessage(EventId = 1, Level = LogLevel.Information,
        Message = "Kafka inbound subscribed to topics [{Topics}] under group '{GroupId}'")]
    private partial void LogSubscribed(string topics, string groupId);

    [LoggerMessage(EventId = 2, Level = LogLevel.Debug,
        Message = "Skipped duplicate envelope {EnvelopeId} (topic {Topic} partition {Partition} offset {Offset})")]
    private partial void LogDuplicate(Guid envelopeId, string topic, int partition, long offset);

    [LoggerMessage(EventId = 3, Level = LogLevel.Error,
        Message = "Kafka inbound handler failed for envelope {EnvelopeId} (topic {Topic} partition {Partition} offset {Offset}); offset NOT committed - Kafka will redeliver")]
    private partial void LogHandlerFailed(Guid envelopeId, string topic, int partition, long offset, Exception ex);

    [LoggerMessage(EventId = 4, Level = LogLevel.Warning,
        Message = "Kafka inbound received a record without an orionpatch-envelope-id header on topic {Topic} partition {Partition} offset {Offset}; dropping and committing")]
    private partial void LogMissingEnvelopeId(string topic, int partition, long offset);

    private readonly IKafkaConsumerFactory factory;
    private readonly KafkaInboxOptions options;
    private readonly IServiceScopeFactory scopeFactory;
    private readonly ILogger<KafkaInboundHostedService> logger;

    public KafkaInboundHostedService(
        IKafkaConsumerFactory factory,
        IOptions<KafkaInboxOptions> options,
        IServiceScopeFactory scopeFactory,
        ILogger<KafkaInboundHostedService>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(scopeFactory);
        this.factory = factory;
        this.options = options.Value;
        this.scopeFactory = scopeFactory;
        this.logger = logger ?? NullLogger<KafkaInboundHostedService>.Instance;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Confluent.Kafka's consumer.Consume is blocking; run the loop on a dedicated
        // background task so we do not stall the host on shutdown.
        return Task.Run(() => RunLoopAsync(stoppingToken), stoppingToken);
    }

    private async Task RunLoopAsync(CancellationToken stoppingToken)
    {
        using var consumer = factory.CreateConsumer();
        consumer.Subscribe(options.Topics);
        if (logger.IsEnabled(LogLevel.Information))
        {
            var topicsJoined = string.Join(", ", options.Topics);
            LogSubscribed(topicsJoined, options.GroupId);
        }

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, byte[]>? result;
                try
                {
                    result = consumer.Consume(options.PollTimeout);
                }
                catch (ConsumeException)
                {
                    // Transient consume failures (rebalance, etc.). The next iteration
                    // retries; nothing was committed so no data is lost.
                    continue;
                }
                if (result is null || result.IsPartitionEOF)
                {
                    continue;
                }
                await HandleAsync(consumer, result, stoppingToken).ConfigureAwait(false);
            }
        }
        finally
        {
            consumer.Close();
        }
    }

    private async Task HandleAsync(
        IConsumer<string, byte[]> consumer,
        ConsumeResult<string, byte[]> result,
        CancellationToken stoppingToken)
    {
        var topic = result.Topic;
        var partition = result.Partition.Value;
        var offset = result.Offset.Value;
        var headers = ExtractHeaders(result.Message.Headers);

        if (!TryReadEnvelopeId(headers, out var envelopeId))
        {
            LogMissingEnvelopeId(topic, partition, offset);
            consumer.Commit(result);
            return;
        }

        await using var scope = scopeFactory.CreateAsyncScope();
        var inbox = scope.ServiceProvider.GetRequiredService<IInbox>();
        var handler = scope.ServiceProvider.GetRequiredService<IKafkaInboundHandler>();

        if (!await inbox.TryAcceptAsync(envelopeId, stoppingToken).ConfigureAwait(false))
        {
            // Duplicate delivery - inbox already accepted this id. Commit so we do not
            // re-process forever.
            LogDuplicate(envelopeId, topic, partition, offset);
            consumer.Commit(result);
            return;
        }

        var message = new InboundKafkaMessage(
            envelopeId,
            headers.GetValueOrDefault("orionpatch-message-type") ?? string.Empty,
            headers.GetValueOrDefault("orionpatch-correlation-id"),
            result.Message.Value,
            headers,
            topic,
            partition,
            offset);

        try
        {
            await handler.HandleAsync(message, stoppingToken).ConfigureAwait(false);
            consumer.Commit(result);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Shutting down. Roll back the inbox so the next consumer can pick up where
            // we left off; the offset has NOT been committed so Kafka redelivers.
            await inbox.RollbackAsync(envelopeId, CancellationToken.None).ConfigureAwait(false);
            throw;
        }
#pragma warning disable CA1031
        catch (Exception ex)
#pragma warning restore CA1031
        {
            // Handler failure: roll the inbox back so the redelivery is not silently
            // suppressed as a duplicate, then deliberately do NOT commit so Kafka
            // redelivers the message on the next consume.
            await inbox.RollbackAsync(envelopeId, CancellationToken.None).ConfigureAwait(false);
            LogHandlerFailed(envelopeId, topic, partition, offset, ex);
        }
    }

    private static Dictionary<string, string> ExtractHeaders(Headers headers)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        if (headers is null)
        {
            return result;
        }
        foreach (var header in headers)
        {
            var raw = header.GetValueBytes();
            if (raw is null)
            {
                continue;
            }
            result[header.Key] = Encoding.UTF8.GetString(raw);
        }
        return result;
    }

    private static bool TryReadEnvelopeId(Dictionary<string, string> headers, out Guid envelopeId)
    {
        envelopeId = Guid.Empty;
        return headers.TryGetValue("orionpatch-envelope-id", out var raw)
               && Guid.TryParseExact(raw, "N", out envelopeId);
    }
}
