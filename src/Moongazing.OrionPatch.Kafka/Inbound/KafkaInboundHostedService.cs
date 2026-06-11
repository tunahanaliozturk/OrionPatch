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

    [LoggerMessage(EventId = 5, Level = LogLevel.Warning,
        Message = "Kafka inbound routed envelope {EnvelopeId} to dead-letter topic '{DlqTopic}' after {Attempts} failed attempts (original topic {OriginalTopic} partition {OriginalPartition} offset {OriginalOffset}); committing original")]
    private partial void LogDeadLettered(Guid envelopeId, string dlqTopic, int attempts, string originalTopic, int originalPartition, long originalOffset);

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
                    // Transient consume failures (rebalance, broker hiccup, auth). The
                    // next iteration retries after a backoff to avoid hot-looping under
                    // sustained failures. Nothing was committed so no data is lost.
                    try
                    {
                        await Task.Delay(options.ConsumeRetryBackoff, stoppingToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
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
            TryCommit(consumer, result);
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
            TryCommit(consumer, result);
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

        var attemptStore = scope.ServiceProvider.GetRequiredService<IKafkaAttemptCountStore>();

        try
        {
            await handler.HandleAsync(message, stoppingToken).ConfigureAwait(false);
            await attemptStore.ClearAsync(envelopeId, stoppingToken).ConfigureAwait(false);
            TryCommit(consumer, result);
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
            // Roll the inbox back FIRST. v0.2.10 reads/writes the attempt store before
            // routing to DLQ, but a transient failure in GetAsync / SetAsync would
            // otherwise prevent the rollback + seek from running - the envelope would
            // stay accepted in the inbox while the offset stays uncommitted, blocking
            // both this redelivery and any future one.
            await inbox.RollbackAsync(envelopeId, CancellationToken.None).ConfigureAwait(false);
            LogHandlerFailed(envelopeId, topic, partition, offset, ex);

            int attemptCount;
            try
            {
                var previousCount = await attemptStore.GetAsync(envelopeId, CancellationToken.None).ConfigureAwait(false);
                attemptCount = previousCount + 1;
                await attemptStore.SetAsync(envelopeId, attemptCount, CancellationToken.None).ConfigureAwait(false);
                KafkaInboundDiagnostics.RecordAttemptSet(topic);
            }
#pragma warning disable CA1031
            catch
#pragma warning restore CA1031
            {
                // Store hiccup: skip DLQ evaluation for this redelivery (we cannot tell
                // whether the cap has been reached without a reliable count) and fall
                // through to the seek+redeliver path so Kafka delivers again.
                TrySeek(consumer, result);
                return;
            }

            _ = ex; // exception is logged above; the variable is captured here only to satisfy the catch binding.
            // v0.2.10: attempt store is consulted before evaluating MaxDeliveryAttempts.
            // The default InMemoryKafkaAttemptCountStore preserves the v0.2.9 best-effort
            // semantics; consumers wiring a persistent store (EF Core, Redis) get
            // restart-survivable DLQ routing.
            if (!string.IsNullOrEmpty(options.DeadLetterTopic)
                && attemptCount >= options.MaxDeliveryAttempts)
            {
                if (await TryDeadLetterAsync(envelopeId, result, attemptCount, ex, stoppingToken).ConfigureAwait(false))
                {
                    await attemptStore.ClearAsync(envelopeId, CancellationToken.None).ConfigureAwait(false);
                    LogDeadLettered(envelopeId, options.DeadLetterTopic!, attemptCount, topic, partition, offset);
                    KafkaInboundDiagnostics.RecordDlqRouted(topic, options.DeadLetterTopic!);
                    TryCommit(consumer, result);
                    return;
                }
                // DLQ produce failed - fall through to the standard seek+redeliver path.
            }
            TrySeek(consumer, result);
        }
    }

    private async Task<bool> TryDeadLetterAsync(
        Guid envelopeId,
        ConsumeResult<string, byte[]> source,
        int attemptCount,
        Exception cause,
        CancellationToken stoppingToken)
    {
        try
        {
            using var scope = scopeFactory.CreateScope();
            var producer = scope.ServiceProvider.GetService<IKafkaInboundDeadLetterProducer>()
                          ?? new NoopDeadLetterProducer();
            var dlqRecord = new Message<string, byte[]>
            {
                Key = source.Message.Key,
                Value = source.Message.Value,
                Headers = CloneHeadersWithDlqMetadata(source, attemptCount, cause),
            };
            await producer.ProduceAsync(options.DeadLetterTopic!, dlqRecord, stoppingToken).ConfigureAwait(false);
            return producer is not NoopDeadLetterProducer;
        }
#pragma warning disable CA1031
        catch
#pragma warning restore CA1031
        {
            return false;
        }
    }

    private static Headers CloneHeadersWithDlqMetadata(
        ConsumeResult<string, byte[]> source, int attempts, Exception cause)
    {
        var headers = new Headers();
        if (source.Message.Headers is not null)
        {
            foreach (var h in source.Message.Headers)
            {
                headers.Add(h.Key, h.GetValueBytes());
            }
        }
        headers.Add("orionpatch-dlq-original-topic", Encoding.UTF8.GetBytes(source.Topic));
        headers.Add("orionpatch-dlq-original-partition", Encoding.UTF8.GetBytes(source.Partition.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)));
        headers.Add("orionpatch-dlq-original-offset", Encoding.UTF8.GetBytes(source.Offset.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)));
        headers.Add("orionpatch-dlq-attempt-count", Encoding.UTF8.GetBytes(attempts.ToString(System.Globalization.CultureInfo.InvariantCulture)));
        headers.Add("orionpatch-dlq-reason", Encoding.UTF8.GetBytes(cause.GetType().FullName ?? "Exception"));
        return headers;
    }

    private sealed class NoopDeadLetterProducer : IKafkaInboundDeadLetterProducer
    {
        public Task ProduceAsync(string topic, Message<string, byte[]> message, CancellationToken ct)
            => Task.CompletedTask;
    }

    private static void TryCommit(IConsumer<string, byte[]> consumer, ConsumeResult<string, byte[]> result)
    {
        try
        {
            consumer.Commit(result);
        }
#pragma warning disable CA1031 // a commit failure must not terminate the consume loop
        catch
        {
            // The offset stays uncommitted - Kafka will redeliver on the next consume,
            // and the inbox's TryAcceptAsync false-return short-circuits the handler.
        }
#pragma warning restore CA1031
    }

    private static void TrySeek(IConsumer<string, byte[]> consumer, ConsumeResult<string, byte[]> result)
    {
        try
        {
            consumer.Seek(new TopicPartitionOffset(result.Topic, result.Partition, result.Offset));
        }
#pragma warning disable CA1031
        catch
        {
            // Best-effort: if the seek fails (rebalance happened) the inbox rollback
            // still ensures redelivery is not suppressed as a duplicate. We log and
            // continue rather than terminating the loop.
        }
#pragma warning restore CA1031
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
