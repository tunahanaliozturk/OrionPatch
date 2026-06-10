using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Models;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Moongazing.OrionPatch.RabbitMQ;

/// <summary>
/// Hosted RabbitMQ consumer that drains the configured queue, decodes each delivery into
/// an <see cref="OutboxEnvelope"/>, deduplicates via the registered
/// <see cref="IInbox"/>, and invokes <see cref="IOrionPatchMessageHandler"/> for first
/// deliveries. ACK on success / first-delivery duplicate; NACK on handler exception.
/// </summary>
/// <remarks>
/// <para>
/// Per-delivery scope: each delivery resolves <see cref="IOrionPatchMessageHandler"/> from
/// a fresh <see cref="IServiceScope"/> so scoped collaborators (DbContext, repositories)
/// behave as if served by an HTTP request. The scope is disposed before ACK so a failed
/// commit during the handler's SaveChanges propagates as a NACK.
/// </para>
/// <para>
/// AMQP threading: the AsyncEventingBasicConsumer raises Received on the connection's
/// dispatch loop. The handler invocation is offloaded inside Received via
/// <see cref="Task.Run(Func{Task})"/>-style queuing through the registered consumer so the
/// dispatch loop stays unblocked and prefetch keeps flowing.
/// </para>
/// </remarks>
public sealed partial class RabbitMqOutboxConsumer : BackgroundService
{
    [LoggerMessage(EventId = 1, Level = LogLevel.Information,
        Message = "RabbitMQ consumer started on queue '{Queue}' with consumer tag '{ConsumerTag}'")]
    private partial void LogStarted(string queue, string consumerTag);

    [LoggerMessage(EventId = 2, Level = LogLevel.Debug,
        Message = "Acked envelope {EnvelopeId} (deliveryTag {DeliveryTag}, reason: {Reason})")]
    private partial void LogAcked(Guid envelopeId, ulong deliveryTag, string reason);

    [LoggerMessage(EventId = 3, Level = LogLevel.Warning,
        Message = "Nacked envelope {EnvelopeId} (deliveryTag {DeliveryTag}, requeue: {Requeue})")]
    private partial void LogNacked(Guid envelopeId, ulong deliveryTag, bool requeue);

    [LoggerMessage(EventId = 4, Level = LogLevel.Warning,
        Message = "Rejecting delivery {DeliveryTag} - missing or invalid orionpatch-envelope-id header")]
    private partial void LogMissingHeader(ulong deliveryTag);

    private readonly IConnection connection;
    private readonly IServiceProvider rootServices;
    private readonly RabbitMqOutboxConsumerOptions options;
    private readonly ILogger<RabbitMqOutboxConsumer> logger;
    private IModel? channel;

    /// <summary>Construct with an already-resolved <see cref="IConnection"/> and DI root.</summary>
    public RabbitMqOutboxConsumer(
        IConnection connection,
        IServiceProvider rootServices,
        IOptions<RabbitMqOutboxConsumerOptions> options,
        ILogger<RabbitMqOutboxConsumer>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(rootServices);
        ArgumentNullException.ThrowIfNull(options);
        this.connection = connection;
        this.rootServices = rootServices;
        this.options = options.Value;
        this.logger = logger ?? NullLogger<RabbitMqOutboxConsumer>.Instance;
    }

    /// <inheritdoc />
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        channel = connection.CreateModel();
        channel.BasicQos(prefetchSize: 0, prefetchCount: options.PrefetchCount, global: false);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.Received += OnReceivedAsync;

        channel.BasicConsume(
            queue: options.QueueName,
            autoAck: false,
            consumerTag: options.ConsumerTag,
            noLocal: false,
            exclusive: false,
            arguments: null,
            consumer: consumer);

        LogStarted(options.QueueName, options.ConsumerTag);
        return Task.CompletedTask;
    }

    private async Task OnReceivedAsync(object sender, BasicDeliverEventArgs delivery)
    {
        var deliveryTag = delivery.DeliveryTag;
        if (!TryExtractEnvelopeId(delivery, out var envelopeId))
        {
            LogMissingHeader(deliveryTag);
            // No envelope id means we cannot dedupe and cannot safely retry. Drop without
            // requeue so an operator can inspect the broker DLQ / log instead of looping.
            channel!.BasicNack(deliveryTag, multiple: false, requeue: false);
            return;
        }

        using var scope = rootServices.CreateScope();
        var sp = scope.ServiceProvider;
        var inbox = sp.GetRequiredService<IInbox>();
        var handler = sp.GetRequiredService<IOrionPatchMessageHandler>();
        var stopping = CancellationToken.None;

        try
        {
            var firstDelivery = await inbox.TryAcceptAsync(envelopeId, stopping).ConfigureAwait(false);
            if (!firstDelivery)
            {
                AckOrNackDuplicate(deliveryTag, envelopeId);
                return;
            }

            var envelope = BuildEnvelope(envelopeId, delivery);
            await handler.HandleAsync(envelope, stopping).ConfigureAwait(false);

            channel!.BasicAck(deliveryTag, multiple: false);
            LogAcked(envelopeId, deliveryTag, "handler-success");
        }
        catch (Exception)
        {
            channel!.BasicNack(deliveryTag, multiple: false, requeue: options.RequeueOnFailure);
            LogNacked(envelopeId, deliveryTag, options.RequeueOnFailure);
            // Re-throw is NOT useful here - we're inside the AsyncEventingBasicConsumer
            // dispatch loop; swallowing keeps the consumer alive. Operator visibility comes
            // through the logged Nacked event + the broker DLQ when configured.
        }
    }

    private void AckOrNackDuplicate(ulong deliveryTag, Guid envelopeId)
    {
        if (options.AckDuplicates)
        {
            channel!.BasicAck(deliveryTag, multiple: false);
            LogAcked(envelopeId, deliveryTag, "duplicate");
        }
        else
        {
            channel!.BasicNack(deliveryTag, multiple: false, requeue: false);
            LogNacked(envelopeId, deliveryTag, false);
        }
    }

    private static bool TryExtractEnvelopeId(BasicDeliverEventArgs delivery, out Guid envelopeId)
    {
        envelopeId = default;
        var headers = delivery.BasicProperties?.Headers;
        if (headers is null || !headers.TryGetValue("orionpatch-envelope-id", out var raw))
        {
            return false;
        }
        var asString = raw switch
        {
            byte[] bytes => Encoding.UTF8.GetString(bytes),
            string s => s,
            _ => raw?.ToString(),
        };
        return Guid.TryParse(asString, out envelopeId);
    }

    private static OutboxEnvelope BuildEnvelope(Guid envelopeId, BasicDeliverEventArgs delivery)
    {
        var messageType = delivery.BasicProperties?.Type
            ?? GetHeaderString(delivery, "orionpatch-message-type")
            ?? "unknown";
        var payload = Encoding.UTF8.GetString(delivery.Body.ToArray());
        var correlationId = delivery.BasicProperties?.CorrelationId
            ?? GetHeaderString(delivery, "orionpatch-correlation-id");
        var headers = CollectHeaders(delivery);
        return new OutboxEnvelope(
            Id: envelopeId,
            MessageType: messageType,
            Payload: payload,
            Headers: headers,
            CorrelationId: correlationId,
            OccurredAtUtc: DateTime.UtcNow,
            AttemptNumber: (int)delivery.DeliveryTag);
    }

    private static string? GetHeaderString(BasicDeliverEventArgs delivery, string key)
    {
        var headers = delivery.BasicProperties?.Headers;
        if (headers is null || !headers.TryGetValue(key, out var raw))
        {
            return null;
        }
        return raw switch
        {
            byte[] bytes => Encoding.UTF8.GetString(bytes),
            string s => s,
            _ => raw?.ToString(),
        };
    }

    private static Dictionary<string, string>? CollectHeaders(BasicDeliverEventArgs delivery)
    {
        var raw = delivery.BasicProperties?.Headers;
        if (raw is null || raw.Count == 0)
        {
            return null;
        }
        var result = new Dictionary<string, string>(raw.Count, StringComparer.Ordinal);
        foreach (var entry in raw)
        {
            if (entry.Key.StartsWith("orionpatch-", StringComparison.OrdinalIgnoreCase))
            {
                // These are reconstructed onto envelope fields above; leaving them in
                // Headers would double-count.
                continue;
            }
            result[entry.Key] = entry.Value switch
            {
                byte[] b => Encoding.UTF8.GetString(b),
                string s => s,
                _ => entry.Value?.ToString() ?? string.Empty,
            };
        }
        return result.Count == 0 ? null : result;
    }

    /// <inheritdoc />
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            channel?.Close();
            channel?.Dispose();
            channel = null;
        }
        catch
        {
            // Best-effort cleanup during shutdown.
        }
        await base.StopAsync(cancellationToken).ConfigureAwait(false);
    }
}
