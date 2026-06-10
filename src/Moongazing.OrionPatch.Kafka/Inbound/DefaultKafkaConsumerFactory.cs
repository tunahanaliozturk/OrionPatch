using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace Moongazing.OrionPatch.Kafka.Inbound;

/// <summary>
/// Default <see cref="IKafkaConsumerFactory"/> over Confluent.Kafka's
/// <see cref="ConsumerBuilder{TKey, TValue}"/>. Disables EnableAutoCommit so the
/// inbound service can drive manual commits gated on handler success.
/// </summary>
public sealed class DefaultKafkaConsumerFactory : IKafkaConsumerFactory
{
    private readonly KafkaInboxOptions options;

    public DefaultKafkaConsumerFactory(IOptions<KafkaInboxOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        this.options = options.Value;
        if (string.IsNullOrWhiteSpace(this.options.BootstrapServers))
        {
            throw new InvalidOperationException(
                "DefaultKafkaConsumerFactory: KafkaInboxOptions.BootstrapServers is empty.");
        }
        if (string.IsNullOrWhiteSpace(this.options.GroupId))
        {
            throw new InvalidOperationException(
                "DefaultKafkaConsumerFactory: KafkaInboxOptions.GroupId is empty.");
        }
    }

    /// <inheritdoc />
    public IConsumer<string, byte[]> CreateConsumer()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = options.GroupId,
            AutoOffsetReset = options.AutoOffsetReset,
            // Manual commits gated on handler success - autocommit would advance the
            // offset before the handler ran and a crash would silently drop messages.
            EnableAutoCommit = false,
        };
        return new ConsumerBuilder<string, byte[]>(config).Build();
    }
}
