using Confluent.Kafka;

namespace Moongazing.OrionPatch.Kafka.Inbound;

/// <summary>
/// Thin abstraction over <see cref="IConsumer{TKey, TValue}"/> construction so the
/// inbound service can be tested without a running Kafka cluster.
/// </summary>
public interface IKafkaConsumerFactory
{
    /// <summary>Build a consumer subscribed to the configured topics.</summary>
    IConsumer<string, byte[]> CreateConsumer();
}
