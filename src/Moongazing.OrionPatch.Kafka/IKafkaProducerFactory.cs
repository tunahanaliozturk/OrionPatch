using Confluent.Kafka;

namespace Moongazing.OrionPatch.Kafka;

/// <summary>
/// Thin abstraction over <see cref="IProducer{TKey, TValue}"/> construction. Production
/// wires <see cref="DefaultKafkaProducerFactory"/> over the official Confluent.Kafka
/// builder; unit tests substitute mocks so the sink can be exercised without a running
/// Kafka cluster.
/// </summary>
public interface IKafkaProducerFactory
{
    /// <summary>Open (or return the cached) producer for the configured options.</summary>
    IProducer<string, byte[]> GetProducer();
}
