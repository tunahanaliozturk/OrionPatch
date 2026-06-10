using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace Moongazing.OrionPatch.Kafka;

/// <summary>
/// Default <see cref="IKafkaProducerFactory"/> over the official Confluent.Kafka
/// <see cref="ProducerBuilder{TKey, TValue}"/>. The producer is built lazily on the first
/// call and reused for the lifetime of the factory; Kafka producers are designed to be
/// long-lived and shared across publish calls.
/// </summary>
public sealed class DefaultKafkaProducerFactory : IKafkaProducerFactory, IDisposable
{
    private readonly KafkaOutboxSinkOptions options;
    private readonly object gate = new();
    private IProducer<string, byte[]>? producer;
    private volatile bool disposed;

    /// <summary>Construct with the configured options snapshot.</summary>
    public DefaultKafkaProducerFactory(IOptions<KafkaOutboxSinkOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        this.options = options.Value;
        if (string.IsNullOrWhiteSpace(this.options.BootstrapServers))
        {
            // Fail-fast at registration: BootstrapServers has no usable default and Kafka
            // would surface the misconfiguration as an opaque "no brokers" error on first
            // produce. Catching it here points the stack trace at the consumer's
            // AddOrionPatchKafkaSink(...) call site.
            throw new InvalidOperationException(
                "DefaultKafkaProducerFactory: KafkaOutboxSinkOptions.BootstrapServers is empty. " +
                "Supply at least one bootstrap broker (e.g. 'kafka-1:9092') in AddOrionPatchKafkaSink.");
        }
    }

    /// <inheritdoc />
    public IProducer<string, byte[]> GetProducer()
    {
        // volatile read pairs with the volatile write in Dispose so a concurrent Dispose
        // on another thread is observed without a memory barrier here.
        ObjectDisposedException.ThrowIf(disposed, this);
        if (producer is not null)
        {
            return producer;
        }
        lock (gate)
        {
            ObjectDisposedException.ThrowIf(disposed, this);
            if (producer is not null)
            {
                return producer;
            }
            var config = new ProducerConfig
            {
                BootstrapServers = options.BootstrapServers,
                EnableIdempotence = options.EnableIdempotence,
                Acks = options.Acks,
            };
            producer = new ProducerBuilder<string, byte[]>(config).Build();
            return producer;
        }
    }

    /// <summary>Dispose the underlying producer; flushes any buffered messages.</summary>
    public void Dispose()
    {
        lock (gate)
        {
            if (disposed)
            {
                return;
            }
            disposed = true;
            if (producer is not null)
            {
                try
                {
                    producer.Flush(TimeSpan.FromSeconds(5));
                }
                catch
                {
                    // Flush may throw on a broker-side failure during shutdown. We still
                    // dispose so the producer's native resources are released - skipping
                    // dispose would leak the librdkafka handle.
                }
                finally
                {
                    producer.Dispose();
                    producer = null;
                }
            }
        }
    }
}
