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
    private bool disposed;

    /// <summary>Construct with the configured options snapshot.</summary>
    public DefaultKafkaProducerFactory(IOptions<KafkaOutboxSinkOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        this.options = options.Value;
    }

    /// <inheritdoc />
    public IProducer<string, byte[]> GetProducer()
    {
        ObjectDisposedException.ThrowIf(disposed, this);
        if (producer is not null)
        {
            return producer;
        }
        lock (gate)
        {
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
        if (disposed)
        {
            return;
        }
        disposed = true;
        lock (gate)
        {
            if (producer is not null)
            {
                producer.Flush(TimeSpan.FromSeconds(5));
                producer.Dispose();
                producer = null;
            }
        }
    }
}
