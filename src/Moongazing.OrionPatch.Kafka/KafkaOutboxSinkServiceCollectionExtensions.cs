using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Moongazing.OrionPatch.Abstractions;

namespace Moongazing.OrionPatch.Kafka;

/// <summary>DI helpers for the Kafka publisher sink.</summary>
public static class KafkaOutboxSinkServiceCollectionExtensions
{
    /// <summary>
    /// Register <see cref="KafkaOutboxSink"/> as the singleton <see cref="IOutboxSink"/>.
    /// The helper wires <see cref="DefaultKafkaProducerFactory"/> by default; consumers
    /// supplying their own <see cref="IKafkaProducerFactory"/> can register it before
    /// this call and the <c>TryAddSingleton</c> here will yield.
    /// </summary>
    public static IServiceCollection AddOrionPatchKafkaSink(
        this IServiceCollection services,
        Action<KafkaOutboxSinkOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        // Single delegate invocation: probe once, transcribe onto the registered options
        // so a delegate with side effects does not run twice (matches the v0.2.6
        // AzureServiceBus pattern).
        var probe = new KafkaOutboxSinkOptions();
        configure(probe);
        services.Configure<KafkaOutboxSinkOptions>(o =>
        {
            o.BootstrapServers = probe.BootstrapServers;
            o.Topic = probe.Topic;
            o.TopicSelector = probe.TopicSelector;
            o.KeySelector = probe.KeySelector;
            o.EnableIdempotence = probe.EnableIdempotence;
            o.Acks = probe.Acks;
            o.SendTimeout = probe.SendTimeout;
        });

        services.TryAddSingleton<IKafkaProducerFactory, DefaultKafkaProducerFactory>();
        services.AddSingleton<IOutboxSink, KafkaOutboxSink>();
        return services;
    }
}
