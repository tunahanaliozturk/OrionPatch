using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Moongazing.OrionPatch.Kafka.Inbound;

/// <summary>DI helpers for the Kafka inbound consumer.</summary>
public static class KafkaInboundServiceCollectionExtensions
{
    /// <summary>
    /// Register the Kafka inbound hosted service. The handler is registered as scoped so
    /// the per-message scope can provide a fresh DbContext / Unit-of-Work to the handler.
    /// </summary>
    /// <typeparam name="THandler">The handler implementation.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Configuration callback.</param>
    public static IServiceCollection AddOrionPatchKafkaInbox<THandler>(
        this IServiceCollection services,
        Action<KafkaInboxOptions> configure)
        where THandler : class, IKafkaInboundHandler
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var probe = new KafkaInboxOptions();
        configure(probe);
        services.Configure<KafkaInboxOptions>(o =>
        {
            o.BootstrapServers = probe.BootstrapServers;
            o.GroupId = probe.GroupId;
            o.Topics = probe.Topics;
            o.AutoOffsetReset = probe.AutoOffsetReset;
            o.PollTimeout = probe.PollTimeout;
            // v0.2.9 fields - without these the DLQ feature is silently disabled when
            // the consumer wires it through AddOrionPatchKafkaInbox.
            o.ConsumeRetryBackoff = probe.ConsumeRetryBackoff;
            o.DeadLetterTopic = probe.DeadLetterTopic;
            o.MaxDeliveryAttempts = probe.MaxDeliveryAttempts;
        });

        services.TryAddSingleton<IKafkaConsumerFactory, DefaultKafkaConsumerFactory>();
        services.AddScoped<IKafkaInboundHandler, THandler>();
        services.AddHostedService<KafkaInboundHostedService>();
        return services;
    }
}
