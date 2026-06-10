using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Moongazing.OrionPatch.Abstractions;

namespace Moongazing.OrionPatch.AzureServiceBus;

/// <summary>DI helpers for the Azure Service Bus publisher sink.</summary>
public static class AzureServiceBusOutboxSinkServiceCollectionExtensions
{
    /// <summary>
    /// Register <see cref="AzureServiceBusOutboxSink"/> as the singleton
    /// <see cref="IOutboxSink"/>. When
    /// <see cref="AzureServiceBusOutboxSinkOptions.ConnectionString"/> is set, the helper
    /// also registers a singleton <see cref="ServiceBusClient"/> built from that connection
    /// string; otherwise the caller must register a <see cref="ServiceBusClient"/>
    /// themselves (e.g., via Azure Identity / managed identity wiring).
    /// </summary>
    public static IServiceCollection AddOrionPatchAzureServiceBusSink(
        this IServiceCollection services,
        Action<AzureServiceBusOutboxSinkOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.Configure(configure);

        var probe = new AzureServiceBusOutboxSinkOptions();
        configure(probe);
        if (!string.IsNullOrWhiteSpace(probe.ConnectionString))
        {
            var connectionString = probe.ConnectionString;
            services.TryAddSingleton(_ => new ServiceBusClient(connectionString));
        }

        services.TryAddSingleton<IServiceBusSenderFactory>(
            sp => new DefaultServiceBusSenderFactory(sp.GetRequiredService<ServiceBusClient>()));
        services.AddSingleton<IOutboxSink, AzureServiceBusOutboxSink>();

        return services;
    }
}
