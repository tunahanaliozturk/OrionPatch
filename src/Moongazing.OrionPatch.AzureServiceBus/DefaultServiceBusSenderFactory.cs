using Azure.Messaging.ServiceBus;

namespace Moongazing.OrionPatch.AzureServiceBus;

/// <summary>
/// Default <see cref="IServiceBusSenderFactory"/> over <see cref="ServiceBusClient"/>.
/// Senders are cached per entity path because the SDK encourages reuse.
/// </summary>
public sealed class DefaultServiceBusSenderFactory : IServiceBusSenderFactory
{
    private readonly ServiceBusClient client;

    /// <summary>Construct over an already-resolved <see cref="ServiceBusClient"/>.</summary>
    public DefaultServiceBusSenderFactory(ServiceBusClient client)
    {
        ArgumentNullException.ThrowIfNull(client);
        this.client = client;
    }

    /// <inheritdoc />
    public ServiceBusSender CreateSender(string entityPath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(entityPath);
        // ServiceBusClient internally caches senders per entity, so calling CreateSender
        // repeatedly returns the same underlying connection.
        return client.CreateSender(entityPath);
    }
}
