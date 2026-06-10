using System.Collections.Concurrent;
using Azure.Messaging.ServiceBus;

namespace Moongazing.OrionPatch.AzureServiceBus;

/// <summary>
/// Default <see cref="IServiceBusSenderFactory"/> over <see cref="ServiceBusClient"/>.
/// Senders are cached per entity path inside this factory because
/// <see cref="ServiceBusClient.CreateSender(string)"/> allocates a fresh
/// <see cref="ServiceBusSender"/> on every call - a long-running dispatcher would otherwise
/// leak connections under load. Cached senders are disposed when the factory is disposed.
/// </summary>
public sealed class DefaultServiceBusSenderFactory : IServiceBusSenderFactory, IAsyncDisposable
{
    private readonly ServiceBusClient client;
    private readonly ConcurrentDictionary<string, ServiceBusSender> senders = new(StringComparer.Ordinal);

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
        return senders.GetOrAdd(entityPath, path => client.CreateSender(path));
    }

    /// <summary>
    /// Dispose all cached senders. Called by DI when the factory's lifetime ends; the
    /// underlying <see cref="ServiceBusClient"/> is owned by DI and NOT disposed here.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        foreach (var sender in senders.Values)
        {
            await sender.DisposeAsync().ConfigureAwait(false);
        }
        senders.Clear();
    }
}
