using Azure.Messaging.ServiceBus;

namespace Moongazing.OrionPatch.AzureServiceBus;

/// <summary>
/// Thin abstraction over the subset of <see cref="ServiceBusClient"/> the sink needs.
/// Production wires <see cref="DefaultServiceBusSenderFactory"/> over the official Azure
/// SDK; unit tests substitute mocks so the sink can be exercised without a real namespace.
/// </summary>
public interface IServiceBusSenderFactory
{
    /// <summary>Open (or return the cached) sender for <paramref name="entityPath"/>.</summary>
    ServiceBusSender CreateSender(string entityPath);
}
