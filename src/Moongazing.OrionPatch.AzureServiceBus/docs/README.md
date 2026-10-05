# OrionPatch.AzureServiceBus

Azure Service Bus sink for the OrionPatch outbox: each outbox row is sent to a queue or topic with `MessageId` set to the envelope id, so Service Bus duplicate detection and downstream consumers can dedup on it.

![OrionPatch outbox write and dispatch: the dispatcher claims due rows and calls IOutboxSink.SendAsync, which the Azure Service Bus sink turns into a send to a queue or topic](https://raw.githubusercontent.com/tunahanaliozturk/OrionPatch/main/docs/diagrams/outbox-dispatch.png)

## Install

    dotnet add package OrionPatch.AzureServiceBus

It plugs into `OrionPatch` as its `IOutboxSink`; you still need a storage backend such as `OrionPatch.EntityFrameworkCore`. Built on `Azure.Messaging.ServiceBus`.

## Quick start

```csharp
using Moongazing.OrionPatch.AzureServiceBus;
using Moongazing.OrionPatch.DependencyInjection;
using Moongazing.OrionPatch.EntityFrameworkCore.DependencyInjection;

services.AddOrionPatch()
    .UseEntityFrameworkCore<AppDbContext>();

services.AddOrionPatchAzureServiceBusSink(o =>
{
    o.ConnectionString = builder.Configuration["ServiceBus:ConnectionString"];
    o.EntityPath = "orders";   // queue or topic name
});
```

## Managed identity

Leave `ConnectionString` empty and register your own `ServiceBusClient`; the sink resolves it from DI:

```csharp
using Azure.Identity;
using Azure.Messaging.ServiceBus;

services.AddSingleton(new ServiceBusClient("my-namespace.servicebus.windows.net", new DefaultAzureCredential()));
services.AddOrionPatchAzureServiceBusSink(o => o.EntityPath = "orders");
```

## Options

| `AzureServiceBusOutboxSinkOptions` | Default |
|------------------------------------|---------|
| `ConnectionString` | `null` (bring your own `ServiceBusClient`) |
| `EntityPath` | `"orionpatch"` |
| `SubjectSelector` | envelope `MessageType` |
| `ContentType` | `"application/json"` |
| `SendTimeout` | 30 s |

Each message carries `MessageId` = envelope id, `Subject`, `CorrelationId` when set, and the application properties `orionpatch-envelope-id`, `orionpatch-message-type`, `orionpatch-correlation-id` plus the envelope's own headers (caller headers cannot override the reserved `orionpatch-*` keys). A failed or timed-out send throws, so the dispatcher retries the row with backoff and dead-letters it after `MaxAttempts`. The sink does not create queues or topics.

## Related packages

- `OrionPatch` - the outbox core this sink plugs into.
- `OrionPatch.EntityFrameworkCore` - storage backend.
- `OrionPatch.Kafka`, `OrionPatch.RabbitMQ` - the other broker sinks.

## Links

- Documentation and full README: https://github.com/tunahanaliozturk/OrionPatch
- Changelog: https://github.com/tunahanaliozturk/OrionPatch/blob/main/CHANGELOG.md
- License: MIT
