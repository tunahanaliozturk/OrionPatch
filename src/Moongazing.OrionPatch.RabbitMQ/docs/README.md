# OrionPatch.RabbitMQ

RabbitMQ sink for the OrionPatch outbox: each outbox row is published to an exchange with publisher confirms and persistent delivery. The package also has a consumer (`AddOrionPatchRabbitMqConsumer`) that dedups on the envelope id before calling your handler.

![OrionPatch outbox write and dispatch: the dispatcher claims due rows and calls IOutboxSink.SendAsync, which the RabbitMQ sink turns into a confirmed publish](https://raw.githubusercontent.com/tunahanaliozturk/OrionPatch/main/docs/diagrams/outbox-dispatch.png)

## Install

    dotnet add package OrionPatch.RabbitMQ

It plugs into `OrionPatch` as its `IOutboxSink`; you still need a storage backend such as `OrionPatch.EntityFrameworkCore`. Built on `RabbitMQ.Client` 6.x.

## Quick start

```csharp
using Moongazing.OrionPatch.DependencyInjection;
using Moongazing.OrionPatch.EntityFrameworkCore.DependencyInjection;
using Moongazing.OrionPatch.RabbitMQ;

services.AddOrionPatch()
    .UseEntityFrameworkCore<AppDbContext>();

services.AddOrionPatchRabbitMqSink(o =>
{
    o.ConnectionString = "amqp://guest:guest@localhost:5672";
    o.ExchangeName = "orders";
});
```

With `ConnectionString` set, the package registers a singleton `IConnection`. Leave it empty to register your own `IConnection`.

## Publisher options

| `RabbitMqOutboxSinkOptions` | Default |
|-----------------------------|---------|
| `ConnectionString` | `null` (bring your own `IConnection`) |
| `ExchangeName` | `"orionpatch"` |
| `RoutingKeySelector` | envelope `MessageType` |
| `UsePublisherConfirms` | `true` |
| `ConfirmTimeout` | 10 s |
| `PersistentDelivery` | `true` |
| `ContentType` | `"application/json"` |

Messages carry `MessageId` = envelope id, `Type` = message type, `CorrelationId` when set, and the headers `orionpatch-envelope-id`, `orionpatch-message-type`, `orionpatch-correlation-id` plus the envelope's own headers. Publishes are `mandatory` and serialised on one channel.

`SendAsync` throws, so the dispatcher retries with backoff, when the broker does not confirm within `ConfirmTimeout` or returns the message as unroutable (no queue bound to the exchange and routing key). The sink does not declare exchanges or queues; create the topology yourself.

## Consumer

```csharp
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Channels;
using Moongazing.OrionPatch.Models;
using Moongazing.OrionPatch.RabbitMQ;

public sealed class OrderConfirmedHandler : IOrionPatchMessageHandler
{
    public Task HandleAsync(OutboxEnvelope envelope, CancellationToken cancellationToken)
    {
        // envelope.Id, envelope.MessageType, envelope.Payload ...
        return Task.CompletedTask;
    }
}

services.AddSingleton<IInbox, InMemoryInbox>();   // or UseEntityFrameworkCoreInbox<T>()
services.AddOrionPatchRabbitMqConsumer<OrderConfirmedHandler>(o =>
{
    o.QueueName = "billing.orders";
    o.PrefetchCount = 8;
});
```

| `RabbitMqOutboxConsumerOptions` | Default |
|---------------------------------|---------|
| `QueueName` | `"orionpatch"` |
| `PrefetchCount` | 8 |
| `ConsumerTag` | `"orionpatch-consumer"` |
| `RequeueOnFailure` | `true` |
| `AckDuplicates` | `true` |

The consumer needs the same `IConnection` and an `IInbox` registration. A message whose envelope id was already accepted skips the handler and is acked (nacked without requeue when `AckDuplicates` is `false`); a message without an envelope id is nacked without requeue; a failing handler nacks it (requeued while `RequeueOnFailure` is `true`, otherwise dead-lettered by the queue's DLX if one is configured).

## Related packages

- `OrionPatch` - the outbox core this sink plugs into.
- `OrionPatch.EntityFrameworkCore` - storage backend and EF Core inbox.
- `OrionPatch.Kafka`, `OrionPatch.AzureServiceBus` - the other broker sinks.

## Links

- Documentation and full README: https://github.com/tunahanaliozturk/OrionPatch
- Changelog: https://github.com/tunahanaliozturk/OrionPatch/blob/main/CHANGELOG.md
- License: MIT
