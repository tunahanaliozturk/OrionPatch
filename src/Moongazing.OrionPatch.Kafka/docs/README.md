# OrionPatch.Kafka

Apache Kafka sink for the OrionPatch outbox: each outbox row is produced to a Kafka topic by an idempotent producer. The package also has an inbound consumer (`AddOrionPatchKafkaInbox`) that dedups on the envelope id and can route poison messages to a dead-letter topic.

![OrionPatch outbox write and dispatch: the dispatcher claims due rows and calls IOutboxSink.SendAsync, which the Kafka sink turns into a produce call](https://raw.githubusercontent.com/tunahanaliozturk/OrionPatch/main/docs/diagrams/outbox-dispatch.png)

## Install

    dotnet add package OrionPatch.Kafka

It plugs into `OrionPatch` as its `IOutboxSink`; you still need a storage backend such as `OrionPatch.EntityFrameworkCore`.

## Quick start

```csharp
using Moongazing.OrionPatch.DependencyInjection;
using Moongazing.OrionPatch.EntityFrameworkCore.DependencyInjection;
using Moongazing.OrionPatch.Kafka;

services.AddOrionPatch()
    .UseEntityFrameworkCore<AppDbContext>();

services.AddOrionPatchKafkaSink(o =>
{
    o.BootstrapServers = "localhost:9092";
    o.Topic = "orders";
});
```

## Producer options

| `KafkaOutboxSinkOptions` | Default |
|--------------------------|---------|
| `BootstrapServers` | empty (required) |
| `Topic` | `"orionpatch"` |
| `TopicSelector` | `null` (use `Topic`); set it to route per envelope |
| `KeySelector` | envelope id (`"N"` format) |
| `EnableIdempotence` | `true` |
| `Acks` | `Acks.All` |
| `SendTimeout` | 30 s |

Each record carries the headers `orionpatch-envelope-id`, `orionpatch-message-type`, `orionpatch-correlation-id` (when set) and the envelope's own headers; caller headers cannot override the reserved `orionpatch-*` keys. A failed or timed-out produce throws, so the dispatcher retries the row with backoff and dead-letters it after `MaxAttempts`.

`KafkaProducerHealthCheck` probes broker metadata: `services.AddHealthChecks().AddCheck<KafkaProducerHealthCheck>("kafka");` (timeout from `KafkaProducerHealthCheckOptions.Timeout`, default 3 s).

## Inbound consumer

```csharp
using Moongazing.OrionPatch.Channels;
using Moongazing.OrionPatch.Kafka.Inbound;

public sealed class OrderConfirmedHandler : IKafkaInboundHandler
{
    public Task HandleAsync(InboundKafkaMessage message, CancellationToken cancellationToken)
    {
        // message.EnvelopeId, message.MessageType, message.Payload, message.Headers ...
        return Task.CompletedTask;
    }
}

services.AddSingleton<IInbox, InMemoryInbox>();   // or UseEntityFrameworkCoreInbox<T>()
services.AddOrionPatchKafkaInbox<OrderConfirmedHandler>(o =>
{
    o.BootstrapServers = "localhost:9092";
    o.GroupId = "billing";
    o.Topics = ["orders"];
    o.DeadLetterTopic = "orders.dlq";
    o.MaxDeliveryAttempts = 5;
});
```

- An `IInbox` registration is required: a record whose envelope id was already accepted is committed without calling the handler. A record without an `orionpatch-envelope-id` header is logged, committed and dropped.
- The offset is committed only after the handler succeeds. A failing record is retried; with `DeadLetterTopic` set it is routed there after `MaxDeliveryAttempts`.
- Dead-letter routing needs an `IKafkaInboundDeadLetterProducer` registration; none is registered by default.
- Attempt counts live in `InMemoryKafkaAttemptCountStore` by default; `EfCoreKafkaAttemptCountStore<TDbContext>` (in `OrionPatch.EntityFrameworkCore`) persists them across restarts.
- Metrics go to the meter `Moongazing.OrionPatch.Kafka.Inbound`.

## Related packages

- `OrionPatch` - the outbox core this sink plugs into.
- `OrionPatch.EntityFrameworkCore` - storage backend, EF Core inbox and attempt-count store.
- `OrionPatch.RabbitMQ`, `OrionPatch.AzureServiceBus` - the other broker sinks.

## Links

- Documentation and full README: https://github.com/tunahanaliozturk/OrionPatch
- Changelog: https://github.com/tunahanaliozturk/OrionPatch/blob/main/CHANGELOG.md
- License: MIT
