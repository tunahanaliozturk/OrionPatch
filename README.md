<p align="center">
  <img src="docs/logo.png" alt="OrionPatch" width="150" />
</p>

<h1 align="center">OrionPatch</h1>

<p align="center">
  Transactional outbox primitive for .NET. Enqueue inside SaveChanges, dispatch at-least-once through a pluggable sink.
</p>

<p align="center">
  <a href="https://www.nuget.org/packages/OrionPatch"><img src="https://img.shields.io/nuget/v/OrionPatch?style=flat-square&color=blue" alt="NuGet" /></a>
  <a href="https://www.nuget.org/packages/OrionPatch"><img src="https://img.shields.io/nuget/dt/OrionPatch?style=flat-square&color=green" alt="Downloads" /></a>
  <a href="LICENSE.txt"><img src="https://img.shields.io/badge/license-MIT-yellow?style=flat-square" alt="License" /></a>
  <img src="https://img.shields.io/badge/.NET-8.0%20%7C%209.0%20%7C%2010.0-purple?style=flat-square" alt="Target" />
</p>

---

## Why OrionPatch?

Every .NET service that mutates its database and then publishes a message has the same
problem: the database transaction can commit and the publish can fail, or the publish can
succeed and the transaction can roll back. The transactional outbox pattern is the standard
fix. OrionPatch packages it as a primitive — small, focused, broker-agnostic.

|                                           | OrionPatch | DIY interceptor | MassTransit | Wolverine |
|-------------------------------------------|:----------:|:---------------:|:-----------:|:---------:|
| Transactional outbox primitive (no framework lock-in) | yes | yes | partial | partial |
| Pluggable sink (Kafka, RabbitMQ, your own) | yes | yes | yes | yes |
| Ships with at-least-once dispatcher, retries, dead-letter | yes | no | yes | yes |
| Brings a mediator / saga / scheduler with it | no | no | yes | yes |
| Adoptable in one file, removable in one file | yes | yes | no | no |

OrionPatch sits *underneath* a messaging framework or *next to* a plain producer. It does
not replace MassTransit or Wolverine; it does what a DIY interceptor would do, but with
the dispatcher loop, dead-letter, telemetry, and competing-consumers semantics already
solved.

---

## Quick start (30 seconds)

```bash
dotnet add package OrionPatch
dotnet add package OrionPatch.EntityFrameworkCore
```

```csharp
services.AddDbContext<AppDbContext>(...);

services.AddOrionPatch()
        .UseEntityFrameworkCore<AppDbContext>()
        .UseSink<MyKafkaSink>();
```

```csharp
public class OrderService
{
    private readonly AppDbContext _db;
    private readonly IOutbox _outbox;

    public async Task ConfirmOrderAsync(Guid orderId, CancellationToken ct)
    {
        var order = await _db.Orders.FindAsync([orderId], ct);
        order.Confirm();

        _outbox.Enqueue(new OrderConfirmed(order.Id, order.TotalCents));

        await _db.SaveChangesAsync(ct);   // outbox row + order update commit together
    }
}
```

A background `OutboxDispatcherHostedService` polls the `OrionPatch_Outbox` table, claims a
batch with the storage's competing-consumers primitive (`SKIP LOCKED` on supported
providers, optimistic-concurrency fallback on SQLite), and hands each envelope to your
`IOutboxSink`. Failures retry with exponential backoff, then dead-letter.

Your sink:

```csharp
public sealed class MyKafkaSink : IOutboxSink
{
    public async Task SendAsync(OutboxEnvelope env, CancellationToken ct)
    {
        await _producer.ProduceAsync(env.MessageType, env.Payload, ct);
    }
}
```

For monoliths and tests, OrionPatch also ships `ChannelOutboxSink` — an in-process sink
backed by `System.Threading.Channels`, zero external dependency.

---

## What it does NOT do (v0.1.0)

- **No inbox / dedup table.** Consumer-side idempotency is the consumer's responsibility
  at v0.1.0. Inbox is v0.2.0 work.
- **No bundled broker.** RabbitMQ, Azure Service Bus, Kafka, NATS sinks are v0.2+ separate
  opt-in packages. `IOutboxSink` is the stable extension point.
- **No saga / process manager.** That is OrionFlow territory.
- **No push-based dispatch yet** (PostgreSQL `LISTEN/NOTIFY`, SQL Server Service Broker).
  Polling is the v0.1.0 baseline; push lands per backend in v0.3+.
- **No operator UI.** May ship eventually as `OrionPatch.Dashboard`; not on the 1.x
  critical path.

---

## Telemetry

`ActivitySource` and `Meter` named `Moongazing.OrionPatch`. Spans `OrionPatch.Enqueue`,
`OrionPatch.Dispatch`, `OrionPatch.Dispatch.Claim`. Counters
`orionpatch.outbox.enqueued`, `orionpatch.outbox.dispatched`, `orionpatch.outbox.failed`,
`orionpatch.outbox.deadlettered`, `orionpatch.outbox.attempts`. Histogram
`orionpatch.outbox.dispatch.duration`. Observable gauge `orionpatch.outbox.queue_depth`.

## Roadmap

Twelve-month forward plan in [ROADMAP.md](ROADMAP.md): v0.2.0 (Q4 2026) inbox + dedup
and concrete broker sinks (RabbitMQ, Azure Service Bus), v0.3.0 (Q1 2027) push-based
dispatch, v0.4.0 (Q1-Q2 2027) operator dashboard + schema-evolution helpers, v1.0.0
(Q2 2027) API freeze. If something on the list matters to you, open an issue with the
`roadmap` label.

## More from the Orion family

- [OrionGuard](https://github.com/tunahanaliozturk/OrionGuard) — validation, guard clauses, DDD primitives, domain events
- [OrionAudit](https://github.com/tunahanaliozturk/OrionAudit) — automatic EF Core change-audit trail
- [OrionKey](https://github.com/tunahanaliozturk/OrionKey) — source-generated strongly-typed IDs
- [OrionLock](https://github.com/tunahanaliozturk/OrionLock) — distributed locking with lease auto-renewal

## License

MIT. See [LICENSE.txt](LICENSE.txt).
