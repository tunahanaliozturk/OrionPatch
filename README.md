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

## What it does

OrionPatch is a transactional outbox primitive for .NET. You enqueue a message inside an EF Core `SaveChanges` call; it commits in the same transaction as your domain data; a background dispatcher hands it to a pluggable `IOutboxSink` at-least-once.

The package is deliberately small. It does NOT ship a broker — RabbitMQ, Azure Service Bus, Kafka, NATS sinks live in separate opt-in sub-packages on the v0.2+ roadmap. v0.1.0 ships one concrete sink: `ChannelOutboxSink` (in-process `System.Threading.Channels`, zero external dependency, useful for monoliths and tests).

It is also deliberately scoped. No inbox in v0.1.0 — inbox idempotency, dedup tables, broker-side consumer wrappers are v0.2 work. v0.1.0 owns one thing well: getting a message from "I just did a domain mutation" to "the sink received it, exactly once per row, even if my process crashes between commit and send."

## Why OrionPatch?

| Feature                          | OrionPatch | DIY interceptor | MassTransit | Wolverine |
|----------------------------------|:----------:|:---------------:|:-----------:|:---------:|
| Transactional enqueue            | Yes        | Yes             | Yes         | Yes       |
| At-least-once dispatch           | Yes        | Maybe           | Yes         | Yes       |
| EF Core SaveChangesInterceptor   | Yes        | Yes             | Optional    | -         |
| Multi-provider claim (SQL Server/Postgres/MySQL/SQLite) | Yes (v0.2 for native SKIP LOCKED) | Maybe | Optional | Yes |
| Pluggable sink (no broker bundled) | Yes      | -               | Bundled     | Bundled   |
| Built-in retry + dead-letter     | Yes        | Maybe           | Yes         | Yes       |
| OpenTelemetry                    | Yes        | Maybe           | Yes         | Yes       |
| In-process test sink             | Yes        | -               | Yes         | Yes       |
| Saga / process manager           | No (out of scope) | -        | Yes         | Yes       |
| Standalone primitive (no framework) | Yes     | Yes             | No          | No        |

OrionPatch is a primitive, not a framework. If you want sagas, request/response, or a built-in mediator, reach for MassTransit or Wolverine. If you want transactional outbox without adopting a messaging framework, OrionPatch is the package.

## 30-second quick start

```csharp
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.DependencyInjection;
using Moongazing.OrionPatch.EntityFrameworkCore;
using Moongazing.OrionPatch.EntityFrameworkCore.DependencyInjection;

services.AddDbContext<AppDbContext>((sp, options) =>
{
    options.UseNpgsql(connectionString);
    options.UseOrionPatch(sp);
});

services.AddOrionPatch()
    .UseEntityFrameworkCore<AppDbContext>()
    .UseSink<MyKafkaSink>();   // or .UseChannelSink() for in-process
```

Apply the entity configuration in `OnModelCreating`:

```csharp
protected override void OnModelCreating(ModelBuilder modelBuilder) =>
    modelBuilder.ApplyOrionPatchConfiguration();
```

Enqueue from your service code:

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

Implement a sink:

```csharp
public sealed class MyKafkaSink : IOutboxSink
{
    public async Task SendAsync(OutboxEnvelope envelope, CancellationToken ct)
    {
        // External publish — keep this the last statement of the implementation so
        // a failure after publish does not silently lose acknowledgement.
        await _producer.ProduceAsync(envelope.MessageType, envelope.Payload, ct);
    }
}
```

That's it. The dispatcher runs as a hosted service; messages flow from your transaction into the sink.

## What ships in v0.1.0

| Package | Description |
|---------|-------------|
| `OrionPatch` | Core: `IOutbox`, `IOutboxSink`, `IOutboxStorage`, dispatcher hosted service, telemetry, options. Includes `ChannelOutboxSink`. |
| `OrionPatch.EntityFrameworkCore` | EF Core storage backend: `OrionPatch_Outbox` table, provider-aware claim (`SKIP LOCKED` for SqlServer/Postgres/MySQL deferred to v0.2; SQLite + unknown providers use a portable compare-and-swap fallback today), `SaveChangesInterceptor` for transactional enqueue. |
| `OrionPatch.Testing` | Test helpers: in-memory storage, deterministic dispatcher, capturing sink, test clock, fluent assertions. Zero EF Core dependency. |

## What v0.1.0 does NOT do

- No inbox / dedup table (v0.2).
- No concrete broker sinks (RabbitMQ / Azure Service Bus / Kafka / NATS) — those are opt-in sub-packages on the v0.2+ roadmap.
- No saga / process manager (that is OrionFlow territory).
- No distributed transactions across heterogeneous sinks.
- No push-based dispatch (PostgreSQL `LISTEN/NOTIFY`, SQL Server Service Broker) — v0.3+ work.

## At-least-once contract

OrionPatch guarantees at-least-once delivery. Duplicates occur in two known scenarios:

1. The sink succeeds but the subsequent `CompleteAsync` write fails or the process crashes before it runs. The row stays Claimed, the lease expires, another dispatcher re-delivers.
2. The sink call exceeds `OrionPatchOptions.LeaseDuration` (default 2 minutes). Another dispatcher may claim and re-deliver the row mid-flight.

Consumer sinks MUST be idempotent. Typical patterns: deduplicate at the destination on `OutboxEnvelope.Id`, or use upserts. Keep the external publish the last statement of the sink implementation so a failure after publish does not silently lose acknowledgement.

## Telemetry

- `ActivitySource` and `Meter` named `Moongazing.OrionPatch`.
- Spans: `OrionPatch.Dispatch` per envelope, tagged with `orionpatch.message.type` and `orionpatch.attempt`.
- Counters: `orionpatch.outbox.enqueued`, `.dispatched`, `.failed`, `.deadlettered`, `.attempts`.
- Histogram: `orionpatch.outbox.dispatch.duration` (milliseconds).

Wire them up with the standard OpenTelemetry .NET helpers.

## Roadmap

12-month forward plan in [ROADMAP.md](ROADMAP.md). The next milestones:

- v0.2.0 (Q4 2026) — Inbox + dedup; concrete sinks for RabbitMQ + Azure Service Bus; native `SKIP LOCKED` SQL for SqlServer/Postgres/MySQL.
- v0.3.0 (Q1 2027) — Push-based dispatch (LISTEN/NOTIFY, Service Broker).
- v0.4.0 (Q1-Q2 2027) — Operator dashboard, schema-evolution helpers.
- v1.0.0 (Q2 2027) — API freeze, LTS window.

If something on the list matters to you, open an issue with the `roadmap` label.

## More from the Orion family

OrionPatch is one of several standalone .NET libraries:

- [OrionGuard](https://github.com/tunahanaliozturk/OrionGuard) — input validation, guard clauses, DDD primitives.
- [OrionAudit](https://github.com/tunahanaliozturk/OrionAudit) — EF Core audit trail with JSON Patch diffs and time-travel reconstruction.
- [OrionKey](https://github.com/tunahanaliozturk/OrionKey) — source-generated strongly-typed IDs.
- [OrionLock](https://github.com/tunahanaliozturk/OrionLock) — distributed lock primitive with auto-renewing leases.

Each ships separately; none depends on another at runtime.

## License

MIT. See [LICENSE.txt](LICENSE.txt).
