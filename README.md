<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/logo.png">
    <img src="docs/icon.png" alt="OrionPatch logo" width="150">
  </picture>
</p>

<h1 align="center">OrionPatch</h1>

<p align="center">
  Transactional outbox primitive for .NET. Enqueue inside SaveChanges, dispatch at-least-once through a pluggable sink.
</p>

<p align="center">
  <a href="https://github.com/tunahanaliozturk/OrionPatch/actions/workflows/ci-cd.yml"><img src="https://github.com/tunahanaliozturk/OrionPatch/actions/workflows/ci-cd.yml/badge.svg" alt="CI/CD" /></a>
  <a href="https://www.nuget.org/packages/OrionPatch"><img src="https://img.shields.io/nuget/v/OrionPatch?style=flat-square&color=blue" alt="NuGet" /></a>
  <a href="LICENSE.txt"><img src="https://img.shields.io/badge/license-MIT-yellow?style=flat-square" alt="License" /></a>
  <img src="https://img.shields.io/badge/.NET-8.0%20%7C%209.0%20%7C%2010.0-purple?style=flat-square" alt="Target" />
</p>

---

## What it does

OrionPatch is a transactional outbox primitive for .NET. You enqueue a message inside an EF Core `SaveChanges` call; it commits in the same transaction as your domain data; a background dispatcher hands it to a pluggable `IOutboxSink` at-least-once.

The current release is 0.4.2. Per-version history is in the [CHANGELOG](CHANGELOG.md).

The core package is deliberately small and ships no broker itself. Concrete broker sinks for **RabbitMQ**, **Azure Service Bus**, and **Kafka** ship as separate opt-in sub-packages (`OrionPatch.RabbitMQ`, `OrionPatch.AzureServiceBus`, `OrionPatch.Kafka`); a NATS sink remains on the roadmap. The core also ships `ChannelOutboxSink` (in-process `System.Threading.Channels`, zero external dependency, useful for monoliths and tests).

At its core it owns one thing well: getting a message from "I just did a domain mutation" to "the sink received it — at least once per row, even if my process crashes between commit and send." (Delivery is at-least-once; sinks must be idempotent.) Inbox idempotency / dedup, the dead-letter store, redrive and archival build outward from that core.

![OrionPatch packages: the app calls the core; OrionPatch.EntityFrameworkCore provides IOutbox and IOutboxStorage over your database; the Kafka, RabbitMQ and Azure Service Bus packages provide IOutboxSink implementations; OrionPatch.Testing swaps in in-memory doubles for tests](docs/diagrams/overview.png)

## How it works

A message is enqueued by application code, persisted by the EF Core interceptor inside the same transaction as your data, then handed to the sink asynchronously by a hosted dispatcher.

![OrionPatch outbox write and dispatch: Enqueue buffers the message, SaveChangesAsync inserts the outbox row and your domain rows in one transaction, the dispatcher claims due rows, calls IOutboxSink.SendAsync for the Kafka, RabbitMQ or Azure Service Bus sink, then marks the row Processed with CompleteAsync](docs/diagrams/outbox-dispatch.png)

The outbox row and the domain rows commit together, but the sink call happens outside the transaction. That is the at-least-once contract: a crash between `SendAsync` and `CompleteAsync` leaves the row claimed, and it is sent again once its lease expires.

When `SendAsync` throws, the row is retried with backoff until `MaxAttempts`, then dead-lettered:

![OrionPatch retry and dead-letter flow: a failed send below MaxAttempts is rescheduled with FailAsync and BackoffStrategy; at MaxAttempts the row moves to the dead-letter store when the storage implements IDeadLetterStore, otherwise it is marked DeadLettered in place; RedriveAsync puts a dead-lettered message back in the outbox](docs/diagrams/dispatch-retry.png)

## Why OrionPatch?

| Feature                          | OrionPatch | DIY interceptor | MassTransit | Wolverine |
|----------------------------------|:----------:|:---------------:|:-----------:|:---------:|
| Transactional enqueue            | Yes        | Yes             | Yes         | Yes       |
| At-least-once dispatch           | Yes        | Maybe           | Yes         | Yes       |
| EF Core SaveChangesInterceptor   | Yes        | Yes             | Optional    | -         |
| Multi-provider claim (SQL Server/Postgres/MySQL/SQLite) | Yes (native `SKIP LOCKED` on Postgres/MySQL, `READPAST` on SQL Server) | Maybe | Optional | Yes |
| Pluggable sink (no broker bundled) | Yes      | -               | Bundled     | Bundled   |
| Built-in retry + dead-letter     | Yes        | Maybe           | Yes         | Yes       |
| Dead-letter store (route exhausted rows out of the hot outbox) | Yes (v0.3) | Maybe | Yes | Yes |
| Outbox archival / retention (reap processed rows) | Yes (v0.3) | Maybe | Optional | Optional |
| OpenTelemetry                    | Yes        | Maybe           | Yes         | Yes       |
| In-process test sink             | Yes        | -               | Yes         | Yes       |
| Saga / process manager           | No (out of scope) | -        | Yes         | Yes       |
| Standalone primitive (no framework) | Yes     | Yes             | No          | No        |

OrionPatch is a primitive, not a framework. If you want sagas, request/response, or a built-in mediator, reach for MassTransit or Wolverine. If you want transactional outbox without adopting a messaging framework, OrionPatch is the package.

## 30-second quick start

```bash
dotnet add package OrionPatch.EntityFrameworkCore
dotnet add package OrionPatch.Kafka   # or OrionPatch.RabbitMQ / OrionPatch.AzureServiceBus
```

```csharp
using Microsoft.EntityFrameworkCore;
using Moongazing.OrionPatch.DependencyInjection;
using Moongazing.OrionPatch.EntityFrameworkCore.DependencyInjection;
using Moongazing.OrionPatch.Kafka;

services.AddDbContext<AppDbContext>((sp, options) =>
{
    options.UseNpgsql(connectionString);
    options.UseOrionPatch(sp);   // adds the SaveChanges interceptor
});

services.AddOrionPatch()
    .UseEntityFrameworkCore<AppDbContext>();

services.AddOrionPatchKafkaSink(o =>
{
    o.BootstrapServers = "localhost:9092";
    o.Topic = "orders";
});
```

Instead of a broker sink, `.UseChannelSink()` delivers in-process, and `.UseSink<TSink>()` registers your own `IOutboxSink`.

Apply the entity configuration in `OnModelCreating` (namespace `Moongazing.OrionPatch.EntityFrameworkCore`), then add a migration; the runtime never creates tables:

```csharp
protected override void OnModelCreating(ModelBuilder modelBuilder) =>
    modelBuilder.ApplyOrionPatchConfiguration();
```

Enqueue from your service code:

```csharp
public sealed class OrderService(AppDbContext db, IOutbox outbox)
{
    public async Task ConfirmOrderAsync(Guid orderId, CancellationToken ct)
    {
        var order = await db.Orders.SingleAsync(o => o.Id == orderId, ct);
        order.Confirm();

        outbox.Enqueue(new OrderConfirmed(order.Id, order.TotalCents));

        await db.SaveChangesAsync(ct);   // outbox row + order update commit together
    }
}
```

Or implement your own sink:

```csharp
public sealed class WebhookSink(HttpClient http) : IOutboxSink
{
    public async Task SendAsync(OutboxEnvelope envelope, CancellationToken cancellationToken = default)
    {
        using var content = new StringContent(envelope.Payload, Encoding.UTF8, "application/json");
        content.Headers.Add("Idempotency-Key", envelope.Id.ToString("N"));

        using var response = await http.PostAsync(
            new Uri($"events/{envelope.MessageType}", UriKind.Relative), content, cancellationToken);
        response.EnsureSuccessStatusCode();   // throwing schedules a retry with backoff
    }
}

// services.AddOrionPatch().UseEntityFrameworkCore<AppDbContext>().UseSink<WebhookSink>();
// (UseSink registers the sink as a singleton; register its dependencies, here HttpClient, yourself.)
```

That's it. The dispatcher runs as a hosted service; messages flow from your transaction into the sink.

## Options

`AddOrionPatch(o => ...)` configures `OrionPatchOptions`:

| Option | Default | Meaning |
|--------|---------|---------|
| `PollingInterval` | 1 s | Wait between polls when the last claim returned no rows. |
| `BatchSize` | 50 | Maximum rows claimed per poll. |
| `MaxAttempts` | 5 | Attempts before a row is dead-lettered. |
| `LeaseDuration` | 2 min | How long a claimed row stays reserved before another poll can reclaim it. |
| `BackoffStrategy` | `BackoffStrategy.Exponential(1 s, 30 min)` | Delay before the next attempt (1 s, 2 s, 4 s, 8 s ...). `BackoffStrategy.Fixed(delay)` is the other built-in. |
| `ArchiveRetention` | 7 days | Horizon for `ArchiveProcessedAsync`; must be non-negative. |
| `DispatcherEnabled` | `true` | `false` registers no dispatcher (enqueue-only hosts). |
| `DispatcherIdentityFactory` | `"{MachineName}/{ProcessId}"` | Identity written to claimed rows. |
| `JsonOptions` | `JsonSerializerDefaults.Web` | Payload serialization. |

## Packages

| Package | Description |
|---------|-------------|
| `OrionPatch` | Core: `IOutbox`, `IOutboxSink`, `IOutboxStorage`, dispatcher hosted service, telemetry, options, `IInbox` + `InMemoryInbox`. Includes `ChannelOutboxSink`. |
| `OrionPatch.EntityFrameworkCore` | EF Core storage backend: `OrionPatch_Outbox` table, provider-aware claim (native `FOR UPDATE SKIP LOCKED` on PostgreSQL/MySQL, `UPDLOCK, READPAST` on SQL Server; SQLite + unknown providers use a portable compare-and-swap fallback), `SaveChangesInterceptor` for transactional enqueue, dead-letter and archive tables, and an inbox / dedup table. |
| `OrionPatch.Testing` | Test helpers: in-memory storage, deterministic dispatcher, capturing sink, test clock, fluent assertions. Zero EF Core dependency. |
| `OrionPatch.RabbitMQ` | RabbitMQ broker sink (`AddOrionPatchRabbitMqSink`) plus an inbox-deduped consumer (`AddOrionPatchRabbitMqConsumer`). |
| `OrionPatch.AzureServiceBus` | Azure Service Bus broker sink (`AddOrionPatchAzureServiceBusSink`). |
| `OrionPatch.Kafka` | Kafka broker sink (`AddOrionPatchKafkaSink`) plus a Kafka inbox consumer (`AddOrionPatchKafkaInbox`) and `KafkaProducerHealthCheck`. |

## What the core does NOT do

- No broker bundled in the core — RabbitMQ, Azure Service Bus, and Kafka sinks ship as opt-in sub-packages; a NATS sink is still on the roadmap.
- No saga / process manager (that is OrionSaga territory).
- No distributed transactions across heterogeneous sinks.
- No push-based dispatch (PostgreSQL `LISTEN/NOTIFY`, SQL Server Service Broker) — on the [roadmap](ROADMAP.md).

## At-least-once contract

OrionPatch guarantees at-least-once delivery. Duplicates occur in two known scenarios:

1. The sink succeeds but the subsequent `CompleteAsync` write fails or the process crashes before it runs. The row is not marked processed, so a later poll (this dispatcher or another) delivers it again.
2. The sink call exceeds `OrionPatchOptions.LeaseDuration` (default 2 minutes). Another dispatcher may claim and re-deliver the row mid-flight.

Consumer sinks MUST be idempotent. Typical patterns: deduplicate at the destination on `OutboxEnvelope.Id`, or use upserts. The broker sinks stamp the envelope id on every message (Kafka key and `orionpatch-envelope-id` header, RabbitMQ `MessageId`, Service Bus `MessageId`), and the RabbitMQ consumer and Kafka inbox deduplicate on it through `IInbox`.

## Dead-letter store, redrive and archival

These are SPIs on the storage backend, not separate services: a storage type opts in by implementing the interface, and the dispatcher uses it when present. `OrionPatch.EntityFrameworkCore` and the in-memory storage of `OrionPatch.Testing` implement all three and register them in DI.

### Dead-letter store (`IDeadLetterStore`)

When a row exhausts `OrionPatchOptions.MaxAttempts`, the dispatcher prefers to route it OUT of the hot outbox into a dedicated dead-letter store instead of flipping it to `DeadLettered` in place. Routing removes the source row from the active outbox (so it can never be reclaimed or retried) and appends a `DeadLetteredMessage` snapshot carrying the final failure context: payload, headers, correlation id, enqueue time, total attempt count, final error, and the dead-letter instant.

Routing is idempotent on the row id. A redelivered or crash-replayed terminal-path call for an already-routed row is a no-op, so a message lands in the store exactly once and produces no duplicate metrics or alerts. Storage that does not implement `IDeadLetterStore` keeps the in-place status flip.

This is distinct from the `IDeadLetterSink` observer. The sink is a fire-and-forget triage notification (Slack, PagerDuty); the store is the durable destination the message is moved into.

```csharp
using var scope = serviceProvider.CreateScope();
var deadLetters = scope.ServiceProvider.GetRequiredService<IDeadLetterStore>();

IReadOnlyList<DeadLetteredMessage> abandoned = await deadLetters.GetDeadLetteredAsync(ct);
foreach (var message in abandoned)
{
    Console.WriteLine($"{message.Id} {message.MessageType} after {message.AttemptCount} attempts: {message.FinalError}");
}
```

### Redrive (`IDeadLetterReplayStore`)

`RedriveAsync(messageId)` puts a dead-lettered message back into the outbox as a `Pending` row with the same id, attempt count reset, and a `redriven-from` header; it is removed from the dead-letter store in the same step and is idempotent (a second call returns `false`). `RedriveAsync(RedriveFilter, batchSize)` redrives a whole class of failures in bounded batches and returns a `RedriveResult`.

```csharp
var replay = scope.ServiceProvider.GetRequiredService<IDeadLetterReplayStore>();

bool redriven = await replay.RedriveAsync(messageId, ct);
RedriveResult result = await replay.RedriveAsync(
    new RedriveFilter(MessageType: "MyApp.OrderConfirmed"), batchSize: 100, ct);
```

### Archival (`IOutboxArchivalStore`)

Successfully dispatched (`Processed`) rows accumulate in the hot outbox; an ever-growing table degrades claim-query planning and storage cost. `ArchiveProcessedAsync` reaps `Processed` rows whose `ProcessedAtUtc` is at or before `nowUtc - retention` out of the active outbox and returns the count moved. Pending, Claimed, and DeadLettered rows are never touched, and a processed row still inside the retention window is never touched. The reap is idempotent and incremental, so it is safe to call on a schedule.

`OrionPatchOptions.ArchiveRetention` (default 7 days, validated non-negative) expresses the retention horizon. `ArchiveProcessedAsync` is operator-invoked maintenance: OrionPatch does not start a background reaper, so call it from your own scheduled job (a hosted `BackgroundService`, Quartz.NET, Hangfire, or a cron-triggered endpoint).

```csharp
// Run from a scheduled maintenance job, e.g. nightly.
var archival = scope.ServiceProvider.GetRequiredService<IOutboxArchivalStore>();
var options = scope.ServiceProvider.GetRequiredService<IOptions<OrionPatchOptions>>().Value;

int reaped = await archival.ArchiveProcessedAsync(options.ArchiveRetention, DateTime.UtcNow, ct);
```

The EF Core backend copies reaped rows to `OrionPatch_OutboxArchive` by default, or deletes them with `UseEntityFrameworkCore<AppDbContext>(purgeOnArchive: true)`. The bundled `InMemoryOutboxStorage` has the same two modes (`new InMemoryOutboxStorage(purgeOnArchive: true)`); archived rows are readable through `GetArchivedAsync`.

## Telemetry

- `ActivitySource` and `Meter` named `Moongazing.OrionPatch` (`OrionPatchDiagnostics.SourceName`).
- Spans: `OrionPatch.Dispatch` per envelope, tagged with `orionpatch.message.type` and `orionpatch.attempt`.
- Counters: `orionpatch.outbox.dispatched`, `.failed`, `.deadlettered`, `.attempts`, `.poll.idle`, `.dead_letter.redriven`, `.dead_letter_sink_failures`, `.dispatch_observer_failures`. `orionpatch.outbox.enqueued` is declared but not recorded in 0.4.2.
- Histograms: `orionpatch.outbox.dispatch.duration` (ms), `.sink.duration_ms`, `.poll.duration`, `.batch_size`, `.claim.batch_fill_ratio`, `.queue_lag`, `.dispatch.pickup_lag_ms`, `.dead_letter.age_ms`, `.attempts_per_row`, `.dispatch.envelope_bytes`.
- Gauge: `orionpatch.outbox.queue_depth`.
- The Kafka inbox has its own meter, `Moongazing.OrionPatch.Kafka.Inbound` (`orionpatch.kafka.inbound.*`).

Wire them up with the standard OpenTelemetry .NET helpers.

## Benchmarks

A BenchmarkDotNet suite for the core's in-memory hot paths lives in `benchmarks/Moongazing.OrionPatch.Benchmarks`. See [benchmarks.md](benchmarks.md) for what it measures and how to run it.

## Roadmap

The current release is 0.4.2. See the [CHANGELOG](CHANGELOG.md) for the full per-version history.

12-month forward plan in [ROADMAP.md](ROADMAP.md). The next milestones:

- Partitioned / ordered dispatch and push-based dispatch (LISTEN/NOTIFY, Service Broker).
- Operator surface: dispatcher health check, dashboard, schema-evolution helpers.
- v1.0.0: API freeze, LTS window.

If something on the list matters to you, open an issue with the `roadmap` label.

## More from the Orion family

OrionPatch is one of several standalone .NET libraries:

- [OrionGuard](https://github.com/tunahanaliozturk/OrionGuard) — input validation, guard clauses, DDD primitives.
- [OrionAudit](https://github.com/tunahanaliozturk/OrionAudit) — EF Core audit trail with JSON Patch diffs and time-travel reconstruction.
- [OrionKey](https://github.com/tunahanaliozturk/OrionKey) — source-generated strongly-typed IDs.
- [OrionLock](https://github.com/tunahanaliozturk/OrionLock) — distributed lock primitive with auto-renewing leases.

Each ships separately; none depends on another at runtime.

### See it in a real app

[Moongazing.OrionShowcase](https://github.com/tunahanaliozturk/OrionShowcase) is a production-shaped banking sample integrating all six Orion packages end-to-end. OrionPatch outbox interceptor captures Account/Customer domain events into the same transaction as SaveChanges. DomainEventOutboxAdapter walks AggregateRoot.DomainEvents and enqueues via reflection on IOutbox.Enqueue<T>. Concrete usage:

- [src/Moongazing.OrionShowcase.Infrastructure/Outbox/DomainEventOutboxAdapter.cs](https://github.com/tunahanaliozturk/OrionShowcase/blob/main/src/Moongazing.OrionShowcase.Infrastructure/Outbox/DomainEventOutboxAdapter.cs)
- [src/Moongazing.OrionShowcase.Infrastructure/DependencyInjection/InfrastructureServiceCollectionExtensions.cs](https://github.com/tunahanaliozturk/OrionShowcase/blob/main/src/Moongazing.OrionShowcase.Infrastructure/DependencyInjection/InfrastructureServiceCollectionExtensions.cs)

## Contributing

Issues and pull requests welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) and the [Code of Conduct](CODE_OF_CONDUCT.md) before opening one. Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md).

## License

MIT. See [LICENSE.txt](LICENSE.txt).
