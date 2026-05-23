# OrionPatch v0.1.0 — Design Spec

**Author:** Tunahan Ali Ozturk
**Date:** 2026-05-24
**Status:** Draft, pending user review
**Target ship:** Q3 2026, mid-quarter

---

## What OrionPatch is

A transactional outbox primitive for .NET. You enqueue messages from inside an EF Core
`SaveChanges` call; they commit in the same transaction as your domain data; a background
dispatcher hands them to a pluggable sink at-least-once.

The package is deliberately small. It does **not** ship a broker — RabbitMQ, Azure Service
Bus, Kafka, NATS sinks live in separate opt-in sub-packages on the v0.2+ roadmap. v0.1.0
ships one concrete sink: `ChannelOutboxSink` (in-process `System.Threading.Channels`,
zero external dependency, useful for monoliths and tests).

It is also deliberately scoped. **No inbox in v0.1.0** — inbox idempotency, dedup tables,
broker-side consumer wrappers are v0.2.0 work. v0.1.0 owns one thing well: getting a message
from "I just did a domain mutation" to "the sink received it, exactly once per row, even if
my process crashes between commit and send."

---

## Why a new package

There are existing options in the .NET space. They are not the same package OrionPatch
intends to be:

- **MassTransit** is excellent but it is a *messaging framework* — sagas, scheduling, request/response,
  serializer abstractions, the whole stack. OrionPatch is a primitive that fits underneath one
  of those, not a replacement for them.
- **NServiceBus** is commercial, and the same "framework, not primitive" comment applies.
- **Wolverine** is closer in spirit but bundles transactional inbox/outbox with a mediator,
  HTTP endpoint generator, and code-first messaging. Strong tool; not a primitive.
- **DIY transactional outbox** in a custom EF Core interceptor — what most teams write today.
  Easy to get wrong: ordering, dead-letter, telemetry, lease/competing-consumers under
  horizontal scale, schema evolution. OrionPatch packages the answers.

The Orion family rule applies: OrionPatch must justify itself standalone. It does — every
microservice needs transactional outbox; very few want to adopt a whole messaging framework
to get it.

## How it fits the family

- **OrionAudit v0.7.0** (Q4 2026) has a planned `IAuditEventPublisher` outbox publish hook.
  OrionPatch is the reference implementation of that hook. OrionAudit does not take a
  `<PackageReference>` on OrionPatch — the bridge will be a separate `OrionAudit.OrionPatch`
  adapter package or pure consumer-side code. Family no-cross-coupling rule preserved.
- **OrionLock** competes with OrionPatch for the same database when both run against EF Core.
  OrionPatch's competing-consumers dispatcher uses provider-specific `SKIP LOCKED` / row-claim
  patterns lifted from OrionLock's EF Core backend — so the two libraries share an idiom
  without sharing a binary.

---

## Public API surface (v0.1.0)

### Enqueueing — interceptor-based, transactional

```csharp
public interface IOutbox
{
    void Enqueue<T>(T message, OutboxEnqueueOptions? options = null) where T : class;
}

public sealed class OutboxEnqueueOptions
{
    public string? MessageType { get; init; }   // override the registered type name
    public string? CorrelationId { get; init; } // overrides AsyncLocal correlation, if set
    public IReadOnlyDictionary<string, string>? Headers { get; init; }
    public DateTime? OccurredAtUtc { get; init; }
}
```

Consumer code:

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

Behind the scenes: `IOutbox` is scoped to the DbContext. `Enqueue` buffers in-memory.
`OrionPatchSaveChangesInterceptor` flushes the buffer as `OrionPatch_Outbox` rows during
`SavingChanges`, so they participate in the user's transaction.

### Dispatcher — background, competing-consumers safe

```csharp
public interface IOutboxSink
{
    Task SendAsync(OutboxEnvelope envelope, CancellationToken ct);
}

public sealed record OutboxEnvelope(
    Guid Id,
    string MessageType,
    string Payload,                                         // JSON
    IReadOnlyDictionary<string, string>? Headers,
    string? CorrelationId,
    DateTime OccurredAtUtc,
    int AttemptNumber);
```

Consumer code:

```csharp
public sealed class MyKafkaSink : IOutboxSink
{
    public async Task SendAsync(OutboxEnvelope env, CancellationToken ct)
    {
        await _producer.ProduceAsync(env.MessageType, env.Payload, ct);
    }
}
```

A single `IOutboxSink` is registered per service. `OutboxDispatcherHostedService` polls the
storage backend on a configurable interval (default 1 s), claims a batch using the storage's
competing-consumers primitive, dispatches each envelope through the sink, and marks
processed/failed in the same row.

**At-least-once.** A claim is held under a lease; if the dispatcher process crashes between
"sink succeeded" and "row marked processed", the next claim will re-deliver. Consumer
critical sections must be idempotent.

**Dead-letter.** After `MaxAttempts` (default 5) with exponential backoff (default
1 s, 5 s, 30 s, 5 min, 30 min), the row's `Status` flips to `DeadLettered`. No retries
after that until an operator resets the row.

### DI surface

```csharp
services.AddOrionPatch()
    .UseEntityFrameworkCore<AppDbContext>()
    .UseSink<MyKafkaSink>();
```

Optional fluent options:

```csharp
services.AddOrionPatch(o =>
{
    o.PollingInterval     = TimeSpan.FromSeconds(1);
    o.BatchSize           = 50;
    o.MaxAttempts         = 5;
    o.RetryBackoff        = BackoffStrategy.Exponential(initial: 1s, max: 30min);
    o.LeaseDuration       = TimeSpan.FromMinutes(2);   // lease while in-flight in dispatcher
    o.DispatcherEnabled   = true;                       // set false to run a "writer-only" replica
})
.UseEntityFrameworkCore<AppDbContext>()
.UseSink<MyKafkaSink>();
```

### Built-in sinks

- **`ChannelOutboxSink`** — pushes envelopes into a bounded
  `System.Threading.Channels.Channel<OutboxEnvelope>`. Useful for monoliths (in-process
  pub/sub) and unit tests. Capacity, full-mode, and consumer wiring all configurable. Zero
  external dependency.

### Storage backend

`IOutboxStorage` is the SPI. v0.1.0 ships **one** implementation:

- **`OrionPatch.EntityFrameworkCore`** — `OrionPatch_Outbox` table, provider-agnostic (SQLite,
  SQL Server, PostgreSQL, MySQL all supported via EF Core). Claim uses `SKIP LOCKED` on
  providers that support it, falls back to `UPDATE ... WHERE Status=Pending AND ClaimedBy IS NULL`
  with a `RowVersion` token on providers that do not (SQLite).

### Telemetry

- `ActivitySource` named `Moongazing.OrionPatch`.
- Spans: `OrionPatch.Enqueue` (per `Enqueue` call), `OrionPatch.Dispatch` (per envelope sink
  call), `OrionPatch.Dispatch.Claim` (per polling claim).
- Counters: `orionpatch.outbox.enqueued`, `orionpatch.outbox.dispatched`,
  `orionpatch.outbox.failed`, `orionpatch.outbox.deadlettered`,
  `orionpatch.outbox.attempts`.
- Histogram: `orionpatch.outbox.dispatch.duration` (per envelope).
- Observable gauge: `orionpatch.outbox.queue_depth` (count of `Status = Pending` rows,
  sampled).

---

## Schema — `OrionPatch_Outbox`

| Column            | Type                  | Nullable | Notes                                            |
|-------------------|-----------------------|:--------:|--------------------------------------------------|
| `Id`              | `uniqueidentifier`    |    N     | PK; generated client-side (sortable not required)|
| `MessageType`     | `nvarchar(256)`       |    N     | Logical type name; defaults to FQN of T          |
| `Payload`         | `nvarchar(max)`/jsonb |    N     | JSON; serialized with `System.Text.Json`         |
| `Headers`         | `nvarchar(max)`/jsonb |    Y     | Optional JSON map of string→string headers       |
| `CorrelationId`   | `nvarchar(128)`       |    Y     | Picks up from `AuditScope`-style `AsyncLocal`    |
| `OccurredAtUtc`   | `datetime2`           |    N     | When the domain event happened                   |
| `EnqueuedAtUtc`   | `datetime2`           |    N     | Defaults to `OccurredAtUtc`; set by interceptor  |
| `Status`          | `tinyint`             |    N     | 0=Pending, 1=Claimed, 2=Processed, 3=DeadLettered|
| `AttemptCount`    | `int`                 |    N     | Incremented on each dispatch attempt             |
| `ClaimedAtUtc`    | `datetime2`           |    Y     | Lease anchor; lease = `ClaimedAtUtc + Lease`     |
| `ClaimedBy`       | `nvarchar(128)`       |    Y     | Dispatcher identity (machine + process id)       |
| `LastError`       | `nvarchar(max)`       |    Y     | Truncated stack/message of the last failure      |
| `ProcessedAtUtc`  | `datetime2`           |    Y     | Set when `Status = Processed`                    |
| `NextAttemptAtUtc`| `datetime2`           |    Y     | Set by backoff; dispatcher filters on this       |
| `RowVersion`      | `rowversion`/`xmin`   |    N     | Optimistic-concurrency token for non-SKIP-LOCKED |

Indexes:

- `IX_OrionPatch_Outbox_Status_NextAttemptAtUtc` — covers the dispatcher's polling query
  (`WHERE Status = Pending AND (NextAttemptAtUtc IS NULL OR NextAttemptAtUtc <= NOW)`).
- `IX_OrionPatch_Outbox_ClaimedAtUtc` — for lease-expiry sweep.

---

## NuGet packages shipped at v0.1.0

| Package                              | Description                                              |
|--------------------------------------|----------------------------------------------------------|
| `OrionPatch`                         | Core: `IOutbox`, `IOutboxSink`, dispatcher hosted service, telemetry, options. Includes `ChannelOutboxSink`. |
| `OrionPatch.EntityFrameworkCore`     | `IOutboxStorage` over EF Core; `OrionPatch_Outbox` table; provider-aware claim; migration helper. |
| `OrionPatch.Testing`                 | In-memory storage, deterministic dispatcher (no background thread), capture/assertion helpers. |

All multi-target net8.0 / net9.0 / net10.0. MIT. `TreatWarningsAsErrors`. `<PackageIcon>`,
`<PackageReadmeFile>`, `<PackageProjectUrl>`, identical to OrionLock's csproj shape.

---

## Out of scope for v0.1.0

These come up in conversation; the answers are "no, on purpose, for this milestone":

- **Inbox / dedup table.** v0.2.0 work. Conceptually a different surface — the sink consumer
  decides their own idempotency boundary; OrionPatch.Inbox will package the storage primitive.
- **Concrete broker sinks.** `OrionPatch.RabbitMQ`, `OrionPatch.AzureServiceBus`,
  `OrionPatch.Kafka`, `OrionPatch.Nats` are v0.2+ separate packages. The `IOutboxSink`
  interface is the stable extension point.
- **Saga / process manager.** That's OrionFlow territory (placeholder future package).
- **Distributed transactions across sinks.** OrionPatch dispatches to one sink with
  at-least-once. Two-phase commit across heterogeneous resources is not in scope.
- **Push-based dispatch** (PostgreSQL `LISTEN/NOTIFY`, SQL Server Service Broker, Redis
  pub/sub trigger). Polling is the v0.1.0 baseline. Push is v0.3+ per backend.
- **Schema evolution / versioning helpers.** Consumers control `MessageType` and `Payload`
  themselves at v0.1.0; an opinionated versioning helper is a candidate for v0.2.
- **Multi-tenant outbox filtering.** OrionAudit ships `IAuditTenantResolver`; OrionPatch
  does not — yet. If the user's `IOutboxSink` cares about tenant, they read it from
  `Headers`. Promotable to first-class in v0.2 if there is demand.

---

## Out of scope for the 1.x line (longer view)

- **A broker.** OrionPatch will never ship a transport. The sink is always pluggable.
- **In-process pub/sub framework.** `ChannelOutboxSink` is a primitive, not a competitor
  to `MediatR` notifications. Use the right tool for the layer.
- **An operator UI.** OrionAudit has the precedent (`OrionAudit.Viewer`); a dead-letter UI
  may ship eventually as `OrionPatch.Dashboard` but it is not on the v1.x critical path.

---

## Risks and trade-offs

- **Polling cost.** A 1-second polling loop against a busy outbox table is fine; against a
  large table with low write rate it is wasteful. v0.3.0 push-based dispatch addresses this.
  Mitigation in v0.1.0: covering index on `(Status, NextAttemptAtUtc)`; configurable interval;
  documented guidance on backoff for idle services.
- **Schema lock-in.** Once `OrionPatch_Outbox` has rows, schema migrations have to preserve
  them. We accept this and freeze the v0.1.0 column set; additions in v0.2+ are nullable
  columns + EF Core migration.
- **`SKIP LOCKED` portability.** SQL Server 2022+, PostgreSQL 9.5+, MySQL 8.0+ support it.
  SQLite does not. The fallback path (`UPDATE WHERE` + `RowVersion`) is correct under
  contention but has more contention failures at scale; SQLite users get correctness, not
  throughput.
- **`IOutbox` scope.** Scoping to the DbContext is non-obvious to .NET DI users who default
  to singleton/transient. We will document this clearly and let the DI helper register the
  right lifetime so users do not have to think about it.
- **Sink crash semantics.** If the sink throws after externally publishing (e.g. Kafka
  produced, then the sink throws on local logging), OrionPatch will retry, causing a
  duplicate. That is the at-least-once contract. The sink author is responsible for keeping
  the "external publish" the last step of `SendAsync`.

---

## Release plan

| Milestone | Window         | Theme                                                |
|-----------|----------------|------------------------------------------------------|
| v0.1.0    | Q3 2026 mid    | Outbox primitive, EF Core storage, Channel sink, telemetry. |
| v0.1.x    | as needed      | Bug fixes; no API changes.                           |
| v0.2.0    | Q4 2026        | Inbox + dedup; concrete sinks (RabbitMQ, Azure Service Bus). |
| v0.3.0    | Q1 2027        | Push-based dispatch (LISTEN/NOTIFY, Service Broker). |
| v0.4.0    | Q1-Q2 2027    | Operator dashboard, schema-evolution helpers.        |
| v1.0.0    | Q2 2027        | API freeze, LTS window, documentation site.          |

---

## Open questions for review

1. **Repository name** — `OrionPatch` is the working name. Reasonable alternatives:
   `OrionMail`, `OrionOutbox`, `OrionRelay`. I prefer `OrionPatch` because the package
   covers more than just "outbox" once v0.2 lands (inbox + retry + dedup are all "patches"
   over an unreliable network).
2. **`IOutbox` lifetime registration** — bound to `DbContext` scope is the right answer;
   confirm the DI helper should handle it transparently versus document that the user
   must register it themselves.
3. **`ChannelOutboxSink` capacity default** — propose 1000 with `BoundedChannelFullMode.Wait`.
   Acceptable?
4. **Dispatcher identity (`ClaimedBy`)** — propose `{MachineName}/{ProcessId}`; reasonable?
5. **`MessageType` default** — propose the FQN of `T` from `Enqueue<T>` unless overridden
   via `OutboxEnqueueOptions.MessageType` or a `MessageTypeRegistry` (latter ships in v0.2).

---

## Next step

Once this spec is approved I will produce the implementation plan
(`docs/superpowers/plans/2026-05-24-orionpatch-v0.1.0.md`) using the writing-plans skill,
then execute it task-by-task via subagent-driven-development.
