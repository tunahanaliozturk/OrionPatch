# OrionPatch Roadmap

This document lists what is shipped, what is actively planned, and what we are deliberately
*not* building. It is a planning artifact, not a contract — dates slip, priorities reshuffle.
If an item here matters to you, open a GitHub issue so we can weigh it against everything else.

## Status legend

- **Shipped** — in the named release on NuGet.
- **Planned** — committed to the named milestone; design is firm.
- **Considered** — interesting but unscheduled. Needs a concrete use case before we commit.
- **Out of scope** — explicitly declined for the 1.x line. The library stays small; some
  features belong in adjacent packages or in user code.

---

## Released

- **v0.1.0** (2026-05-24) - Initial public release. `IOutbox` + `IOutboxSink` + EF Core storage + retry / dead-letter / OpenTelemetry / in-process channel sink. Three NuGet packages: `OrionPatch`, `OrionPatch.EntityFrameworkCore`, `OrionPatch.Testing`.
- **v0.1.1** (2026-05-26) - Logo refresh. No functional change.
- **v0.2.0** (2026-06-01) - `MessageTypeRegistry` for schema-evolution-safe wire names + `MessageTypeRegistryOptions.AllowAssemblyQualifiedNameFallback`. Source-compatible.

---

## v0.1.0 — Foundation *(planned, Q3 2026)*

The first release. Enough to enqueue messages inside an EF Core `SaveChanges` transaction
and dispatch them at-least-once to a pluggable sink, with retries, dead-letter, and
OpenTelemetry.

- `IOutbox` with `Enqueue<T>(T message, OutboxEnqueueOptions? options)` — scoped to the
  user's `DbContext`, flushed by a `SaveChangesInterceptor` so outbox rows commit in the
  same transaction as the user's domain data.
- `IOutboxSink` — the single sink contract called per envelope by the dispatcher.
- `OutboxDispatcherHostedService` — background loop: claim a batch, dispatch each
  envelope, complete or fail. Single instance per process; competing-consumers safe
  across replicas via the storage backend's claim primitive.
- Retry with exponential backoff (default 1 s → 5 s → 30 s → 5 min → 30 min). Dead-letter
  after `MaxAttempts` (default 5).
- `ChannelOutboxSink` — built-in in-process sink backed by `System.Threading.Channels`.
  Useful for monoliths and tests; zero external dependency.
- `OrionPatch.EntityFrameworkCore` — `IOutboxStorage` over EF Core; `OrionPatch_Outbox`
  table; provider-aware claim (`SKIP LOCKED` on SQL Server 2022+, PostgreSQL 9.5+,
  MySQL 8.0+; optimistic-concurrency fallback for SQLite).
- `OrionPatch.Testing` — in-memory storage, deterministic dispatcher (no background
  thread), capturing sink, fluent assertions.
- OpenTelemetry `ActivitySource` and `Meter` (`Moongazing.OrionPatch`) — spans, counters,
  histogram, observable queue-depth gauge.
- `AddOrionPatch()` DI with `UseEntityFrameworkCore<TDbContext>()` / `UseInMemory()` /
  `UseSink<T>()` / `UseChannelSink(...)`.

---

## v0.2.0 — Schema-evolution helper *(shipped 2026-06-01)*

First slice of the v0.2.x line. Schema-evolution-safe wire names so consumers can rename
or refactor message types without breaking in-flight outbox rows.

- **`MessageTypeRegistry`** - bidirectional logical-name <-> CLR-type mapping. Built via `MessageTypeRegistryBuilder` and registered with `services.AddOrionPatch().UseMessageTypeRegistry(...)`. Backed by a `FrozenDictionary` so look-ups are allocation-free.
- **`MessageTypeRegistryOptions.AllowAssemblyQualifiedNameFallback`** (default `true`) - controls whether unmapped CLR types fall back to `Type.FullName` or throw.

### Deferred from v0.2.0 to follow-up patches

The original v0.2.0 milestone listed four other items. New targets:

- **`IOutboxTenantResolver`** (multi-tenant outbox filtering) -> v0.2.1. The documented `Headers["tenant-id"]` workaround stays supported.
- **`OrionPatch.Inbox`** (sibling storage primitive for consumer-side dedup) -> v0.2.2.
- **`OrionPatch.RabbitMQ`** sink (publisher path) -> v0.2.4 (shipped 2026-06-10); consumer / subscription side -> v0.2.5 (shipped 2026-06-10).
- **`OrionPatch.AzureServiceBus`** sink -> v0.2.6 (bumped one minor to make room for the RabbitMQ consumer).

---

## v0.3.0 — Push-based dispatch *(planned, Q1 2027)*

The 1-second polling loop is fine for busy services and wasteful for idle ones. Push
removes the latency floor and the wasted queries.

- **`OrionPatch.EntityFrameworkCore.Postgres.ListenNotify`** — opt-in push using
  PostgreSQL `LISTEN/NOTIFY`. Falls back to polling if the channel is unreachable.
- **`OrionPatch.EntityFrameworkCore.SqlServer.ServiceBroker`** — opt-in push using SQL
  Server Service Broker, same fallback contract.
- **`OrionPatch.Kafka`** — concrete sink, deferred from v0.2 because the Kafka producer
  contract is meaningfully different from broker queues.
- **`OrionPatch.Nats`** — concrete sink, JetStream-aware.
- **Concurrency stress harness** — multi-process integration test that runs N dispatchers
  against a shared backend and asserts at-least-once delivery and exclusive claim under
  contention. Catches regressions in the lease/renewal paths.

---

## v0.4.0 — Operator dashboard & schema evolution *(planned, Q1-Q2 2027)*

The first release that goes beyond "primitive that works" into operational quality of life.

- **`OrionPatch.Dashboard`** — embeddable ASP.NET Core dashboard for dead-letter
  inspection, per-message-type counts, retry triggers. Same precedent as
  `OrionAudit.Viewer`. Optional package; the core library does not depend on it.
- **Schema-evolution helpers** — `EnvelopeRehydrator` for upcasting old payloads on the
  consumer side; documented patterns for adding nullable columns to `OrionPatch_Outbox`
  without breaking in-flight rows.
- **Health-check helper** — `IHealthCheck` that surfaces storage backend reachability so
  consumers can fail-fast in container probes.
- **Richer telemetry** — per-sink latency histogram, dead-letter-rate counter, lease-loss
  counter, and documented dashboards for Grafana and Application Insights.

---

## v1.0.0 — Stable API *(planned, Q2 2027)*

The 1.0 release is a commitment: we stop changing public types and method signatures
inside the 1.x line. Anything obsolete by then is removed; everything that remains is
stable.

- **API stability** — `IOutbox`, `IOutboxSink`, `IOutboxStorage`, `OutboxEnvelope`,
  `OrionPatchOptions`, and every shipped sink and storage backend freeze. Additions only.
- **Documentation pass** — every public type has a runnable example. At-least-once
  pitfalls documented exhaustively. Migration guide from any breaking change introduced
  in 0.x.
- **AOT readiness audit** — every reflection path annotated; trimmer-safe by default.
- **`OrionPatch.Testing` polish** — scenario builders for dead-letter, lease loss,
  competing-consumers contention, mid-dispatch crash.

---

## Considered (no commitment yet)

- **`OrionPatch.Cosmos`** — Cosmos DB change-feed-backed storage backend.
- **`OrionPatch.MongoDb`** — MongoDB transactions + change-streams storage backend.
- **Bulk-enqueue API** — `IOutbox.EnqueueMany<T>(IEnumerable<T>)` for cases where the
  current per-call API materializes too many list copies.
- **`OrionAudit.OrionPatch` bridge** — adapter so OrionAudit's planned
  `IAuditEventPublisher` hook routes audit events through OrionPatch without consumer
  glue code. Tracked in OrionAudit v0.7.0 spec.

If any of the above maps to a real workload you are on right now, open an issue with the
`roadmap` label and a short description — that is how items move from *considered* to *planned*.

---

## Out of scope for the 1.x line

- **A bundled broker.** OrionPatch will never ship a transport. The sink is always
  pluggable. RabbitMQ, Service Bus, Kafka, NATS live in opt-in sub-packages.
- **An in-process pub/sub framework.** `ChannelOutboxSink` is a primitive, not a
  competitor to `MediatR` notifications. Use the right tool for the layer.
- **A mediator / saga / process manager.** That is OrionFlow territory; OrionPatch
  exposes the outbox primitive and stays out of the orchestration layer.
- **Distributed transactions across sinks.** OrionPatch dispatches to one sink with
  at-least-once. Two-phase commit across heterogeneous resources is not in scope.
- **Exactly-once delivery.** At-least-once is the contract. The combination of
  OrionPatch.Outbox + OrionPatch.Inbox + idempotent consumer logic is the path to
  effectively-once. We will not invent a stronger guarantee than the underlying broker
  can provide.

---

## How to influence priority

- **Open an issue** with the `roadmap` label and describe your use case. Real workload
  demand bumps items up.
- **Reference OrionPatch in a public project**, and let us know. Adoption signal matters.
- **Send a focused PR** for a *Considered* item with a concrete design. We will prioritise
  reviewing it.

Dates are targets, not commitments. If a milestone date slips by more than four weeks, the
delay shows up here.
