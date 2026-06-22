# OrionPatch Roadmap

This document lists what is shipped, what is actively planned, and what we are deliberately
*not* building. It is a planning artifact, not a contract - dates slip, priorities reshuffle.
If an item here matters to you, open a GitHub issue so we can weigh it against everything else.

## Status legend

- **Shipped** - in the named release on NuGet.
- **Planned** - committed to the named milestone; design is firm.
- **Considered** - interesting but unscheduled. Needs a concrete use case before we commit.
- **Out of scope** - explicitly declined for the 1.x line. The library stays small; some
  features belong in adjacent packages or in user code.

---

## Released

Current version: **0.3.1**. A transactional outbox primitive plus an idempotent inbox, three
broker sinks (Kafka, RabbitMQ, Azure Service Bus), a durable dead-letter store, retention-based
archival, and an OpenTelemetry surface. The full per-version history is in the
[CHANGELOG](CHANGELOG.md); the highlights below are the items worth knowing about at a glance.

- **v0.1.0** (2026-05-24) - Initial public release. `IOutbox` + `IOutboxSink` + EF Core storage + retry / in-place dead-letter / OpenTelemetry / in-process channel sink. Three NuGet packages: `OrionPatch`, `OrionPatch.EntityFrameworkCore`, `OrionPatch.Testing`.
- **v0.1.1** (2026-05-26) - Logo refresh. No functional change.
- **v0.2.0** (2026-06-01) - `MessageTypeRegistry` for schema-evolution-safe wire names + `MessageTypeRegistryOptions.AllowAssemblyQualifiedNameFallback`. Source-compatible.
- **v0.2.1** (2026-06-04) - `IOutboxTenantResolver` ambient tenant capture; `DelegateOutboxTenantResolver`. Caller-supplied `Headers["tenant-id"]` still wins.
- **v0.2.2 - v0.2.3** (2026-06-09) - `IInbox` consumer-side dedup primitive (`InMemoryInbox`, `InboxFilter`) and its EF Core storage (`OrionPatch_Inbox` table, composite key on message id + consumer).
- **v0.2.4 - v0.2.5** (2026-06-10) - `OrionPatch.RabbitMQ`: publisher sink with publisher confirms, then the inbox-deduped consumer (`RabbitMqOutboxConsumer`, QoS / requeue / DLX options).
- **v0.2.6** (2026-06-10) - `OrionPatch.AzureServiceBus` sink (queues and topics, `MessageId`-based duplicate detection, managed-identity friendly).
- **v0.2.7 - v0.2.8** (2026-06-10) - `OrionPatch.Kafka`: idempotent producer sink, then the inbound consumer (manual commit gated on handler success, `IInbox` dedup).
- **v0.2.9 - v0.2.12** (2026-06-11) - Kafka inbound poison-message handling: dead-letter-topic routing after `MaxDeliveryAttempts`, the `IKafkaAttemptCountStore` persistence hook, an EF Core-backed store, and an L1 in-memory cache decorator.
- **v0.2.13 - v0.2.30** (2026-06-11 - 2026-06-16) - Telemetry build-out. Kafka inbound counters/histograms; a `KafkaProducerHealthCheck`; and a wide set of dispatcher instruments (`batch_size`, `batch_fill_ratio`, `poll.duration`, `poll.idle`, `sink.duration_ms`, `queue_depth` gauge, `queue_lag`, `pickup_lag_ms`, `dead_letter.age_ms`, `attempts_per_row`, `envelope_bytes`) plus the `IDeadLetterSink` and `IOutboxDispatchObserver` extensibility hooks.
- **v0.2.22** (2026-06-11) - Fix: pack and publish the three sibling sink packages (`Kafka`, `RabbitMQ`, `AzureServiceBus`), which the release workflow had been omitting.
- **v0.3.0** (2026-06-19) - Outbox dead-letter **store** (`IDeadLetterStore`) and outbox **archival** (`IOutboxArchivalStore`). See the dedicated section below.
- **v0.3.1** (2026-06-20) - The Kafka inbound diagnostics `Meter` version now derives from the owning assembly's `AssemblyInformationalVersionAttribute` at runtime instead of a hardcoded string, so the metric version tracks the package version and no longer drifts on release.

---

## v0.1.0 - Foundation *(shipped 2026-05-24)*

The first release. Enough to enqueue messages inside an EF Core `SaveChanges` transaction
and dispatch them at-least-once to a pluggable sink, with retries, in-place dead-letter, and
OpenTelemetry.

- `IOutbox` with `Enqueue<T>(T message, OutboxEnqueueOptions? options)` - scoped to the
  user's `DbContext`, flushed by a `SaveChangesInterceptor` so outbox rows commit in the
  same transaction as the user's domain data.
- `IOutboxSink` - the single sink contract called per envelope by the dispatcher.
- `OutboxDispatcherHostedService` - background loop: claim a batch, dispatch each
  envelope, complete or fail. Single instance per process; competing-consumers safe
  across replicas via the storage backend's claim primitive.
- Retry with exponential backoff (default 1 s → 5 s → 30 s → 5 min → 30 min). Dead-letter
  after `MaxAttempts` (default 5).
- `ChannelOutboxSink` - built-in in-process sink backed by `System.Threading.Channels`.
  Useful for monoliths and tests; zero external dependency.
- `OrionPatch.EntityFrameworkCore` - `IOutboxStorage` over EF Core; `OrionPatch_Outbox`
  table; provider-aware claim routing (SQL Server / PostgreSQL / MySQL through the
  `SkipLockedClaimStrategy`, SQLite and unknown providers through a portable compare-and-swap
  fallback). Native `SKIP LOCKED` SQL is still pending; see v0.4.0 below.
- `OrionPatch.Testing` - in-memory storage, deterministic dispatcher (no background
  thread), capturing sink, fluent assertions.
- OpenTelemetry `ActivitySource` and `Meter` (`Moongazing.OrionPatch`) - spans, counters,
  histogram, observable queue-depth gauge.
- `AddOrionPatch()` DI with `UseEntityFrameworkCore<TDbContext>()` / `UseInMemory()` /
  `UseSink<T>()` / `UseChannelSink(...)`.

---

## v0.2.0 - Schema-evolution helper *(shipped 2026-06-01)*

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

## v0.3.0 - Dead-letter store and archival *(shipped 2026-06-19)*

Two outbox-maintenance capabilities, both expressed as optional SPIs on the storage backend
rather than separate services: a storage type opts in by implementing the interface. The dispatcher
routes to the dead-letter SPI on a row's terminal path; archival is operator-invoked (see the archival
bullet below), not run by the dispatcher, so processed rows accumulate until an operator schedules it.
Storage that does not implement these keeps the prior behaviour, so both are backward compatible.

- **`IDeadLetterStore`** - when a row exhausts `MaxAttempts`, the dispatcher prefers to route it
  OUT of the hot outbox into a durable dead-letter store (appending a `DeadLetteredMessage`
  snapshot with the final failure context) instead of flipping it to `DeadLettered` in place.
  Routing is idempotent on the row id, so a crash-replayed terminal path lands the message
  exactly once. `GetDeadLetteredAsync` exposes the held messages for inspection and triage.
  Distinct from the v0.2.18 `IDeadLetterSink` observer: the sink notifies, the store durably owns.
- **`IOutboxArchivalStore`** - `ArchiveProcessedAsync(retention, nowUtc, ct)` reaps `Processed`
  rows past the retention window out of the active outbox so claim-query planning stays healthy.
  Pending / Claimed / DeadLettered rows and processed rows still inside the window are never
  touched; the reap is idempotent and incremental. `OrionPatchOptions.ArchiveRetention`
  (default 7 days) sets the horizon. Operator-invoked: OrionPatch does not start a background
  reaper, so it is called from the consumer's own scheduled job.
- Implemented on the bundled `InMemoryOutboxStorage` (archive or purge mode). The EF Core
  backend does not implement either SPI yet - that is the first v0.3.2 item below.

---

## v0.3.2 - Dead-letter and archival on the EF Core backend *(planned, Q3 2026)*

v0.3.0 shipped the `IDeadLetterStore` and `IOutboxArchivalStore` SPIs and implemented them on
the in-memory testing storage only. This release brings both to the production EF Core backend
so deployments get the durable dead-letter destination and the retention reaper without writing
their own storage.

- **`EfCoreOutboxStorage : IDeadLetterStore`** - routes an exhausted row into a sibling
  `OrionPatch_DeadLetter` table inside the same transaction as the source-row delete, so the
  move is atomic and exactly once. `GetDeadLetteredAsync` reads back with paging (the in-memory
  snapshot signature returns everything; the relational read needs a bounded query).
- **`EfCoreOutboxStorage : IOutboxArchivalStore`** - `ArchiveProcessedAsync` as a set-based
  `ExecuteDelete` / `ExecuteUpdate` over `Processed` rows past the retention cutoff, in batches
  so a large backlog does not take one long lock. Archive target is a `OrionPatch_OutboxArchive`
  table; purge mode skips it and just deletes.
- **EF Core migration guidance** - the runtime never creates tables; documented `dotnet ef`
  steps and the `ApplyOrionPatchConfiguration()` additions for the two new tables.

---

## v0.3.3 - Dead-letter replay and operator tooling *(planned, Q3 2026)*

A dead-lettered message is currently a terminal record: `GetDeadLetteredAsync` can read it, but
there is no supported path to put a fixed message back into the active outbox. This release
closes that loop, which is the most-requested operation once a dead-letter store exists.

- **Replay / redrive API** - `IDeadLetterStore.RedriveAsync(messageId, ...)` re-enqueues a
  dead-lettered message as a fresh `Pending` outbox row (new row id, attempt count reset,
  original payload / headers / correlation id preserved, a `redriven-from` header stamped for
  traceability) and removes it from the dead-letter store in one transaction. Idempotent on the
  dead-letter id so a double-click or retried call does not enqueue twice.
- **Bulk redrive** - redrive-by-predicate (message type, dead-letter window) for recovering a
  whole class of failures after a downstream outage is resolved, batched and resumable.
- **Replay metrics** - `orionpatch.outbox.dead_letter.redriven` counter and a dead-letter
  depth gauge so operators can see the backlog drain.
- **Tooling** - a minimal CLI / `IHostedService` maintenance surface plus `OrionPatch.Testing`
  scenario helpers (`AssertRedriven`) so the redrive path is covered like the dispatch path.

---

## v0.4.0 - Dispatch performance and ordering *(planned, Q4 2026)*

The dispatcher today polls on an interval and claims unordered batches. This release tackles the
two longest-standing performance items and adds opt-in ordering for consumers that need it.

- **Native `SKIP LOCKED` claim** - `SkipLockedClaimStrategy` currently delegates to the portable
  compare-and-swap fallback. Land the real provider SQL (`FOR UPDATE SKIP LOCKED` + `RETURNING`
  on PostgreSQL, the `OUTPUT` / readpast equivalent on SQL Server, the two-statement MySQL form)
  so high-contention multi-dispatcher deployments stop wasting claim round-trips.
- **Push-based dispatch** - opt-in `OrionPatch.EntityFrameworkCore.Postgres.ListenNotify`
  (PostgreSQL `LISTEN/NOTIFY`) and `...SqlServer.ServiceBroker`, each falling back to polling if
  the channel is unreachable. Removes the polling latency floor and the idle-poll cost for
  low-traffic services (the `poll.idle` counter quantifies that cost today).
- **Partitioned / ordered dispatch** - opt-in per-key ordering (`OutboxEnqueueOptions.PartitionKey`)
  so messages sharing a key dispatch in enqueue order while distinct keys still dispatch
  concurrently. At-least-once is unchanged; this constrains ordering, not delivery count.
- **Concurrency stress harness** - multi-process integration test running N dispatchers against
  one backend, asserting at-least-once delivery and exclusive claim under contention. Guards the
  lease / renewal and the new native-claim paths against regressions.

---

## v0.4.1 - Operator surface *(planned, Q1 2027)*

Quality-of-life for running an outbox in production, building on the v0.3.x dead-letter and
archival work.

- **Dispatcher health check** - `IHealthCheck` that surfaces storage-backend reachability and
  dispatcher liveness (last successful poll, queue depth trend) so a container probe fails fast
  before the backlog grows. Pairs with the existing `KafkaProducerHealthCheck`.
- **`OrionPatch.Dashboard`** - optional embeddable ASP.NET Core dashboard for dead-letter
  inspection, per-message-type counts, and one-click redrive (built on the v0.3.3 API). Same
  precedent as `OrionAudit.Viewer`; the core library never depends on it.
- **Versioned-payload / schema-evolution helpers** - an `EnvelopeRehydrator` for upcasting old
  payloads on the consumer side and a documented payload-version header convention, complementing
  the existing `MessageTypeRegistry` for wire-name evolution.
- **Inbox hardening pass** - TTL / compaction for the inbox dedup table so it does not grow
  unbounded, plus documented guidance on inbox-id selection and retention so effectively-once
  stays correct over long-running deployments.

---

## v1.0.0 - Stable API *(planned, Q2-Q3 2027)*

The 1.0 release is a commitment: we stop changing public types and method signatures
inside the 1.x line. Anything obsolete by then is removed; everything that remains is
stable.

- **API stability** - `IOutbox`, `IOutboxSink`, `IOutboxStorage`, `IDeadLetterStore`,
  `IOutboxArchivalStore`, `OutboxEnvelope`, `OrionPatchOptions`, and every shipped sink and
  storage backend freeze. Additions only.
- **Documentation pass** - every public type has a runnable example. At-least-once
  pitfalls documented exhaustively. Migration guide from any breaking change introduced
  in 0.x.
- **AOT readiness audit** - every reflection path annotated; trimmer-safe by default.
- **`OrionPatch.Testing` polish** - scenario builders for dead-letter, redrive, lease loss,
  competing-consumers contention, mid-dispatch crash.

---

## Considered (no commitment yet)

- **`OrionPatch.Nats`** - JetStream-aware sink. Deferred from the original v0.3 plan; no
  blocking demand yet.
- **`OrionPatch.Cosmos`** - Cosmos DB change-feed-backed storage backend.
- **`OrionPatch.MongoDb`** - MongoDB transactions + change-streams storage backend.
- **Bulk-enqueue API** - `IOutbox.EnqueueMany<T>(IEnumerable<T>)` for cases where the
  current per-call API materializes too many list copies.
- **`OrionAudit.OrionPatch` bridge** - adapter so OrionAudit's planned
  `IAuditEventPublisher` hook routes audit events through OrionPatch without consumer
  glue code. Tracked in OrionAudit v0.7.0 spec.

If any of the above maps to a real workload you are on right now, open an issue with the
`roadmap` label and a short description - that is how items move from *considered* to *planned*.

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
