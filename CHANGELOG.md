# Changelog

All notable changes to OrionPatch are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/) and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.2] - 2026-06-22

### Added

#### Dead-letter and archival on the EF Core backend

v0.3.0 shipped the `IDeadLetterStore` and `IOutboxArchivalStore` SPIs but implemented them only on the in-memory testing storage; on the EF Core backend dead-lettering fell back to the in-place status flip and archival was unavailable. This release brings both SPIs to the production EF Core backend (`OrionPatch.EntityFrameworkCore`), so deployments get the durable dead-letter destination and the retention reaper without writing their own storage. Purely additive: existing public APIs and their behavior are unchanged.

- `EfCoreOutboxStorage` now also implements `IDeadLetterStore` and `IOutboxArchivalStore` in addition to `IOutboxStorage`.
- `IDeadLetterStore.DeadLetterAsync(rowId, DeadLetterContext, ct)` deletes the source row from `OrionPatch_Outbox` and inserts a snapshot into the new `OrionPatch_DeadLetter` table inside a single explicit transaction, so the move is atomic. Routing is idempotent on the dead-letter primary key (the source row id): a replayed terminal path either finds the row already routed (pre-check) or loses the insert race and trips the primary-key constraint (caught), and both report the exactly-once no-op. Matches the in-memory store's semantics exactly.
- `GetDeadLetteredAsync(ct)` reads back the most recent dead-letters (newest first, bounded). A new `GetDeadLetteredAsync(skip, take, ct)` overload pages over a large backlog for triage tooling, which the SPI's unbounded snapshot signature cannot express on a relational store.
- `IOutboxArchivalStore.ArchiveProcessedAsync(retention, nowUtc, ct)` reaps `Processed` rows whose `ProcessedAtUtc <= nowUtc - retention` out of the active outbox in bounded batches (so a large backlog does not take one long lock) and returns the count moved. Archive mode copies reaped rows into the new `OrionPatch_OutboxArchive` table before deleting them; purge mode deletes outright and leaves the archive empty. The cutoff is inclusive and the reap is idempotent and incremental, matching the in-memory store. Deletes target an explicit id set rather than `DELETE ... LIMIT`, so the reap is portable across providers.
- Archive vs purge mode is selected at registration via a new `UseEntityFrameworkCore<TDbContext>(purgeOnArchive: true)` overload; the parameterless overload keeps the archive-mode default. The same scoped `EfCoreOutboxStorage` instance is now also resolvable as `IDeadLetterStore` and `IOutboxArchivalStore`. As before, archival is operator-invoked from the consumer's own scheduled job; OrionPatch does not start a background reaper.
- New entities and configurations wired into `ApplyOrionPatchConfiguration()`: `DeadLetterRow` (`OrionPatch_DeadLetter`, keyed on the source row id, indexed by `DeadLetteredAtUtc`) and `OutboxArchiveRow` (`OrionPatch_OutboxArchive`, indexed by `ProcessedAtUtc`). The runtime never creates tables; regenerate a migration (`dotnet ef migrations add OrionPatch_v0_3_2_DeadLetterAndArchive`) and apply it. The new tables are inert until the dead-letter store or the reaper is invoked, and the migration is additive (no existing column or index changes).

## [0.3.1] - 2026-06-20

### Changed

- The Kafka inbound diagnostics `Meter` version is now derived from the owning assembly's `AssemblyInformationalVersionAttribute` at runtime instead of a hardcoded `"0.2.14"` string, so the metric version tracks the package version automatically and no longer drifts on release. The resolver reads only its own assembly's version.

## [0.3.0] - 2026-06-19

### Added

#### Outbox dead-letter store (`IDeadLetterStore`)

A new core abstraction routes messages that exhaust their delivery budget OUT of the hot outbox into a dedicated dead-letter store, carrying their final failure context, instead of retrying forever or silently dropping them.

- `IDeadLetterStore.DeadLetterAsync(rowId, DeadLetterContext, ct)` removes the source row from the active outbox and appends a `DeadLetteredMessage` snapshot (payload, headers, correlation id, enqueue time, total attempt count, final error, dead-letter instant).
- Routing is idempotent on the row id: a redelivered or crash-replayed terminal-path call for an already-routed row is a no-op, so a message lands in the dead-letter store EXACTLY ONCE and is never reclaimed or retried again (the source row is gone from the claim set).
- This complements, and is distinct from, the v0.2.18 `IDeadLetterSink` observer: the sink is a fire-and-forget triage notification (Slack, PagerDuty), the store is the durable destination the message is moved into.
- Both `OutboxDispatcherHostedService` and the test `DeterministicDispatcher` now PREFER routing into an `IDeadLetterStore` when the storage implements it, and fall back to the in-place `OutboxStatus.DeadLettered` status flip otherwise (fully backward compatible for storage that does not implement the new interface).
- New types: `IDeadLetterStore`, `DeadLetteredMessage`, `DeadLetterContext`.

#### Outbox archival (`IOutboxArchivalStore`)

A new core abstraction reaps successfully dispatched (`Processed`) rows out of the hot outbox once they cross a retention window, so the active outbox stays small and claim-query planning stays healthy.

- `IOutboxArchivalStore.ArchiveProcessedAsync(retention, nowUtc, ct)` moves every `Processed` row whose `ProcessedAtUtc <= nowUtc - retention` out of the active outbox and returns the number reaped. Pending, Claimed, and DeadLettered rows are never touched; processed rows still inside the retention window are never touched. The cutoff is inclusive and the reap is idempotent and incremental.
- The in-memory storage supports both an ARCHIVE mode (default, moved rows observable via `GetArchivedAsync`) and a PURGE mode (`new InMemoryOutboxStorage(purgeOnArchive: true)`, moved rows discarded).
- New `OrionPatchOptions.ArchiveRetention` (default 7 days, validated non-negative) expresses the retention horizon for operators.
- New types: `IOutboxArchivalStore`.

#### In-memory store implements both capabilities

`InMemoryOutboxStorage` now implements `IDeadLetterStore` and `IOutboxArchivalStore` in addition to `IOutboxStorage`, with all transitions serialized through the existing per-instance lock. New inspection accessors: `DeadLetteredMessages`, `ArchivedRows`.

### Tests

- `InMemoryDeadLetterStoreTests`: routes a row out of the active outbox with full failure context; idempotent on repeat calls (exactly once, first context wins); returns false for missing rows; rejects empty error text; a dead-lettered row is no longer claimable.
- `InMemoryArchivalStoreTests`: archives only processed rows past retention; never touches Pending/Claimed/DeadLettered; inclusive cutoff boundary; idempotent across passes; purge mode discards without archiving; throws on negative retention.
- `DeterministicDispatcherTests`: a row exceeding `MaxAttempts` is routed to the dead-letter store exactly once, preserves its failure context, and is not retried on subsequent passes.

## [0.2.32] - 2026-06-17

### Changed
- Set the NuGet package icon to the navy Moongazing mark and the README logo to the white Moongazing mark.

## [0.2.31] - 2026-06-17

### Changed
- Updated the package icon and README logo to the new Moongazing mark.

## [0.2.30] - 2026-06-16

### Added

#### `orionpatch.outbox.dispatch.pickup_lag_ms` histogram

A new `Histogram<double>` records dispatcher PICKUP lag: the gap between `OutboxRow.EnqueuedAtUtc` and the moment the dispatcher begins a row's FIRST dispatch attempt.

- Where the v0.2.21 `queue_lag` measures end-to-end latency to successful dispatch (and so folds in every retry and backoff a row incurred), this isolates how long a row waited to be picked up at all: the dispatcher's polling plus claim responsiveness, before any sink work.
- Recorded once per row, on the first attempt only, so retries do not re-record a pickup that already happened. A row that fails its first attempt still records here because the pickup did occur.
- Recorded before the sink call, so a slow or failing sink does not inflate it.
- Negative inputs are clamped to 0 so clock skew across enqueue and dispatcher hosts does not pull the histogram p50 down.
- Comparing `pickup_lag_ms` p99 against `queue_lag` p99 decomposes the latency budget: a large pickup lag points at `PollingInterval` / `BatchSize` / claim contention, while a small pickup lag with a large queue lag points at retry-and-sink time dominating.

### Fixed

- The bundled outboxes (`EfCoreOutbox`, `InMemoryOutbox`) now stamp the real write time into `OutboxRow.EnqueuedAtUtc` instead of copying the (possibly backdated) `OccurredAtUtc` into it (codex). A caller that backdates `OutboxEnqueueOptions.OccurredAtUtc` to reflect when a domain event happened no longer inflates the enqueue-based telemetry: the new `pickup_lag_ms` and the v0.2.29 `dead_letter.age_ms` now measure outbox dwell rather than the event backdate, and FIFO claim ordering (which sorts by `EnqueuedAtUtc`) reflects actual enqueue order rather than the backdate.

### Tests

- `PickupLagHistogramTests`: the helper emits for positive milliseconds and clamps negatives to 0.
- `OutboxDispatcherHostedServiceTests`: the dispatcher records pickup lag on the first attempt and does NOT record it when a row is dispatched on a later (retry) attempt.
- Serialized `QueueDepthGaugeTests` and `OutboxDispatcherHostedServiceTests` into one non-parallel collection. The gauge test reads the process-global queue-depth value while every hosted-service test writes it each poll cycle; running them in isolation removes a pre-existing cross-class race that the new dispatcher tests would otherwise widen.

## [0.2.29] - 2026-06-15

### Added

#### `orionpatch.outbox.dead_letter.age_ms` histogram

`Histogram<double>` records how long a row spent in the outbox before it was dead-lettered (the gap between `OutboxRow.EnqueuedAtUtc` and the moment it exhausted `MaxAttempts`). It is the failure-path analog to the v0.2.21 `queue_lag` success histogram.

- Operators graph p99 to tune the retry policy: a dead-letter age that tracks `MaxAttempts` x the backoff schedule means rows are exhausting genuine transient retries, whereas a much shorter age means rows are failing fast on terminal errors and the retry budget is being spent pointlessly.
- Recorded post-persist (after `DeadLetterAsync`), alongside the existing `deadlettered` counter, so a storage failure that leaves the row un-abandoned does not record a spurious age. Negative inputs are clamped to 0 for clock skew.
- Public `OrionPatchDiagnostics.RecordDeadLetterAge(double)` helper.

### Tests

- `DeadLetterAgeHistogramTests`: emits for a positive age, clamps a negative to 0.

## [0.2.28] - 2026-06-15

### Added

#### `orionpatch.outbox.poll.idle` counter

`Counter<long>` increments on each dispatcher cycle that claims an empty batch (the backlog was empty). Operators graph the idle-poll rate against the total poll rate to right-size `PollingInterval`: a high idle fraction is a cost-of-poll signal, while a low fraction means the dispatcher is busy and `BatchSize` may need raising instead.

- Pairs with the v0.2.16 `batch_size` histogram, which deliberately skips these zero-row cycles.
- Public `OrionPatchDiagnostics.RecordIdlePoll()` helper.
- Mirrors the Guard v6.5.17 `poll.idle` counter on the Patch side.

### Tests

- `IdlePollCounterTests`: `RecordIdlePoll` increments the counter.

## [0.2.27] - 2026-06-13

### Added

#### `orionpatch.outbox.claim.batch_fill_ratio` histogram

`Histogram<double>` of claimed rows / configured `BatchSize` as a 0..1 ratio. Operators graph p99 to right-size `BatchSize` WITHOUT knowing the configured value out-of-band:

- Ratio near 1.0 = the dispatcher is BatchSize-bound (raise it for throughput).
- Ratio near 0 = BatchSize is over-provisioned for the actual backlog.

The absolute v0.2.16 `batch_size` histogram cannot answer this on its own (a p99 of 80 means nothing without knowing whether BatchSize is 100 or 1000). Only non-empty claims emit; over-claims are clamped to 1.0.

### Tests

4 facts.

### Migration from v0.2.26

Source-compatible.

## [0.2.26] - 2026-06-12

### Added

#### `orionpatch.outbox.queue_depth` ObservableGauge

`ObservableGauge<long>` reports pending outbox rows awaiting dispatch, as last observed by the dispatcher via `IOutboxStorage.QueueDepthAsync`. Operators alert on sustained growth (dispatcher cannot keep up with producers). Mirrors the OrionAudit `capture.queue_depth` shape so both outbox families expose the same liveness gauge.

- Snapshotted on EVERY cycle including zero-row (the v0.7.23 Audit lesson: non-empty-only snapshots leave the gauge stale).
- 0 until the first dispatch cycle completes.
- Public `OrionPatchDiagnostics.SetQueueDepth(long)` for consumer-owned dispatchers.

### Tests

1 fact (x2 TFM).

### Migration from v0.2.25

Source-compatible.

## [0.2.25] - 2026-06-12

### Added

#### `orionpatch.outbox.sink.duration_ms` histogram

`Histogram<double>` measuring `IOutboxSink.SendAsync` wall-clock per envelope. Isolates the broker / downstream call cost from the existing `dispatch.duration` which covers the full `DispatchOneAsync` method (deserialise + envelope build + sink + complete + housekeeping).

- try/finally so a slow failing sink (broker timeout, downstream 5xx) still emits the sample - the most operator-relevant tail.
- Negative values clamped to 0 (clock-skew safety).
- Public `OrionPatchDiagnostics.RecordSinkDuration(double)` helper.

### Tests

2 facts.

### Migration from v0.2.24

Source-compatible.

## [0.2.24] - 2026-06-12

### Added

#### `orionpatch.outbox.dispatch.envelope_bytes` histogram

`Histogram<int>` of dispatched envelope payload byte size (the JSON `Payload` string length). Operators graph p99 to spot a message-type whose payload grew suddenly and to size storage column types / broker frame limits against actual byte shape.

- Recorded only on the success path (after `sink.SendAsync` + `storage.CompleteAsync` succeed) so failed dispatches do not skew the distribution.
- Zero/negative inputs ignored.

### Tests

2 facts.

### Migration from v0.2.23

Source-compatible.

## [0.2.23] - 2026-06-11

### Added

#### `orionpatch.outbox.attempts_per_row` histogram

`Histogram<int>` of how many attempts each row took before reaching its terminal state. Operators graph p99 to spot rows that burn most of `MaxAttempts` before stabilising:

- Tail rising = `BackoffStrategy` needs tuning or the sink is flapping under specific message types.
- p50 == 1 with p99 == MaxAttempts = healthy steady-state with rare hard-fails (dead-letters).

Recorded on BOTH terminal paths (success commit + dead-letter commit) so the distribution covers the full row lifecycle, not just happy-path successes.

Public `OrionPatchDiagnostics.RecordAttemptsPerRow(int)` helper.

### Tests

2 facts.

### Migration from v0.2.22

Source-compatible.

## [0.2.22] - 2026-06-11

### Fixed

#### Publish missing sibling packages (Kafka, RabbitMQ, AzureServiceBus)

The CI/CD release workflow only packed `OrionPatch`, `OrionPatch.EntityFrameworkCore`, and `OrionPatch.Testing`. The sibling sink packages were never published despite `OrionPatch.EntityFrameworkCore` carrying a project reference to `OrionPatch.Kafka` (for `EfCoreKafkaAttemptCountStore`). Every downstream consumer hit `NU1101: Unable to find package OrionPatch.Kafka` on restore.

v0.2.22 adds the three missing packs so the transitive dependency resolves.

### Migration from v0.2.21

Source-compatible.

## [0.2.21] - 2026-06-11

### Added

#### `orionpatch.outbox.queue_lag` histogram

`Histogram<double>` of per-row dispatch lag (`OutboxRow.OccurredAtUtc` -> successful dispatch). Mirrors v6.5.16 OrionGuard `orionguard.outbox.dispatcher.queue_lag` for the Patch dispatcher. Operators graph p50/p99 to spot a backing-up queue BEFORE the steady-state dispatched-count rate visibly slows.

- Recorded AFTER `storage.CompleteAsync` (post-commit pattern) so a failed Complete does not cause a double-count when the row is re-dispatched on the next cycle.
- Negative values clamped to 0 (clock-skew safety).
- Public `OrionPatchDiagnostics.RecordQueueLag(double)` helper.

### Tests

2 facts.

### Migration from v0.2.20

Source-compatible.

## [0.2.20] - 2026-06-11

### Added

#### `IOutboxDispatchObserver` success-path extensibility

Consumer-supplied observer invoked AFTER a row is successfully dispatched (sink + storage complete). Mirror of v0.2.18 `IDeadLetterSink` on the success side. Useful for per-message audit trails or downstream fan-out for confirmed deliveries.

- `IOutboxDispatchObserver` interface in `Moongazing.OrionPatch.Abstractions`.
- `NullOutboxDispatchObserver` no-op default.
- New 7-arg ctor wires observer; 5/6-arg legacy ctors preserved.
- Observer fires AFTER `storage.CompleteAsync`; throwing observer does NOT roll back; exceptions counted via new `orionpatch.outbox.dispatch_observer_failures` and logged.

#### `orionpatch.outbox.dispatch_observer_failures` counter

`Counter<long>` tagged with `exception_type`. Operator-facing alert for misbehaving observers.

### Tests

2 facts.

### Migration from v0.2.19

Source-compatible.

```csharp
services.AddSingleton<IOutboxDispatchObserver, MyObserver>();
```

## [0.2.19] - 2026-06-11

### Added

#### `orionpatch.outbox.dead_letter_sink_failures` counter

`Counter<long>` increments each time the v0.2.18 `IDeadLetterSink` observer throws. The dead-letter database state is still applied (the sink is observability, not load-bearing) so this metric is purely operator-facing alerting for "your DLQ notifier is down".

- Tag: `exception_type`.
- Pairs with the existing structured log line - operators can both alert on rate AND grep the log for context.
- Public `OrionPatchDiagnostics.RecordDeadLetterSinkFailure(string)` helper.

### Tests

1 fact.

### Migration from v0.2.18

Source-compatible.

## [0.2.18] - 2026-06-11

### Added

#### `IDeadLetterSink` extensibility

Consumer-supplied observer invoked when an outbox row is dead-lettered. Useful for routing the envelope to an external triage system (Slack, PagerDuty, follow-up review queue) without baking the routing into the dispatcher.

- `IDeadLetterSink` interface in `Moongazing.OrionPatch.Abstractions`.
- `NullDeadLetterSink` default implementation.
- Optional 6-arg `OutboxDispatcherHostedService` ctor wires the sink; 5-arg legacy ctor still works.
- Sink invoked AFTER `storage.DeadLetterAsync` succeeds; sink exceptions are logged and swallowed (sink is observability, not load-bearing).
- Cancellation propagates.

### Tests

2 facts.

### Migration from v0.2.17

Source-compatible.

```csharp
services.AddSingleton<IDeadLetterSink, MyTriageSink>();
```

## [0.2.17] - 2026-06-11

### Added

#### `orionpatch.outbox.poll.duration` histogram

`Histogram<double>` measuring `IOutboxStorage.ClaimNextAsync` wall-clock per dispatcher cycle. Operators graph p99 to spot a storage backend that is slow to claim rows.

- ALL cycles emit (including zero-row) because poll latency is the signal itself.
- Recorded immediately after `ClaimNextAsync` returns, BEFORE the batch_size check.
- Public on `OrionPatchDiagnostics` so consumer-owned dispatchers can opt in.

### Tests

1 fact (x2 TFM).

### Migration from v0.2.16

Source-compatible.

## [0.2.16] - 2026-06-11

### Added

#### `orionpatch.outbox.batch_size` histogram

`Histogram<int>` on the existing `OrionPatchDiagnostics` Meter that records rows claimed per dispatcher cycle. Operators graph p99 to spot a dispatcher that is consistently maxing out `BatchSize` (a sign that throughput is bottlenecked and the batch should be raised) or staying near 0 (a sign that polling cadence is over-sized for the actual traffic).

- Recorded in `OutboxDispatcherHostedService` immediately after the storage `ClaimNextAsync` returns.
- Zero-row cycles do NOT emit so the histogram tail reflects actual produced batches, not idle polling.
- Public on `OrionPatchDiagnostics` so consumer-owned dispatchers can opt in.

### Tests

1 fact (x2 TFM).

### Migration from v0.2.15

Source-compatible.

## [0.2.15] - 2026-06-11

### Added

#### `KafkaProducerHealthCheck`

`IHealthCheck` that probes the configured Kafka broker by listing its metadata. Returns Healthy when the metadata call returns within `Timeout`, Unhealthy otherwise. Pairs with the outbound producer so the consumer ASP.NET Core `/health` probe downgrades when the broker becomes unreachable BEFORE the outbox starts piling up failed produces.

- `KafkaProducerHealthCheckOptions.Timeout` (default 3 seconds).
- Reuses `KafkaOutboxSinkOptions.BootstrapServers` so consumers do not configure connection details twice.
- Lazily initialises an `IAdminClient` and reuses it across probes; the client is disposed when the health check is disposed.
- `Microsoft.Extensions.Diagnostics.HealthChecks.Abstractions` package reference added (transitive cost minimal).

### Tests

3 facts (x2 TFM).

### Migration from v0.2.14

Source-compatible.

```csharp
services.AddHealthChecks()
    .AddCheck<KafkaProducerHealthCheck>("kafka-producer");
```

## [0.2.14] - 2026-06-11

### Added

#### Kafka inbound success-path telemetry

Two new instruments complete the `Moongazing.OrionPatch.Kafka.Inbound` Meter; operators now see attempt + DLQ + success on the same dashboard without log scraping.

- `orionpatch.kafka.inbound.processed` (`Counter<long>`): envelopes the handler returned successfully. Tagged with `topic`.
- `orionpatch.kafka.inbound.processing_duration_ms` (`Histogram<double>`): wall-clock per `handler.HandleAsync` call. Tagged with `topic`. Operators graph p99 to size handler timeouts.
- Recorded BEFORE `attemptStore.ClearAsync` (same pattern as v0.2.13 `RecordDlqRouted`) so a transient store outage during cleanup does not undercount the success metric.
- Public `RecordProcessed` / `RecordProcessingDuration` helpers.

### Tests

2 new facts (x2 TFM).

### Migration from v0.2.13

Source-compatible.

## [0.2.13] - 2026-06-11

### Added

#### Kafka inbound OTel counters

Two new counters exposed via the new `Moongazing.OrionPatch.Kafka.Inbound` Meter. Operators wire these into Grafana to visualise redelivery storms and per-route DLQ patterns instead of scraping log messages.

- `orionpatch.kafka.inbound.attempt_set` (`Counter<long>`): incremented on every `IKafkaAttemptCountStore.SetAsync` call from the inbound consumer. Tagged with `topic`.
- `orionpatch.kafka.inbound.dlq_routed` (`Counter<long>`): incremented once per envelope routed to the configured DLQ topic. Tagged with `topic` (source) AND `dlq` (destination) so multi-route deployments can split alarms.
- `KafkaInboundDiagnostics.RecordAttemptSet` / `RecordDlqRouted` public so consumer-owned DLQ producers can opt in.

### Tests

2 new facts (x2 TFM).

### Migration from v0.2.12

Source-compatible.

## [0.2.12] - 2026-06-11

### Added

#### `MemoryCacheKafkaAttemptCountStore` L1 wrapper

Decorator that fronts a slower persistent inner `IKafkaAttemptCountStore` (v0.2.11 `EfCoreKafkaAttemptCountStore`, future Redis impl) with an in-memory write-through cache. The hot-path read on every failure no longer round-trips to the database, but the count stays restart-survivable because the inner store sees every write.

- `MemoryCacheKafkaAttemptCountStore(IKafkaAttemptCountStore inner)`.
- `GetAsync` reads cache first; on miss forwards to inner and populates the cache with the result. Does NOT cache zero so concurrent inner writes become visible.
- `SetAsync` writes to inner FIRST then the cache so a transient inner failure does not leave the cache ahead of the truth.
- `ClearAsync` evicts the cache before forwarding to the inner so a concurrent redelivery sees the inner truth rather than a stale cache hit.
- No TTL: counts are write-driven (every failure / success ticks the value) so stale entries naturally refresh.

### Tests

6 new facts.

### Migration from v0.2.11

Source-compatible.

```csharp
services.AddSingleton<EfCoreKafkaAttemptCountStore<AppDbContext>>();
services.AddSingleton<IKafkaAttemptCountStore>(sp =>
    new MemoryCacheKafkaAttemptCountStore(sp.GetRequiredService<EfCoreKafkaAttemptCountStore<AppDbContext>>()));
```

## [0.2.11] - 2026-06-11

### Added

#### `EfCoreKafkaAttemptCountStore<TDbContext>` - EF Core-backed attempt persistence

Builds on the v0.2.10 `IKafkaAttemptCountStore` abstraction. v0.2.10 shipped the contract + an in-memory default; v0.2.11 ships the EF Core-backed implementation so production deployments get restart-survivable DLQ routing.

- `EfCoreKafkaAttemptCountStore<TDbContext>` resolves a fresh DbContext per call from `IServiceScopeFactory` and persists per-envelope attempt counts in a `KafkaInboundAttempt` entity (one row per envelope id, `AttemptCount` + `LastUpdatedUtc`).
- `KafkaInboundAttempt.Configure(ModelBuilder)` convenience helper for the default mapping (table `OrionPatchKafkaInboundAttempts`, PK on `EnvelopeId`).
- Lives in `Moongazing.OrionPatch.EntityFrameworkCore` so it co-locates with the existing v0.2.x EF Core storage; that package now project-references `Moongazing.OrionPatch.Kafka`.

### Tests

6 new facts (x3 TFM).

### Migration from v0.2.10

Source-compatible. Three wiring steps required to flip on persistent counters:

1. Register the entity in the DbContext (the convenience helper covers the default mapping):

```csharp
protected override void OnModelCreating(ModelBuilder modelBuilder)
{
    base.OnModelCreating(modelBuilder);
    KafkaInboundAttempt.Configure(modelBuilder);
}
```

2. Generate + apply an EF Core migration. The runtime does NOT auto-create the table - production deployments use the consumer's migrations pipeline:

```bash
dotnet ef migrations add AddKafkaInboundAttempts
dotnet ef database update
```

3. Register the store + the inbound consumer:

```csharp
services.AddSingleton<IKafkaAttemptCountStore, EfCoreKafkaAttemptCountStore<AppDbContext>>();
services.AddOrionPatchKafkaInbox<MyHandler>(o => { o.BootstrapServers = "..."; o.DeadLetterTopic = "orders.dlq"; o.MaxDeliveryAttempts = 5; });
```

## [0.2.10] - 2026-06-11

### Added

#### `IKafkaAttemptCountStore` persistence hook

v0.2.9 introduced DLQ routing but kept the per-envelope attempt counter in memory only - a consumer restart reset the count and a poison-pill envelope could escape DLQ routing by surviving across restarts. v0.2.10 lets the consumer register a persistent store so the counter survives restarts and DLQ routing becomes restart-survivable.

- `IKafkaAttemptCountStore` abstraction: `GetAsync`, `SetAsync`, `ClearAsync`.
- `InMemoryKafkaAttemptCountStore` default implementation (preserves the v0.2.9 best-effort behaviour) registered automatically by `AddOrionPatchKafkaInbox` via `TryAddSingleton` so consumers wiring an EF Core / Redis-backed store win without explicit removal.
- The hosted service now reads the previous attempt count from the store at the start of failure handling and persists the updated count before evaluating `MaxDeliveryAttempts`. On successful handle / DLQ routing the store is cleared so a future re-delivery starts at 0 again.

### Tests

2 new facts: attempt count persisted on failure + cleared on success, DLQ uses persisted attempt count across simulated restarts. 23 facts (x3 TFM).

### Migration from v0.2.9

Source-compatible. Default in-memory store preserves v0.2.9 behaviour.

## [0.2.9] - 2026-06-11

### Added

#### Kafka inbound dead-letter topic routing

Extends the v0.2.8 inbound consumer. v0.2.8 redelivered failed messages indefinitely (handler failure -> rollback + seek-back + Kafka redelivery). v0.2.9 adds a poison-pill protection: after `MaxDeliveryAttempts` failures, the record is routed to a configurable dead-letter topic and the original offset is committed.

- `KafkaInboxOptions.DeadLetterTopic` (nullable, default null = v0.2.8 behaviour) + `MaxDeliveryAttempts` (default 5).
- `IKafkaInboundDeadLetterProducer` consumer-supplied sink that the inbound service produces to when an envelope has failed `MaxDeliveryAttempts` times.
- DLQ record carries the original key + value + headers PLUS `orionpatch-dlq-original-topic` / `original-partition` / `original-offset` / `attempt-count` / `reason` diagnostic headers.
- Attempt counter is in-memory per envelope id (best-effort; resets on consumer restart). Production deployments needing transactional guarantees back the counter with the IInbox store.
- When DLQ routing is configured but no producer is registered, the service falls back to the v0.2.8 redeliver-forever path so the feature is safe to flip on incrementally.

### Tests

1 new fact: after 3 failures, the envelope is routed to the DLQ topic with the original-topic / attempt-count headers stamped. 21 facts total across the Kafka package (x3 TFM).

### Migration from v0.2.8

Source-compatible.

## [0.2.8] - 2026-06-10

### Added

#### Kafka inbound consumer (subscription side for `Moongazing.OrionPatch.Kafka`)

Closes the v0.2.7 deferral. v0.2.7 shipped the producer sink (publish to Kafka); v0.2.8 ships the consumer side so a service can also subscribe to OrionPatch envelopes from Kafka.

- **`KafkaInboundHostedService`** consumes records, extracts the OrionPatch envelope metadata from Kafka headers, dedups via the existing `IInbox` primitive, and dispatches to the registered handler. The handler runs inside a per-message DI scope so it can resolve scoped dependencies (DbContext, unit-of-work).
- **`IKafkaInboundHandler`** consumer contract: `HandleAsync(InboundKafkaMessage, CancellationToken)`. Throwing is the redelivery signal.
- **`InboundKafkaMessage(EnvelopeId, MessageType, CorrelationId, Payload, Headers, Topic, Partition, Offset)`** record captures both the OrionPatch envelope view and the Kafka coordinates so handlers can correlate with broker telemetry.
- **Commit semantics**: manual commits gated on handler success. `EnableAutoCommit` is forced to `false` in `DefaultKafkaConsumerFactory` so a crash mid-handler is replayed.
- **Failure path**: handler throws -> `IInbox.RollbackAsync` runs (so redelivery is not silently suppressed as a duplicate) AND the offset is NOT committed (so Kafka redelivers on the next consume).
- **Missing envelope id header**: record is dropped and committed (a record without `orionpatch-envelope-id` is not an OrionPatch envelope - re-attempting would just spin).
- **`IKafkaConsumerFactory`** abstraction + `DefaultKafkaConsumerFactory` (validates `BootstrapServers` and `GroupId` at construction).
- **`AddOrionPatchKafkaInbox<THandler>(configure)`** DI helper.

### Tests

7 new facts cover: handler dispatched + offset committed on success, duplicate envelope id skipped + still committed, handler failure triggers rollback + no commit, missing envelope-id header is dropped + committed, `DefaultKafkaConsumerFactory` rejects empty `BootstrapServers` and empty `GroupId`, DI helper registers hosted service + scoped handler. 18 facts total across the Kafka package (x3 TFM).

### Migration from v0.2.7

Source-compatible. Opt-in:

```csharp
services.AddSingleton<IInbox, InMemoryInbox>();
services.AddOrionPatchKafkaInbox<MyHandler>(o =>
{
    o.BootstrapServers = "kafka-1:9092";
    o.GroupId = "orders-consumer";
    o.Topics = new[] { "orders" };
});
```

## [0.2.7] - 2026-06-10

### Added

#### `Moongazing.OrionPatch.Kafka` (NEW PACKAGE) - publisher sink

Third broker sink. Implements `IOutboxSink` against Apache Kafka with idempotent producer mode.

- **`KafkaOutboxSink`** produces each envelope to the configured topic with the envelope id (Guid N format) as the Kafka message key by default, so partition affinity is stable per envelope. Override via `KeySelector` to route by aggregate id / tenant when partition ordering by that axis is more meaningful for the consumer.
- **`KafkaOutboxSinkOptions`**: `BootstrapServers`, `Topic`, optional `TopicSelector` (per-envelope routing), `KeySelector` (default = envelope id), `EnableIdempotence` (default `true`), `Acks` (default `All`; required when idempotence is on), `SendTimeout` (default 30 s; CancelAfter on the linked token so a hung produce does not stall the dispatcher).
- **Header stamping**: `orionpatch-envelope-id`, `orionpatch-message-type`, `orionpatch-correlation-id` (when present). Caller-supplied envelope `Headers` (W3C `traceparent` / `tracestate`, tenant id) flow through verbatim. Reserved `orionpatch-*` keys win over consumer overrides.
- **`IKafkaProducerFactory`** abstraction wraps `ProducerBuilder<string, byte[]>.Build()`. Production wires `DefaultKafkaProducerFactory` which lazily builds the producer on first use and reuses it for the factory's lifetime (Kafka producers are designed to be long-lived). `Flush(5s)` + `Dispose()` on factory disposal so buffered messages drain.
- **`AddOrionPatchKafkaSink(configure)`** DI helper registers the sink as singleton `IOutboxSink`. Configure delegate invoked exactly once (probe then transcribe onto registered options).

### Tests

11 facts (across 3 TFM): produces to configured topic with envelope id key, `TopicSelector` flows through, `KeySelector` flows through, `orionpatch-envelope-id` / `orionpatch-message-type` / `orionpatch-correlation-id` headers stamped, correlation header omitted when absent, caller headers propagate but reserved keys win, payload UTF-8 round-trip, DI configure invoked exactly once, DI registers sink as singleton `IOutboxSink`.

### Migration from v0.2.6

Source-compatible. Add-on is opt-in:

```csharp
services.AddOrionPatchKafkaSink(o =>
{
    o.BootstrapServers = "kafka-1:9092,kafka-2:9092";
    o.Topic = "orders";
    o.KeySelector = e => e.Headers?.GetValueOrDefault("aggregate-id") ?? e.Id.ToString("N");
});
```

## [0.2.6] - 2026-06-10

### Added

#### `Moongazing.OrionPatch.AzureServiceBus` (NEW PACKAGE) - publisher sink

Second broker sink. Lands the v0.2.4 deferral. Implements `IOutboxSink` against Azure Service Bus queues and topics.

- **`AzureServiceBusOutboxSink`** publishes each envelope as a `ServiceBusMessage` to the configured `EntityPath` (queue or topic). `MessageId` is set to the envelope id (Guid N format) so Service Bus' built-in duplicate detection (when enabled on the entity) absorbs broker-side retries. `Subject` flows from `SubjectSelector` (default = envelope `MessageType`); topic subscriptions filter on Subject + ApplicationProperties.
- **Header stamping**: `ApplicationProperties["orionpatch-envelope-id"]`, `["orionpatch-message-type"]`, `["orionpatch-correlation-id"]` (when present). Caller-supplied envelope `Headers` (W3C `traceparent` / `tracestate`, tenant id) flow through verbatim. Reserved `orionpatch-*` keys win over consumer overrides so a malicious caller cannot hijack the envelope id.
- **`AzureServiceBusOutboxSinkOptions`** covers `ConnectionString` (optional - leave null when registering `ServiceBusClient` via Azure Identity / managed identity), `EntityPath` (default `"orionpatch"`), `SubjectSelector`, `ContentType` (default `"application/json"`), and `SendTimeout` (default 30 s; CancelAfter on the linked token so a hung SDK call does not stall the dispatcher).
- **`IServiceBusSenderFactory`** abstraction wraps `ServiceBusClient.CreateSender`. Production wires `DefaultServiceBusSenderFactory`; unit tests substitute mocks so the sink can be exercised without a real namespace.
- **`AddOrionPatchAzureServiceBusSink(configure)`** DI helper registers the sink as singleton `IOutboxSink`. Auto-wires a `ConnectionString`-backed `ServiceBusClient` when supplied; otherwise the caller registers `ServiceBusClient` themselves (Azure Identity / managed identity path).

### Tests

9 facts (across 3 TFM): publishes to configured entity with envelope MessageId, ApplicationProperties stamping for envelope id / message type, caller-supplied headers propagate but reserved keys win, correlation id stamping when present, SubjectSelector flows through to message subject, ContentType from options, payload round-trips through message body, SendTimeout cancels in-flight send when SDK hangs, DI registration returns the sink as singleton `IOutboxSink`.

### Migration from v0.2.5

Source-compatible. The sink is an opt-in add-on:

```csharp
services.AddOrionPatchAzureServiceBusSink(o =>
{
    o.ConnectionString = "Endpoint=sb://contoso.servicebus.windows.net/;...";
    o.EntityPath = "orders";
});
```

## [0.2.5] - 2026-06-10

### Added

#### `Moongazing.OrionPatch.RabbitMQ` consumer / subscription side

Lands the v0.2.4 deferral. Pairs with the v0.2.4 publisher path so a single package now provides end-to-end RabbitMQ wiring (publisher confirms on the way out; inbox-deduped delivery on the way in).

- **`RabbitMqOutboxConsumer`** `BackgroundService` drains the configured queue, decodes each AMQP delivery into an `OutboxEnvelope` (envelope id from the `orionpatch-envelope-id` header stamped by v0.2.4's `RabbitMqOutboxSink`), deduplicates via the registered `IInbox`, and invokes `IOrionPatchMessageHandler` for first deliveries only. ACK on success / duplicate ACK; NACK on handler exception.
- **Per-delivery scope**: each delivery resolves `IOrionPatchMessageHandler` and `IInbox` from a fresh `IServiceScope` so scoped collaborators (DbContext, repositories) behave as if served by an HTTP request. The scope is disposed inline with ACK / NACK.
- **QoS**: `RabbitMqOutboxConsumerOptions.PrefetchCount` (default 8) maps to `BasicQos(0, n, false)` so the broker does not push more than `n` un-acked deliveries to one consumer at a time. Tune up for low-latency / high-throughput handlers; tune down when handler latency is high so a slow consumer does not hog the queue.
- **Failure / requeue**: `RequeueOnFailure` (default `true`) controls the NACK `requeue` flag. Set false when paired with a dead-letter exchange so failures are captured for operator review instead of looping.
- **Duplicates**: `AckDuplicates` (default `true`) ACKs duplicates as a silent no-op; set false to NACK without requeue so the broker removes them via a configured DLX.
- **Missing envelope id** (delivery with no `orionpatch-envelope-id` header) is NACKed without requeue so an operator can inspect via DLQ; the consumer never silently drops a message it cannot dedupe.
- **`AddOrionPatchRabbitMqConsumer<THandler>(configure)`** DI helper registers the hosted service plus a scoped `IOrionPatchMessageHandler` binding.

### Tests

9 new `RabbitMqOutboxConsumerTests` facts (3 TFM): first delivery invokes + ACKs, duplicate ACKs without invoking, `AckDuplicates=false` NACKs without requeue, missing envelope id NACKs without requeue, handler exception NACKs with requeue by default, `RequeueOnFailure=false` NACKs without requeue, `BasicQos` set to configured prefetch, caller-supplied headers propagate excluding `orionpatch-*`, `AddOrionPatchRabbitMqConsumer` registers handler scoped (distinct instances per scope). 27 facts total in the RabbitMQ test suite (9 consumer + 10 v0.2.4 sink + 8 v0.2.4 sink supporting cases).

### Deferred

- **`OrionPatch.AzureServiceBus`** sink -> v0.2.6 (unchanged target)

### Migration from v0.2.4

Source-compatible. The consumer is an opt-in add-on registered alongside the v0.2.4 sink:

```csharp
services.AddOrionPatchRabbitMqSink(o =>
{
    o.ConnectionString = "amqp://guest:guest@localhost:5672/";
    o.ExchangeName = "orders";
});
services.AddOrionPatchRabbitMqConsumer<MyOrderHandler>(o =>
{
    o.QueueName = "orders.processor";
    o.PrefetchCount = 16;
});
```

## [0.2.4] - 2026-06-10

### Added

#### `Moongazing.OrionPatch.RabbitMQ` (NEW PACKAGE) - publisher sink

First broker sink. Implements `IOutboxSink` by publishing each envelope to a configurable RabbitMQ exchange with publisher confirms enabled. Subscription / consumer side stages to v0.2.5.

- **`RabbitMqOutboxSink`** opens one `IModel` lazily per sink instance and reuses it for the sink's lifetime; the underlying `IConnection` is owned by DI so multiple sinks can share one connection. Channel reopen is automatic when the previous channel closes.
- **`RabbitMqOutboxSinkOptions`** covers `ExchangeName` (default `orionpatch`), `RoutingKeySelector` (default = envelope's `MessageType`), `UsePublisherConfirms` (default `true`), `ConfirmTimeout` (default 10 s), `PersistentDelivery` (default `true`), and `ContentType` (default `application/json`). Optional `ConnectionString` triggers a built-in `IConnection` registration via `ConnectionFactory`.
- **Headers stamped**: `orionpatch-envelope-id` (Guid N format), `orionpatch-message-type`, `orionpatch-correlation-id` (when present). Caller-supplied envelope `Headers` (W3C `traceparent` / `tracestate`, tenant id) flow through verbatim; reserved `orionpatch-*` keys cannot be overridden.
- **Publisher confirms**: `WaitForConfirms(ConfirmTimeout)` after every publish; a non-ack throws so the outbox row stays unprocessed and the next dispatch cycle re-delivers. Set `UsePublisherConfirms = false` for fire-and-forget where lower per-message latency matters more than broker-side durability.
- **`AddOrionPatchRabbitMqSink(configure)`** DI helper registers the sink as singleton `IOutboxSink`; auto-wires a `ConnectionFactory`-backed singleton `IConnection` when `ConnectionString` is supplied.

### Deferred

- **`OrionPatch.RabbitMQ` consumer / subscription side** -> v0.2.5 (originally bundled in v0.2.4; staged so the publisher path ships now and the consumer gets a focused review with publish-side experience to ground the contract).
- **`OrionPatch.AzureServiceBus` sink** -> v0.2.6 (was v0.2.5; bumps one minor to make room for the RabbitMQ consumer)

### Migration from v0.2.3

Source-compatible. The sink is an opt-in add-on:

```csharp
services.AddOrionPatchRabbitMqSink(o =>
{
    o.ConnectionString = "amqp://guest:guest@localhost:5672/";
    o.ExchangeName = "orders";
    o.RoutingKeySelector = e => $"order.{e.MessageType}";
});
```

## [0.2.3] - 2026-06-09

### Added

#### EF Core inbox storage in `Moongazing.OrionPatch.EntityFrameworkCore`

Persists the v0.2.2 `IInbox` contract across process restarts.

- **`InboxRow`** entity: `MessageId` (Guid) + `Consumer` (string, nullable, max 128 chars) + `AcceptedAtUtc` (UTC DateTime). Maps to table `OrionPatch_Inbox`.
- **`InboxEntityConfiguration`** EF Core mapping with composite primary key on (`MessageId`, `Consumer`) so a single inbox table can serve multiple consumers without one consumer's accepted-set masking another's.
- **`EfCoreInbox`** implementation: change-tracker fast-path detects same-DbContext duplicates without throwing, then attempts the insert and catches `DbUpdateException` as "another connection beat us to it". Detaches the failed entry so the change tracker does not retry it on subsequent `SaveChanges` calls.
- **`InboxBuilderExtensions.UseEntityFrameworkCoreInbox<TDbContext>(consumer?)`** DI helper. Replaces any prior `IInbox` registration (including the v0.2.2 `InMemoryInbox` default) and registers `EfCoreInbox` as scoped so it shares the DbContext lifetime.

### Deferred

- `OrionPatch.RabbitMQ` sink -> v0.2.4 (was v0.2.4, unchanged)
- `OrionPatch.AzureServiceBus` sink -> v0.2.5 (was v0.2.5, unchanged)

`ROADMAP.md` reflects the targets.

### Migration from v0.2.2

Source-compatible. Adopt the new inbox storage by:

1. Call `modelBuilder.ApplyOrionPatchConfiguration()` from your DbContext's `OnModelCreating` (the helper now applies both `OutboxEntityConfiguration` and `InboxEntityConfiguration`; the latter is also `public` so consumers can apply it directly).
2. Add and apply an EF Core migration that creates the `OrionPatch_Inbox` table.
3. Register: `services.AddOrionPatch().UseEntityFrameworkCore<AppDbContext>().UseEntityFrameworkCoreInbox<AppDbContext>();`

Consumers staying on v0.2.2's in-memory inbox see no behaviour change.

## [0.2.2] - 2026-06-09

### Added

#### `IInbox` consumer-side dedup primitive

- **`IInbox`** in `Moongazing.OrionPatch.Abstractions`. Single method `TryAcceptAsync(messageId, ct)` returns `true` on first delivery, `false` on duplicate. Concurrency contract: at most one caller for the same id observes `true`, even under parallel delivery.
- **`InMemoryInbox`** in `Moongazing.OrionPatch.Channels` - `ConcurrentDictionary<Guid, byte>` backed implementation. Bounded by host RAM; intended for tests, demos, and single-instance services where dedup does not need to survive restart. `Count` and `Reset()` helpers exposed for fact runs in shared xunit collections.
- **`InboxFilter`** wrapper - `InvokeIfFirstAsync(envelope, handler, ct)` runs a handler delegate only on first delivery so consumer broker pipelines can plug dedup in with one decorator instead of branching at every handler.

### Deferred

- **EF Core inbox storage** (`OrionPatch.EntityFrameworkCore` inbox table + storage interface) -> v0.2.3
- **`OrionPatch.RabbitMQ`** sink -> v0.2.4 (was v0.2.3; renamed because EF inbox takes that slot)
- **`OrionPatch.AzureServiceBus`** sink -> v0.2.5 (was v0.2.4)

`ROADMAP.md` reflects the new sequence.

### Migration from v0.2.1

Source-compatible. The new types are additive; consumers wire dedup explicitly via `InboxFilter` or the raw `IInbox`. There is no auto-registration.

## [0.2.1] - 2026-06-04

### Added

#### `IOutboxTenantResolver` ambient tenant capture

- New `IOutboxTenantResolver` interface in `Moongazing.OrionPatch.Abstractions`. Single method `Resolve()` called per `IOutbox.Enqueue<T>` invocation to capture the ambient tenant identifier without forcing callers to pass `Headers["tenant-id"]` manually.
- `NullOutboxTenantResolver` default registration: always returns null, so v0.2.0 behaviour is preserved when nothing is registered.
- `DelegateOutboxTenantResolver(Func<string?>)` for consumers who already have a resolution function (closure over `IHttpContextAccessor`, ambient AsyncLocal, etc.) and do not want a one-off class.
- Standard header key `IOutboxTenantResolver.TenantHeaderName = "tenant-id"`. Resolver-supplied tenants merge with caller-supplied `OutboxEnqueueOptions.Headers`; caller-supplied `Headers["tenant-id"]` wins on conflict so explicit per-enqueue overrides remain authoritative.
- `EfCoreOutbox.Enqueue` consults the registered resolver. `UseEntityFrameworkCore` adds the default via `TryAddSingleton`, so opting in is a single `services.AddSingleton<IOutboxTenantResolver, ...>()` line before that call.

### Deferred

Remaining v0.2.x items continue with their previously published targets:

- `OrionPatch.Inbox` (consumer-side dedup) -> v0.2.2
- `OrionPatch.RabbitMQ` sink -> v0.2.3
- `OrionPatch.AzureServiceBus` sink -> v0.2.4

### Migration from v0.2.0

Source-compatible. Opt in via:

```csharp
services.AddSingleton<IOutboxTenantResolver>(_ =>
    new DelegateOutboxTenantResolver(() => HttpContext.Current?.User?.GetTenantId()));
services.AddOrionPatch().UseEntityFrameworkCore<AppDbContext>();
```

## [0.2.0] - 2026-06-01

### Added

- **`MessageTypeRegistry`** (public, in `Moongazing.OrionPatch.Configuration`). A bidirectional mapping between logical wire names (`"OrderShipped"`, `"OrderShipped.V2"`) and CLR types. Lets consumers rename or refactor message types without breaking in-flight outbox rows: the row keeps the logical name, and the registry resolves it back to a CLR type at dispatch time. Built via `MessageTypeRegistryBuilder` and surfaced via `services.AddOrionPatch().UseMessageTypeRegistry(r => r.Map<OrderShipped>("OrderShipped"))`. Backed by a `FrozenDictionary` so look-ups are allocation-free.
- **`MessageTypeRegistryOptions.AllowAssemblyQualifiedNameFallback`** (default `true`). When enabled, unmapped CLR types fall back to `Type.FullName`. Set to `false` to require explicit mapping for every type the outbox sees; missing mappings then throw `InvalidOperationException` at enqueue time with a guidance message.

### Changed

- **`MessageTypeNameResolver`** consults the registry before falling back. Precedence is now: per-enqueue `OutboxEnqueueOptions.MessageType` override, then registered logical name, then `Type.FullName` (or throw, depending on the fallback option). The default registry is `MessageTypeRegistry.Empty`, so consumers that never call `.UseMessageTypeRegistry(...)` see identical behaviour to v0.1.x.

### Deferred from v0.2.0

The original v0.2.0 milestone listed five items. Four are de-scoped to keep this minor focused and reviewable:

- **`IOutboxTenantResolver`** (multi-tenant outbox filtering) -> v0.2.1. Documented `Headers["tenant-id"]` workaround remains supported.
- **`OrionPatch.Inbox`** (sibling storage primitive for consumer-side dedup) -> v0.2.2.
- **`OrionPatch.RabbitMQ`** sink -> v0.2.3.
- **`OrionPatch.AzureServiceBus`** sink -> v0.2.4.

`ROADMAP.md` reflects the new targets.

### Migration from v0.1.1

Source-compatible. The default registration of `MessageTypeRegistry.Empty` plus the existing FullName fallback means consumers see no behaviour change until they opt in via `.UseMessageTypeRegistry(...)`. Existing outbox rows continue to dispatch under their FullName-based `MessageType`.

To opt in:

```csharp
services.AddOrionPatch()
    .UseChannelSink(...)
    .UseMessageTypeRegistry(r => r
        .Map<OrderShipped>("OrderShipped")
        .Map<OrderShippedV2>("OrderShipped.V2"));
```

## [0.1.1] - 2026-05-26

### Changed

- Logo now ships with a cream (#F7F1E3) background instead of transparent. Improves contrast against dark-mode README rendering and NuGet package card backgrounds. No functional change.

## [0.1.0] - 2026-05-24

Initial public release. Three NuGet packages: `OrionPatch`, `OrionPatch.EntityFrameworkCore`,
`OrionPatch.Testing`. Multi-targets `net8.0`, `net9.0`, `net10.0`. 282 tests passing across
the three test projects (94 per TFM).

### Added — Core (`OrionPatch`)

- `IOutbox` scope-bound enqueue API.
- `IOutboxSink` pluggable destination contract.
- `IOutboxStorage` storage SPI.
- `IOutboxDispatcherClock` time + delay abstraction.
- `OutboxEnvelope`, `OutboxRow`, `OutboxStatus`, `OutboxEnqueueOptions` model types.
- `OrionPatchOptions` with 8 tunable knobs (polling interval, batch size, max attempts,
  lease duration, dispatcher enabled, backoff strategy, dispatcher identity factory, JSON
  options) — all documented defaults.
- `BackoffStrategy` static factory (`Exponential(initial, max)` with long-overflow guard;
  `Fixed(delay)`).
- `OrionPatchDiagnostics` — `ActivitySource` + `Meter` named `Moongazing.OrionPatch`; 5
  counters (`orionpatch.outbox.enqueued`, `.dispatched`, `.failed`, `.deadlettered`,
  `.attempts`) + 1 histogram (`orionpatch.outbox.dispatch.duration`, ms).
- `ChannelOutboxSink` — in-process `System.Threading.Channels`-backed sink; default
  capacity 1000, `BoundedChannelFullMode.Wait`. Zero external dependency.
- `OutboxDispatcherHostedService` — `BackgroundService` claim → dispatch →
  complete/fail/dead-letter loop with retry + exponential backoff + lease expiry +
  source-generated structured logging.
- DI: `AddOrionPatch()`, `UseSink<T>()`, `UseChannelSink()` (last-call-wins via
  `RemoveAll<IOutboxSink>()`).
- `NoOpHostedService` for writer-only replicas (`DispatcherEnabled = false`).

### Added — EF Core (`OrionPatch.EntityFrameworkCore`)

- `OutboxEntityConfiguration` — maps `OutboxRow` to `OrionPatch_Outbox` table with covering
  indexes.
- `OrionPatchDbContextExtensions.ApplyOrionPatchConfiguration()` ModelBuilder helper.
- `EfCoreOutbox` — buffers per-DbContext, binds via `ConditionalWeakTable` for interceptor
  lookup, honors configured `JsonSerializerOptions` for both payload AND headers.
- `OrionPatchSaveChangesInterceptor` — three-phase Flush/Commit/Revert (six lifecycle
  overrides) preserves at-least-once on save failure (re-buffers rows + detaches added
  entries on `SaveChangesFailed`).
- `EfCoreOutboxStorage` — `ExecuteUpdateAsync` single-round-trip Complete/Fail/DeadLetter;
  `AsNoTracking` queue depth.
- Provider-aware claim strategy: SqlServer / PostgreSQL / MySQL routed through
  `SkipLockedClaimStrategy` (delegates to portable fallback at v0.1.0; native `SKIP LOCKED`
  SQL targeted for v0.2). SQLite + unknown providers use `CompareAndSwapClaimStrategy`.
- DI: `UseEntityFrameworkCore<TDbContext>()` on `OrionPatchBuilder` + `UseOrionPatch(IServiceProvider)`
  on `DbContextOptionsBuilder`.

### Added — Testing (`OrionPatch.Testing`)

- `InMemoryOutboxStorage` — thread-safe in-memory `IOutboxStorage` (no EF Core dep).
- `InMemoryOutbox` — `IOutbox` companion writing directly to storage.
- `DeterministicDispatcher.DispatchOnceAsync` — synchronous test driver.
- `CapturingOutboxSink` — records every dispatched envelope.
- `TestClock` — settable `IOutboxDispatcherClock`.
- `OutboxAssertions` — fluent `AssertDispatched<T>(predicate)` + `AssertDeadLettered(predicate)`.
- DI: `UseInMemory()` extension that replaces any prior storage/outbox registrations.

### Added — Sample

- `sample/Moongazing.OrionPatch.Sample` — generic-host console demonstrating end-to-end
  enqueue → dispatch → `ChannelOutboxSink` consumption.

### Notes

- v0.1.0 ships outbox-only. Inbox, dedup, concrete broker sinks (RabbitMQ / Azure Service
  Bus / Kafka / NATS), saga support, and push-based dispatch are on the v0.2+ roadmap. See
  [ROADMAP.md](ROADMAP.md).
- Targets: `net8.0`, `net9.0`, `net10.0`.

[Unreleased]: https://github.com/tunahanaliozturk/OrionPatch/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/tunahanaliozturk/OrionPatch/releases/tag/v0.1.0
