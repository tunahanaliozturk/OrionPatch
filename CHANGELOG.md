# Changelog

All notable changes to OrionPatch are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/) and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
