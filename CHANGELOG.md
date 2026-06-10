# Changelog

All notable changes to OrionPatch are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/) and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
