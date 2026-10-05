# OrionPatch

Transactional outbox primitive for .NET. Enqueue messages inside your database transaction; a background dispatcher hands them to a pluggable `IOutboxSink` at least once, with retry, dead-letter, redrive and OpenTelemetry built in.

![OrionPatch outbox write and dispatch: SaveChangesAsync commits the outbox row with your data, the dispatcher claims due rows, calls IOutboxSink.SendAsync for Kafka, RabbitMQ or Azure Service Bus, then marks the row Processed](https://raw.githubusercontent.com/tunahanaliozturk/OrionPatch/main/docs/diagrams/outbox-dispatch.png)

## Install

    dotnet add package OrionPatch

This is the core package: `IOutbox`, `IOutboxSink`, `IOutboxStorage`, the dispatcher hosted service, options, telemetry, the in-process `ChannelOutboxSink` and the `IInbox` dedup contract. It needs a storage backend; `OrionPatch.EntityFrameworkCore` is the production one.

## Quick start

```csharp
using Moongazing.OrionPatch.DependencyInjection;
using Moongazing.OrionPatch.EntityFrameworkCore.DependencyInjection;

services.AddDbContext<AppDbContext>((sp, options) =>
{
    options.UseNpgsql(connectionString);
    options.UseOrionPatch(sp);   // SaveChanges interceptor
});

services.AddOrionPatch(o => o.PollingInterval = TimeSpan.FromSeconds(1))
    .UseEntityFrameworkCore<AppDbContext>()
    .UseChannelSink();           // or .UseSink<MySink>(), or a broker sink package
```

`UseEntityFrameworkCore` only registers services. Map the outbox tables in your `DbContext` (namespace `Moongazing.OrionPatch.EntityFrameworkCore`):

```csharp
protected override void OnModelCreating(ModelBuilder modelBuilder) =>
    modelBuilder.ApplyOrionPatchConfiguration();
```

This maps four tables (`OrionPatch_Outbox`, `OrionPatch_DeadLetter`, `OrionPatch_OutboxArchive`, `OrionPatch_Inbox`). The runtime never creates them; add and apply a migration:

```bash
dotnet ef migrations add AddOrionPatch
dotnet ef database update
```

Then enqueue with `IOutbox.Enqueue(message)` before `SaveChangesAsync`; the message commits with your data and is dispatched after the commit.

The built-in `ChannelOutboxSink` delivers in-process (bounded channel, capacity 1000, `BoundedChannelFullMode.Wait`). Drain it anywhere in your app:

```csharp
var sink = serviceProvider.GetRequiredService<ChannelOutboxSink>();
await foreach (var envelope in sink.Reader.ReadAllAsync(cancellationToken))
{
    Console.WriteLine($"{envelope.MessageType}: {envelope.Payload}");
}
```

## Options

| `OrionPatchOptions` | Default |
|---------------------|---------|
| `PollingInterval` (wait after an empty poll) | 1 s |
| `BatchSize` | 50 |
| `MaxAttempts` (then dead-letter) | 5 |
| `LeaseDuration` (claimed row reserved for) | 2 min |
| `BackoffStrategy` | `BackoffStrategy.Exponential(1 s, 30 min)` |
| `ArchiveRetention` | 7 days |
| `DispatcherEnabled` | `true` |

## Failure semantics

- Delivery is at least once. A crash between `SendAsync` and the completion write, or a send longer than `LeaseDuration`, delivers the row again. Make sinks and consumers idempotent on `OutboxEnvelope.Id`.
- A throwing `SendAsync` is retried after `BackoffStrategy(attempt)`. At `MaxAttempts` the row moves to the dead-letter store (`IDeadLetterStore`) or, if the storage has none, is marked `DeadLettered` in place.
- `IDeadLetterReplayStore.RedriveAsync` puts dead-lettered messages back in the outbox; `IOutboxArchivalStore.ArchiveProcessedAsync` reaps processed rows older than `ArchiveRetention`.
- `IDeadLetterSink` and `IOutboxDispatchObserver` are optional hooks; their failures are logged and counted, never retried.

## Telemetry

`ActivitySource` and `Meter` named `Moongazing.OrionPatch`: an `OrionPatch.Dispatch` span per envelope, `orionpatch.outbox.*` counters (dispatched, failed, deadlettered, attempts, ...), histograms (dispatch duration, sink duration, queue lag, pickup lag, batch size, ...) and a `queue_depth` gauge.

## Related packages

- `OrionPatch.EntityFrameworkCore` - EF Core storage, transactional enqueue interceptor, native claim, dead-letter and archive tables.
- `OrionPatch.Kafka`, `OrionPatch.RabbitMQ`, `OrionPatch.AzureServiceBus` - broker sinks.
- `OrionPatch.Testing` - in-memory storage, deterministic dispatcher and assertions for tests.

## Links

- Documentation and full README: https://github.com/tunahanaliozturk/OrionPatch
- Changelog: https://github.com/tunahanaliozturk/OrionPatch/blob/main/CHANGELOG.md
- License: MIT
