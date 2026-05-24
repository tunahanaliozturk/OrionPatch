# OrionPatch

Transactional outbox primitive for .NET. Enqueue messages inside an EF Core `SaveChanges` transaction; a background dispatcher hands them to a pluggable `IOutboxSink` at-least-once.

This is the core package. It defines `IOutbox`, `IOutboxSink`, `IOutboxStorage`, the dispatcher hosted service, telemetry, and a built-in in-process `ChannelOutboxSink`. Use the companion `OrionPatch.EntityFrameworkCore` package for the EF Core storage backend, and `OrionPatch.Testing` for test helpers.

## 30-second quick start

```csharp
services.AddOrionPatch(o => o.PollingInterval = TimeSpan.FromSeconds(1))
    .UseSink<MyKafkaSink>();   // or .UseChannelSink() for in-process
```

You'll also need a storage backend — see [`OrionPatch.EntityFrameworkCore`](https://www.nuget.org/packages/OrionPatch.EntityFrameworkCore).

## Built-in `ChannelOutboxSink`

Useful for monoliths (in-process pub/sub) and tests. Zero external dependency.

```csharp
services.AddOrionPatch().UseChannelSink(o => o.Capacity = 1000);

// Drain envelopes from anywhere in your app:
var sink = sp.GetRequiredService<ChannelOutboxSink>();
await foreach (var envelope in sink.Reader.ReadAllAsync(cancellationToken))
{
    // handle envelope.MessageType / envelope.Payload
}
```

## Telemetry

`ActivitySource` and `Meter` named `Moongazing.OrionPatch`. Counters cover enqueued / dispatched / failed / dead-lettered / attempts; histogram measures per-envelope dispatch duration in milliseconds.

See the [repo README](https://github.com/tunahanaliozturk/OrionPatch) for the full picture, comparison vs MassTransit / Wolverine, the at-least-once contract, and the roadmap.
