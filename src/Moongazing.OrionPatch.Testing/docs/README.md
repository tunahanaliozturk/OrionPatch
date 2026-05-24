# OrionPatch.Testing

Test helpers for [OrionPatch](https://github.com/tunahanaliozturk/OrionPatch). In-memory storage, deterministic dispatcher driver, capturing sink, test clock, fluent assertions. Zero EF Core dependency.

## 30-second quick start

```csharp
[Fact]
public async Task OrderConfirmed_ShouldDispatch_WhenSaved()
{
    var services = new ServiceCollection();
    services.AddOrionPatch().UseInMemory().UseChannelSink();
    var sp = services.BuildServiceProvider();

    var outbox = sp.GetRequiredService<IOutbox>();
    outbox.Enqueue(new OrderConfirmed(Guid.NewGuid(), 100));

    // Drive a single dispatch cycle synchronously:
    var storage = sp.GetRequiredService<InMemoryOutboxStorage>();
    var sink = sp.GetRequiredService<ChannelOutboxSink>();
    var clock = new TestClock();
    var dispatcher = new DeterministicDispatcher(storage, sink, clock);
    var processed = await dispatcher.DispatchOnceAsync();

    Assert.Equal(1, processed);
}
```

## Helpers

- `InMemoryOutboxStorage` — thread-safe `IOutboxStorage`, same FIFO + lease-expiry semantics as the EF Core claim strategy.
- `InMemoryOutbox` — `IOutbox` companion that writes directly to the in-memory storage (no DbContext needed).
- `DeterministicDispatcher` — `DispatchOnceAsync(CancellationToken)` driver; mirrors the production dispatcher's per-envelope semantics (retry + dead-letter) but called explicitly from tests.
- `CapturingOutboxSink` — `IOutboxSink` that records every dispatched envelope into a thread-safe list.
- `TestClock` — settable `IOutboxDispatcherClock`; `UtcNow` advances only when `Advance` / `Set` is called, `DelayAsync` is a no-op.
- `OutboxAssertions` — fluent helpers: `sink.AssertDispatched<T>(predicate)`, `storage.AssertDeadLettered(predicate)`.
- `UseInMemory()` — `OrionPatchBuilder` extension that wires the in-memory pair as singletons and replaces any previously-registered `IOutbox` / `IOutboxStorage`.

See the [repo README](https://github.com/tunahanaliozturk/OrionPatch) for the full picture.
