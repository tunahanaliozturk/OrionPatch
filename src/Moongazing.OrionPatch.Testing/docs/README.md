# OrionPatch.Testing

Test helpers for OrionPatch: in-memory storage, a deterministic dispatcher you drive by hand, a capturing sink, a settable clock and fluent assertions. No EF Core and no database needed.

![OrionPatch packages: OrionPatch.Testing swaps in in-memory storage and a capturing sink for the core in tests](https://raw.githubusercontent.com/tunahanaliozturk/OrionPatch/main/docs/diagrams/overview.png)

## Install

    dotnet add package OrionPatch.Testing

It plugs into `OrionPatch` (added as a dependency).

## Quick start

```csharp
using Microsoft.Extensions.DependencyInjection;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.DependencyInjection;
using Moongazing.OrionPatch.Testing;
using Moongazing.OrionPatch.Testing.DependencyInjection;

[Fact]
public async Task Confirmed_order_is_dispatched()
{
    var services = new ServiceCollection();
    services.AddOrionPatch().UseInMemory();
    using var provider = services.BuildServiceProvider();

    var outbox = provider.GetRequiredService<IOutbox>();
    outbox.Enqueue(new OrderConfirmed(Guid.NewGuid(), 100));

    var storage = provider.GetRequiredService<InMemoryOutboxStorage>();
    var sink = new CapturingOutboxSink();
    var dispatcher = new DeterministicDispatcher(storage, sink, new TestClock());

    var dispatched = await dispatcher.DispatchOnceAsync();

    Assert.Equal(1, dispatched);
    sink.AssertDispatched<OrderConfirmed>(e => e.TotalCents == 100);
}
```

## Helpers

- `UseInMemory()` - registers `InMemoryOutboxStorage` and `InMemoryOutbox` as singletons, replacing any `IOutbox` / `IOutboxStorage`; the storage is also exposed as `IDeadLetterStore`, `IOutboxArchivalStore` and `IDeadLetterReplayStore`.
- `InMemoryOutboxStorage` - thread-safe `IOutboxStorage` with the same FIFO, lease-expiry, dead-letter, redrive and archive behaviour as the EF Core backend. `new InMemoryOutboxStorage(purgeOnArchive: true)` discards archived rows.
- `InMemoryOutbox` - `IOutbox` that writes straight to the in-memory storage; `Enqueue` is immediately visible.
- `DeterministicDispatcher` - `DispatchOnceAsync()` runs one claim and dispatch pass (retry with backoff, dead-letter at `MaxAttempts`) on the calling thread and returns the number of rows dispatched. Takes an optional `OrionPatchOptions`.
- `CapturingOutboxSink` - records every envelope in `Sent`; `Clear()` resets it.
- `TestClock` - `UtcNow` moves only on `Advance` / `Set`; `DelayAsync` returns at once.
- `OutboxAssertions` - `sink.AssertDispatched<T>(predicate)`, `storage.AssertDeadLettered(predicate)`, `storage.AssertRedriven(predicate)`; each throws `InvalidOperationException` when nothing matches. `AssertDeadLettered` looks at rows marked `DeadLettered` in the outbox; a row the dispatcher moved to the dead-letter store is read with `storage.GetDeadLetteredAsync()`.

## Related packages

- `OrionPatch` - the core these helpers stand in for.
- `OrionPatch.EntityFrameworkCore` - the production storage backend.

## Links

- Documentation and full README: https://github.com/tunahanaliozturk/OrionPatch
- Changelog: https://github.com/tunahanaliozturk/OrionPatch/blob/main/CHANGELOG.md
- License: MIT
