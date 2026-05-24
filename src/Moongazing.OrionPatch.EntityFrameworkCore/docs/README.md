# OrionPatch.EntityFrameworkCore

EF Core storage backend for [OrionPatch](https://github.com/tunahanaliozturk/OrionPatch). Adds the `OrionPatch_Outbox` table with provider-aware competing-consumers claim and a `SaveChangesInterceptor` that flushes buffered messages into your transaction.

## 30-second quick start

```csharp
services.AddDbContext<AppDbContext>((sp, options) =>
{
    options.UseNpgsql(connectionString);
    options.UseOrionPatch(sp);   // hook the interceptor into your DbContext
});

services.AddOrionPatch()
    .UseEntityFrameworkCore<AppDbContext>()
    .UseSink<MyKafkaSink>();
```

In your `DbContext.OnModelCreating`:

```csharp
protected override void OnModelCreating(ModelBuilder modelBuilder) =>
    modelBuilder.ApplyOrionPatchConfiguration();
```

Enqueue from service code:

```csharp
_outbox.Enqueue(new OrderConfirmed(orderId, totalCents));
await _db.SaveChangesAsync(ct);   // outbox row commits with your other entity changes
```

## What's in the box

- `OutboxEntityConfiguration` — maps `OutboxRow` to `OrionPatch_Outbox` with covering indexes for the dispatcher's polling and lease-expiry queries.
- `EfCoreOutbox` — `IOutbox` that buffers per-DbContext and binds via `ConditionalWeakTable` so the interceptor finds the right buffer without a service-provider hop.
- `OrionPatchSaveChangesInterceptor` — six-override lifecycle (Saving / Saved / SaveChangesFailed and their async siblings) implementing a three-phase Flush / Commit / Revert so save failures re-buffer cleanly without double-inserting on retry.
- `EfCoreOutboxStorage` — claim/complete/fail/dead-letter via `ExecuteUpdateAsync` single round-trips.
- Provider-aware claim strategy:
  - SqlServer / PostgreSQL / MySQL — `SkipLockedClaimStrategy` with dialect-specific SQL. (v0.1.0 currently delegates these to the portable fallback; true `SKIP LOCKED` SQL lands in v0.2.)
  - SQLite + unknown providers — `CompareAndSwapClaimStrategy` (portable optimistic-concurrency claim).

## Multi-DbContext

v0.1.0 supports one OrionPatch-bound DbContext per host. Calling `UseEntityFrameworkCore<TDbContext>` twice silently overrides the previous registration's `IOutbox` and `IOutboxStorage` services. First-class multi-DbContext support is on the v0.2 roadmap.

See the [repo README](https://github.com/tunahanaliozturk/OrionPatch) for the full picture.
