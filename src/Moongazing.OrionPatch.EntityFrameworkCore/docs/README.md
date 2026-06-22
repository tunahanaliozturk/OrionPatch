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
- `EfCoreOutboxStorage` — claim/complete/fail/dead-letter via `ExecuteUpdateAsync` single round-trips. From v0.3.2 it also implements `IDeadLetterStore` and `IOutboxArchivalStore` (see below).
- Provider-aware claim strategy:
  - SqlServer / PostgreSQL / MySQL — `SkipLockedClaimStrategy` with dialect-specific SQL. (v0.1.0 currently delegates these to the portable fallback; true `SKIP LOCKED` SQL lands in v0.2.)
  - SQLite + unknown providers — `CompareAndSwapClaimStrategy` (portable optimistic-concurrency claim).

## Dead-letter and archival (v0.3.2)

`EfCoreOutboxStorage` implements the v0.3.0 `IDeadLetterStore` and `IOutboxArchivalStore` SPIs against two new tables, so deployments get the durable dead-letter destination and the retention reaper without writing their own storage.

- **`OrionPatch_DeadLetter`** (`DeadLetterRow`) — the dispatcher routes a row that exhausts `MaxAttempts` here instead of flipping it to `DeadLettered` in place. The move (delete the source outbox row, insert the snapshot) runs in one transaction so it is atomic, and the dead-letter primary key is the source row id so a crash-replayed terminal path lands the message exactly once. Read it back with `GetDeadLetteredAsync()` (newest first, capped) or the paged `GetDeadLetteredAsync(skip, take, ct)` overload for triage over a large backlog.
- **`OrionPatch_OutboxArchive`** (`OutboxArchiveRow`) — `ArchiveProcessedAsync(retention, nowUtc, ct)` reaps `Processed` rows older than the retention cutoff out of the hot outbox in bounded batches and returns the count moved. Archive mode (default) copies them here first; purge mode skips the table and deletes outright. This is operator-invoked — OrionPatch does not start a background reaper, so call it from your own scheduled job.

Choose the mode at registration:

```csharp
services.AddOrionPatch()
    .UseEntityFrameworkCore<AppDbContext>();             // archive mode (default)
// or
services.AddOrionPatch()
    .UseEntityFrameworkCore<AppDbContext>(purgeOnArchive: true);   // purge mode
```

Invoke the reaper from a scheduled job, resolving the archival store from a scope:

```csharp
using var scope = serviceProvider.CreateScope();
var archival = scope.ServiceProvider.GetRequiredService<IOutboxArchivalStore>();
var reaped = await archival.ArchiveProcessedAsync(options.ArchiveRetention, DateTime.UtcNow, ct);
```

### Migration

The runtime never creates tables. After upgrading to v0.3.2, `ApplyOrionPatchConfiguration()` adds the two new tables to your model; regenerate and apply a migration so they exist before the dead-letter store or the reaper runs:

```bash
dotnet ef migrations add OrionPatch_v0_3_2_DeadLetterAndArchive
dotnet ef database update
```

The two new tables are inert until you route a dead-letter or call `ArchiveProcessedAsync`, so applying the migration is safe to do ahead of turning either path on. No existing column or index changes — this migration is purely additive.

## Multi-DbContext

v0.1.0 supports one OrionPatch-bound DbContext per host. Calling `UseEntityFrameworkCore<TDbContext>` twice silently overrides the previous registration's `IOutbox` and `IOutboxStorage` services. First-class multi-DbContext support is on the v0.2 roadmap.

See the [repo README](https://github.com/tunahanaliozturk/OrionPatch) for the full picture.
