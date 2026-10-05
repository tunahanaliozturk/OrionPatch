# OrionPatch.EntityFrameworkCore

EF Core storage backend for OrionPatch: a `SaveChangesInterceptor` writes outbox rows in the same transaction as your data, and a provider-native claim lets several dispatchers share one `OrionPatch_Outbox` table safely.

![OrionPatch outbox write and dispatch: SaveChangesAsync commits the outbox row with your data, the dispatcher claims due rows, calls IOutboxSink.SendAsync, then marks the row Processed](https://raw.githubusercontent.com/tunahanaliozturk/OrionPatch/main/docs/diagrams/outbox-dispatch.png)

## Install

    dotnet add package OrionPatch.EntityFrameworkCore

It plugs into `OrionPatch` (added as a dependency). Add the EF Core provider for your database as well.

## Quick start

```csharp
using Moongazing.OrionPatch.DependencyInjection;
using Moongazing.OrionPatch.EntityFrameworkCore.DependencyInjection;

services.AddDbContext<AppDbContext>((sp, options) =>
{
    options.UseNpgsql(connectionString);
    options.UseOrionPatch(sp);   // hook the interceptor into your DbContext
});

services.AddOrionPatch()
    .UseEntityFrameworkCore<AppDbContext>()
    .UseChannelSink();           // or a broker sink package
```

In your `DbContext` (namespace `Moongazing.OrionPatch.EntityFrameworkCore`):

```csharp
protected override void OnModelCreating(ModelBuilder modelBuilder) =>
    modelBuilder.ApplyOrionPatchConfiguration();
```

Enqueue from service code:

```csharp
outbox.Enqueue(new OrderConfirmed(orderId, totalCents));
await db.SaveChangesAsync(ct);   // outbox row commits with your other entity changes
```

## Tables and migrations

`ApplyOrionPatchConfiguration()` maps four tables: `OrionPatch_Outbox`, `OrionPatch_DeadLetter`, `OrionPatch_OutboxArchive` and `OrionPatch_Inbox`. The runtime never creates tables; add and apply a migration:

```bash
dotnet ef migrations add AddOrionPatch
dotnet ef database update
```

## Claim strategy

The claim is picked from the DbContext's provider:

- PostgreSQL and MySQL / MariaDB: `FOR UPDATE SKIP LOCKED`.
- SQL Server: `WITH (UPDLOCK, READPAST, ROWLOCK, READCOMMITTEDLOCK)`, so `READPAST` skips locked rows even with `READ_COMMITTED_SNAPSHOT` on.
- SQLite and unrecognised providers: a portable compare-and-swap claim.

A row another dispatcher holds is skipped, not waited on. A claimed row whose lease (`LeaseDuration`, default 2 min) has expired can be claimed again.

## Dead-letter, redrive, archival and inbox

`UseEntityFrameworkCore<TDbContext>()` also registers `IDeadLetterStore`, `IDeadLetterReplayStore` and `IOutboxArchivalStore` (scoped):

- Rows that exhaust `MaxAttempts` move to `OrionPatch_DeadLetter` in one transaction, once per row id.
- `RedriveAsync(id)` or `RedriveAsync(filter, batchSize)` puts them back in the outbox.
- `ArchiveProcessedAsync(retention, nowUtc, ct)` moves processed rows to `OrionPatch_OutboxArchive` in batches; pass `purgeOnArchive: true` to `UseEntityFrameworkCore` to delete them instead. Call it from your own scheduled job.

`UseEntityFrameworkCoreInbox<TDbContext>(consumer: "billing")` registers an `IInbox` backed by `OrionPatch_Inbox` for consumer-side dedup (used by the RabbitMQ consumer and the Kafka inbox).

One OrionPatch-bound `DbContext` per host: a second `UseEntityFrameworkCore<T>` call adds a second `IOutbox` / `IOutboxStorage` registration and the last one wins. Not AOT- or trim-compatible, because EF Core is not.

## Related packages

- `OrionPatch` - the core this backend plugs into.
- `OrionPatch.Kafka`, `OrionPatch.RabbitMQ`, `OrionPatch.AzureServiceBus` - broker sinks.
- `OrionPatch.Testing` - in-memory storage for tests without a database.

## Links

- Documentation and full README: https://github.com/tunahanaliozturk/OrionPatch
- Changelog: https://github.com/tunahanaliozturk/OrionPatch/blob/main/CHANGELOG.md
- License: MIT
