# OrionPatch.EntityFrameworkCore

EF Core storage backend for [OrionPatch](https://github.com/tunahanaliozturk/OrionPatch). Adds the `OrionPatch_Outbox` table with provider-aware competing-consumers claim and a `SaveChangesInterceptor` that flushes buffered messages into your transaction.

See the [repo README](https://github.com/tunahanaliozturk/OrionPatch) for the full picture.
