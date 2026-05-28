# OrionPatch Benchmarks

> **Status: pending v0.2.0.** A dedicated `bench/Moongazing.OrionPatch.Bench` project will land in the next minor release. This document outlines the scenarios we intend to measure and the comparison baselines we will report against.

## Scenarios on the roadmap

- **Interceptor overhead (no outbox rows).** SaveChanges with the OrionPatch SaveChangesInterceptor registered but zero enqueued messages, vs. baseline SaveChanges without the interceptor. Expected metric: per-call overhead in microseconds and allocated bytes. Goal: stay under 5 percent overhead and zero extra allocations on the hot path.
- **Interceptor with N enqueued events.** Same call, 1 / 10 / 100 enqueued events. Expected metric: throughput in transactions per second on SQLite (in-memory) and Postgres (Testcontainers). Goal: linear scaling, dominated by the underlying INSERT cost.
- **Dispatcher claim batch.** Dispatcher polling against a pre-seeded outbox table of 10,000 ready rows, batch size 50 / 500. Expected metric: rows claimed per second and contention behavior with N concurrent dispatchers (1 / 4 / 16). Goal: confirm the portable compare-and-swap claim performs acceptably until the v0.2 `SKIP LOCKED` work lands.
- **End-to-end enqueue to sink latency.** From `IOutbox.Enqueue` to `IOutboxSink.SendAsync` returning, with the in-process `ChannelOutboxSink`. Expected metric: p50 / p99 latency in milliseconds. Goal: provide a baseline number readers can compare against once broker sinks ship in v0.2.
- **Allocations on the dispatcher hot loop.** GC stats over 1,000,000 dispatched messages. Goal: no Gen2 collections; bounded Gen0 churn.

## Why not yet?

The dispatcher has been benchmarked informally during development (release-mode test runs with stopwatch instrumentation against SQLite and Postgres) and the numbers were healthy enough to ship v0.1.0. A formal BenchmarkDotNet harness was deprioritized so v0.1.0 could ship with a working `ChannelOutboxSink` and EF Core backend before broker sinks land in v0.2. The harness is the first piece of v0.2 work.

If you have a specific scenario you want measured before v0.2, open an issue with the `benchmark` label and we will prioritize it.

## How it will be run

```bash
cd <repo-root>
dotnet run -c Release --project bench/Moongazing.OrionPatch.Bench
```

Results will land in `BenchmarkDotNet.Artifacts/results/` and a summary will be committed back to this file with each release.

## Comparison baselines

We will report OrionPatch numbers next to two honest baselines so readers can place them in context:

- **DIY interceptor.** A minimal hand-rolled SaveChangesInterceptor that writes to an outbox table and a background poller. Establishes how much overhead the OrionPatch abstractions add over the bare metal.
- **MassTransit InMemory mediator + outbox.** Same workload using MassTransit's in-memory transport with the EF Core outbox. Establishes how OrionPatch compares against the closest commodity alternative when you do not need a framework.

The point of the comparison is to be honest about where OrionPatch sits, not to win a chart. If MassTransit is faster on a given scenario we will say so and explain why.
