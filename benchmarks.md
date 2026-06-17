# OrionPatch Benchmarks

A [BenchmarkDotNet](https://benchmarkdotnet.org/) suite for the OrionPatch core library lives in
`benchmarks/Moongazing.OrionPatch.Benchmarks`. It measures the pure, in-memory hot paths of the
core package only. None of these benchmarks touch a database, a message broker (RabbitMQ, Kafka,
Azure Service Bus), or any other external dependency, so they run anywhere `dotnet` does and the
numbers reflect library overhead rather than I/O.

This document describes what is measured and how to run it. It intentionally contains no result
tables: numbers are machine-specific, so the suite prints them locally rather than committing
figures that would be stale or misleading on someone else's hardware.

## What is measured

Each class is a `[MemoryDiagnoser]` benchmark and runs on .NET 8 and .NET 9 via
`[SimpleJob(RuntimeMoniker.Net80)]` and `[SimpleJob(RuntimeMoniker.Net90)]`.

- **BackoffStrategyBenchmarks.** Evaluates `BackoffStrategy.Exponential` and `BackoffStrategy.Fixed`
  across attempt numbers spanning the early ramp, the saturation cap, and the overflow guard
  (`Attempt` = 1, 5, 30, 100). This delegate runs on every failed dispatch to compute the next
  attempt time, so its arithmetic and the overflow-saturation branch sit on the retry hot path. The
  suite also times constructing the factory delegate versus reusing a cached one.
- **OutboxEnvelopeBenchmarks.** Constructs the public `OutboxEnvelope` record with and without a
  header dictionary. The dispatcher materializes one envelope per row immediately before invoking
  the sink, so this is the per-message allocation on the dispatch hot loop.
- **ChannelOutboxSinkBenchmarks.** Writes N envelopes through `ChannelOutboxSink.SendAsync` and
  drains them via `ChannelOutboxSink.Reader` (`MessageCount` = 1, 100, 1000). This is the only sink
  shipped in the core library and stands in for in-process fan-out throughput. Channel capacity is
  sized above the batch so writes never back-pressure, isolating enqueue and drain cost from the
  full-channel path.
- **MessageTypeRegistryBenchmarks.** Times building the immutable `MessageTypeRegistry` (which
  snapshots into a `FrozenDictionary`, a one-time startup cost) and resolving names on the
  enqueue/dispatch hot path via `ResolveLogicalName` and `ResolveClrType` (one lookup per message).
- **OutboxRowLifecycleBenchmarks.** Constructs an `OutboxRow` and applies the in-memory
  claim/complete and claim/fail-with-backoff state transitions the dispatcher performs on each row,
  independent of any storage backend.

## Running

```bash
dotnet run -c Release --project benchmarks/Moongazing.OrionPatch.Benchmarks
```

Pass a filter to run a subset, for example:

```bash
dotnet run -c Release --project benchmarks/Moongazing.OrionPatch.Benchmarks -- --filter '*Backoff*'
```

Results are written to `BenchmarkDotNet.Artifacts/results/` next to the project. Run on a quiet
machine on AC power for stable measurements; laptop thermal throttling and background load skew the
numbers.

## Scope and exclusions

The suite deliberately covers only the core library's deterministic, in-memory paths. Backend and
broker behaviour (EF Core SaveChanges interception, claim contention under `SKIP LOCKED`, end-to-end
enqueue-to-broker latency) depends on external infrastructure and belongs in integration-style
performance tests against real dependencies, not in this microbenchmark project. If you want a
specific in-memory scenario added, open an issue with the `benchmark` label.
