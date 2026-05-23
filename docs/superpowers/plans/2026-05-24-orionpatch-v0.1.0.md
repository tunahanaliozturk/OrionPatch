# OrionPatch v0.1.0 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship OrionPatch v0.1.0 — a transactional outbox primitive for .NET (`OrionPatch`, `OrionPatch.EntityFrameworkCore`, `OrionPatch.Testing` on NuGet) with at-least-once dispatch through a pluggable `IOutboxSink`, one concrete `ChannelOutboxSink`, EF Core storage with provider-aware competing-consumers claim, and OpenTelemetry.

**Architecture:** Three projects matching the Orion-family layout. Core defines `IOutbox`, `IOutboxSink`, `IOutboxStorage`, the dispatcher hosted service, and telemetry. EF Core package implements `IOutboxStorage` against `OrionPatch_Outbox` with provider-aware SKIP LOCKED / fallback. Testing package ships an in-memory storage and a deterministic dispatcher driver. A `SaveChangesInterceptor` (in the EF Core package) flushes enqueued messages into the user's transaction.

**Tech Stack:** .NET 8/9/10 multi-target, xUnit (no FluentAssertions), EF Core 8.x for the storage backend, `System.Threading.Channels`, `System.Text.Json`, `Microsoft.Extensions.{DependencyInjection,Hosting,Options,Logging}`, `System.Diagnostics.{DiagnosticSource}` for OpenTelemetry. Family conventions: `TreatWarningsAsErrors`, source-gen-friendly, MIT, Conventional Commits, no co-author trailer.

---

## File Structure

### `src/Moongazing.OrionPatch/` (`OrionPatch` package)

| File | Responsibility |
|------|----------------|
| `Abstractions/IOutbox.cs` | Public scope-bound enqueue API (`Enqueue<T>`). |
| `Abstractions/IOutboxSink.cs` | Sink contract called per envelope. |
| `Abstractions/IOutboxStorage.cs` | Storage SPI (`AppendAsync`, `ClaimNextAsync`, `CompleteAsync`, `FailAsync`, `QueueDepthAsync`). |
| `Abstractions/IOutboxDispatcherClock.cs` | Clock + delay abstraction so tests can drive time. |
| `Models/OutboxEnvelope.cs` | Sink-facing record (immutable). |
| `Models/OutboxRow.cs` | Storage-facing row model used between storage SPI and dispatcher (Status enum included). |
| `Models/OutboxEnqueueOptions.cs` | Per-enqueue overrides. |
| `Configuration/OrionPatchOptions.cs` | All knobs (`PollingInterval`, `BatchSize`, `MaxAttempts`, backoff, `LeaseDuration`, `DispatcherEnabled`, dispatcher identity factory, JSON options). |
| `Configuration/BackoffStrategy.cs` | `Exponential(initial, max)` + `Fixed(delay)` static factories returning `Func<int, TimeSpan>`. |
| `Channel/ChannelOutboxSink.cs` | In-process sink backed by `Channel<OutboxEnvelope>`; exposes reader for consumers. |
| `Channel/ChannelOutboxSinkOptions.cs` | Capacity, full-mode. |
| `Hosting/OutboxDispatcherHostedService.cs` | Background loop: claim → dispatch → complete/fail. Single-instance per process. |
| `Telemetry/OrionPatchDiagnostics.cs` | `ActivitySource` + `Meter` + all instrument definitions (`orionpatch.outbox.*`). |
| `DependencyInjection/OrionPatchBuilder.cs` | Fluent builder returned from `AddOrionPatch()`; carries the service collection. |
| `DependencyInjection/OrionPatchServiceCollectionExtensions.cs` | `AddOrionPatch()`; registers core services, options, telemetry, hosted service. |
| `DependencyInjection/OutboxBuilderExtensions.cs` | `UseSink<T>()`, `UseChannelSink(...)`. |
| `Internal/SystemClock.cs` | Default `IOutboxDispatcherClock`. |
| `Internal/DefaultDispatcherIdentity.cs` | `{MachineName}/{ProcessId}` factory. |
| `Internal/MessageSerializer.cs` | Wraps `JsonSerializer` with the configured options. |
| `Internal/MessageTypeNameResolver.cs` | Default `typeof(T).FullName`, honors `OutboxEnqueueOptions.MessageType`. |
| `Moongazing.OrionPatch.csproj` | csproj copied from OrionLock core. |
| `docs/README.md` | NuGet readme (short version of the repo README). |
| `docs/logo.png` | NuGet PackageIcon, deployed from repo `docs/logo.png`. |

### `src/Moongazing.OrionPatch.EntityFrameworkCore/` (`OrionPatch.EntityFrameworkCore` package)

| File | Responsibility |
|------|----------------|
| `EfCoreOutbox.cs` | `IOutbox` implementation: buffers enqueued messages in an `AsyncLocal`/scoped list flushed by the interceptor. |
| `EfCoreOutboxStorage.cs` | `IOutboxStorage` implementation. Claim uses `SKIP LOCKED` on supported providers, `UPDATE ... WHERE` + `RowVersion` on others. |
| `OrionPatchSaveChangesInterceptor.cs` | `SaveChangesInterceptor` that materializes buffered messages into `OutboxRow` entities on `SavingChanges`/`SavingChangesAsync`. |
| `Entities/OutboxRow.cs` | EF Core entity for `OrionPatch_Outbox`. |
| `Configuration/OutboxEntityConfiguration.cs` | `IEntityTypeConfiguration<OutboxRow>` with indexes. |
| `OrionPatchDbContextExtensions.cs` | `modelBuilder.ApplyOrionPatchConfiguration()`. |
| `Internal/ProviderClaimStrategy.cs` | Resolves the right claim SQL per provider (`Microsoft.EntityFrameworkCore.SqlServer`, `Npgsql.*`, `Pomelo.EntityFrameworkCore.MySql`, `Microsoft.EntityFrameworkCore.Sqlite`). |
| `Internal/SkipLockedClaimStrategy.cs` | Composes the `SELECT ... FOR UPDATE SKIP LOCKED` + `UPDATE` claim, returns claimed rows. |
| `Internal/RowVersionClaimStrategy.cs` | Optimistic-concurrency fallback for SQLite. |
| `DependencyInjection/OrionPatchEntityFrameworkCoreBuilderExtensions.cs` | `UseEntityFrameworkCore<TDbContext>()` extension method on `OrionPatchBuilder`. |
| `Moongazing.OrionPatch.EntityFrameworkCore.csproj` | EF Core 8.0.x + `Moongazing.OrionPatch` project reference. |
| `docs/README.md` | NuGet readme. |
| `docs/logo.png` | NuGet PackageIcon. |

### `src/Moongazing.OrionPatch.Testing/` (`OrionPatch.Testing` package)

| File | Responsibility |
|------|----------------|
| `InMemoryOutboxStorage.cs` | Thread-safe in-memory `IOutboxStorage` for tests. |
| `InMemoryOutbox.cs` | `IOutbox` companion that goes through `InMemoryOutboxStorage` directly (no DbContext required). |
| `DeterministicDispatcher.cs` | A `DispatchOnceAsync(CancellationToken)` driver — no hosted service, called explicitly from tests. |
| `CapturingOutboxSink.cs` | `IOutboxSink` that records every dispatched envelope into a thread-safe list. |
| `TestClock.cs` | Settable `IOutboxDispatcherClock` for advancing time. |
| `OutboxAssertions.cs` | Fluent `AssertDispatched<T>(predicate)` helpers. |
| `DependencyInjection/OrionPatchTestingBuilderExtensions.cs` | `UseInMemory()` extension on `OrionPatchBuilder`. |
| `Moongazing.OrionPatch.Testing.csproj` | References core. |
| `docs/README.md` | NuGet readme. |
| `docs/logo.png` | NuGet PackageIcon. |

### `tests/`

| Project | Covers |
|---------|--------|
| `tests/Moongazing.OrionPatch.Tests/` | Core: options, builders, telemetry definitions, `ChannelOutboxSink`, `OutboxDispatcherHostedService` (with in-memory storage), backoff strategy. |
| `tests/Moongazing.OrionPatch.EntityFrameworkCore.Tests/` | SQLite-based integration tests of `EfCoreOutbox`, `EfCoreOutboxStorage`, interceptor flushing, claim/complete/fail/dead-letter cycle, lease expiry, concurrent dispatcher correctness. |
| `tests/Moongazing.OrionPatch.Testing.Tests/` | Validates the test helpers behave deterministically. |

### `sample/Moongazing.OrionPatch.Sample/`

| File | Responsibility |
|------|----------------|
| `Program.cs` | A minimal console host that wires `AddOrionPatch().UseEntityFrameworkCore<SampleDb>().UseChannelSink(...)` against an in-memory SQLite, enqueues a few `OrderConfirmed` messages, and prints what the channel sink received. |

### Repo root

| File | Responsibility |
|------|----------------|
| `Moongazing.OrionPatch.sln` | Classic `.sln` (not `.slnx`) so .NET 8/9 SDKs in the CI matrix can open it. |
| `Directory.Build.props` | Multi-target net8.0/9.0/10.0, family defaults (TreatWarningsAsErrors, MIT, packing metadata, Version=0.1.0). |
| `Directory.Build.targets` | Empty placeholder; reserved for future shared pack targets. |
| `Directory.Packages.props` | Central package management (EF Core, Microsoft.Extensions.*, xUnit, coverlet). |
| `NuGet.config` | Pin nuget.org as source. |
| `.gitignore` | dotnet defaults. |
| `LICENSE.txt` | MIT. |
| `README.md` | Public README, family-style. |
| `ROADMAP.md` | Public roadmap (already drafted in the spec; this file mirrors). |
| `CHANGELOG.md` | Keep-a-Changelog format with `[Unreleased]` header. |
| `docs/logo.png` | Master logo (deployed from `OrionPatchNewLogo.png` after user provides it). |
| `docs/icon.png` | Same image as `logo.png`; some packages prefer `icon.png` filename. |
| `.github/workflows/ci-cd.yml` | Matrix build + release-published publish to NuGet (copied from OrionLock). |
| `.github/FUNDING.yml` | Same as OrionLock. |

---

## Task 0: Repo bootstrap

**Files:**
- Create: `Moongazing.OrionPatch.sln`, `Directory.Build.props`, `Directory.Build.targets`, `Directory.Packages.props`, `NuGet.config`, `.gitignore`, `LICENSE.txt`, `README.md`, `ROADMAP.md`, `CHANGELOG.md`, `.github/workflows/ci-cd.yml`, `.github/FUNDING.yml`.

- [ ] **Step 1: Copy non-source family files from OrionLock**

Run from the OrionPatch repo root:

```bash
# Already in /c/Users/Tunahan Ali Ozturk/OneDrive - PEAKUP/Desktop/OrionPatch
cp "../OrionLock/.gitignore" .
cp "../OrionLock/LICENSE.txt" .
cp "../OrionLock/NuGet.config" .
cp "../OrionLock/Directory.Build.props" .
cp -r "../OrionLock/.github" .
```

Then open `Directory.Build.props` and change:
- `<RepositoryUrl>` to `https://github.com/tunahanaliozturk/OrionPatch`
- `<PackageProjectUrl>` to the same
- `<Version>` to `0.1.0`

Open `.github/workflows/ci-cd.yml` and change the `SOLUTION_PATH` env to `Moongazing.OrionPatch.sln`.

- [ ] **Step 2: Write `README.md`**

Family-style header (logo placeholder, NuGet badges for the three packages, .NET target chip), one-liner pitch, 30-second quick start, "Why OrionPatch" comparison table vs DIY/MassTransit/Wolverine, "What it does / what it doesn't" section, Roadmap pointer.

The 30-second quick start shows:

```csharp
services.AddDbContext<AppDbContext>(...);
services.AddOrionPatch()
    .UseEntityFrameworkCore<AppDbContext>()
    .UseSink<MyKafkaSink>();
```

Plus the enqueue example from the spec.

- [ ] **Step 3: Write `ROADMAP.md`**

Mirror the "Release plan" table from the spec, expand each milestone with its theme bullets. Same shape as the OrionLock ROADMAP.

- [ ] **Step 4: Write `CHANGELOG.md`**

```markdown
# Changelog

All notable changes to OrionPatch are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/) and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

(Initial v0.1.0 development in progress.)
```

- [ ] **Step 5: Write `Directory.Packages.props`**

```xml
<Project>
  <PropertyGroup>
    <ManagePackageVersionsCentrally>true</ManagePackageVersionsCentrally>
    <CentralPackageTransitivePinningEnabled>true</CentralPackageTransitivePinningEnabled>
  </PropertyGroup>
  <ItemGroup>
    <PackageVersion Include="Microsoft.EntityFrameworkCore" Version="8.0.10" />
    <PackageVersion Include="Microsoft.EntityFrameworkCore.Relational" Version="8.0.10" />
    <PackageVersion Include="Microsoft.EntityFrameworkCore.Sqlite" Version="8.0.10" />
    <PackageVersion Include="Microsoft.Extensions.DependencyInjection.Abstractions" Version="8.0.2" />
    <PackageVersion Include="Microsoft.Extensions.Hosting.Abstractions" Version="8.0.1" />
    <PackageVersion Include="Microsoft.Extensions.Logging.Abstractions" Version="8.0.2" />
    <PackageVersion Include="Microsoft.Extensions.Options" Version="8.0.2" />
    <PackageVersion Include="Microsoft.Extensions.Options.ConfigurationExtensions" Version="8.0.0" />
    <PackageVersion Include="System.Text.Json" Version="8.0.5" />
    <PackageVersion Include="xunit" Version="2.9.0" />
    <PackageVersion Include="xunit.runner.visualstudio" Version="2.8.2" />
    <PackageVersion Include="Microsoft.NET.Test.Sdk" Version="17.11.1" />
    <PackageVersion Include="coverlet.collector" Version="6.0.2" />
  </ItemGroup>
</Project>
```

- [ ] **Step 6: Create empty `Moongazing.OrionPatch.sln`**

```bash
dotnet new sln --name Moongazing.OrionPatch --format sln
```

The `--format sln` is required so the .NET 8 leg of the CI matrix can open it.

- [ ] **Step 7: `git init` + initial commit**

```bash
git init
git checkout -b main
git add .
git commit -m "chore: bootstrap repository skeleton

Initial commit: README, ROADMAP, CHANGELOG, Directory.Build.props,
Directory.Packages.props, NuGet.config, LICENSE, .gitignore, CI workflow,
empty solution file. No source code yet."
```

- [ ] **Step 8: Create GitHub repo and push**

```bash
gh repo create tunahanaliozturk/OrionPatch --public --description "Transactional outbox primitive for .NET. EF Core storage, pluggable IOutboxSink, at-least-once dispatch."
git remote add origin https://github.com/tunahanaliozturk/OrionPatch.git
git push -u origin main
```

---

## Task 1: Core abstractions and models

**Files:**
- Create: `src/Moongazing.OrionPatch/Moongazing.OrionPatch.csproj`
- Create: `src/Moongazing.OrionPatch/Abstractions/IOutbox.cs`
- Create: `src/Moongazing.OrionPatch/Abstractions/IOutboxSink.cs`
- Create: `src/Moongazing.OrionPatch/Abstractions/IOutboxStorage.cs`
- Create: `src/Moongazing.OrionPatch/Abstractions/IOutboxDispatcherClock.cs`
- Create: `src/Moongazing.OrionPatch/Models/OutboxEnvelope.cs`
- Create: `src/Moongazing.OrionPatch/Models/OutboxRow.cs`
- Create: `src/Moongazing.OrionPatch/Models/OutboxEnqueueOptions.cs`
- Create: `src/Moongazing.OrionPatch/Models/OutboxStatus.cs`
- Test: `tests/Moongazing.OrionPatch.Tests/Moongazing.OrionPatch.Tests.csproj`
- Test: `tests/Moongazing.OrionPatch.Tests/Models/OutboxEnvelopeTests.cs`

- [ ] **Step 1: Write the csproj**

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <PackageId>OrionPatch</PackageId>
    <Description>Transactional outbox primitive for .NET. Enqueue messages inside an EF Core SaveChanges transaction; a background dispatcher hands them to a pluggable IOutboxSink at-least-once. Ships ChannelOutboxSink (in-process); broker sinks are opt-in sub-packages.</Description>
    <PackageTags>outbox;messaging;ef-core;at-least-once;transactional;dispatcher</PackageTags>
    <PackageIcon>icon.png</PackageIcon>
    <PackageReadmeFile>docs/README.md</PackageReadmeFile>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Microsoft.Extensions.DependencyInjection.Abstractions" />
    <PackageReference Include="Microsoft.Extensions.Options" />
    <PackageReference Include="Microsoft.Extensions.Logging.Abstractions" />
  </ItemGroup>
  <ItemGroup>
    <None Include="..\..\docs\icon.png" Pack="true" PackagePath="" Visible="false" />
    <None Include="docs/README.md" Pack="true" PackagePath="docs/" />
  </ItemGroup>
</Project>
```

- [ ] **Step 2: Write the failing test for `OutboxEnvelope`**

```csharp
public class OutboxEnvelopeTests
{
    [Fact]
    public void Constructor_ShouldExposeAllValues_WhenAllProvided()
    {
        var id = Guid.NewGuid();
        var headers = new Dictionary<string, string> { ["k"] = "v" };
        var env = new OutboxEnvelope(
            Id: id,
            MessageType: "App.OrderConfirmed",
            Payload: "{\"orderId\":1}",
            Headers: headers,
            CorrelationId: "corr-1",
            OccurredAtUtc: new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc),
            AttemptNumber: 1);

        Assert.Equal(id, env.Id);
        Assert.Equal("App.OrderConfirmed", env.MessageType);
        Assert.Equal("{\"orderId\":1}", env.Payload);
        Assert.NotNull(env.Headers);
        Assert.Equal("v", env.Headers!["k"]);
        Assert.Equal("corr-1", env.CorrelationId);
        Assert.Equal(DateTimeKind.Utc, env.OccurredAtUtc.Kind);
        Assert.Equal(1, env.AttemptNumber);
    }
}
```

Run: `dotnet test tests/Moongazing.OrionPatch.Tests --filter FullyQualifiedName~OutboxEnvelopeTests`
Expected: FAIL — `OutboxEnvelope` does not exist.

- [ ] **Step 3: Define `OutboxEnvelope`, `OutboxRow`, `OutboxStatus`, `OutboxEnqueueOptions`**

```csharp
// OutboxStatus.cs
namespace Moongazing.OrionPatch.Models;
public enum OutboxStatus : byte
{
    Pending = 0,
    Claimed = 1,
    Processed = 2,
    DeadLettered = 3,
}
```

```csharp
// OutboxEnvelope.cs
namespace Moongazing.OrionPatch.Models;
public sealed record OutboxEnvelope(
    Guid Id,
    string MessageType,
    string Payload,
    IReadOnlyDictionary<string, string>? Headers,
    string? CorrelationId,
    DateTime OccurredAtUtc,
    int AttemptNumber);
```

```csharp
// OutboxRow.cs (the storage-facing view; serialization to a DB row lives in the EF Core package)
namespace Moongazing.OrionPatch.Models;
public sealed class OutboxRow
{
    public Guid Id { get; init; }
    public string MessageType { get; init; } = default!;
    public string Payload { get; init; } = default!;
    public string? HeadersJson { get; init; }
    public string? CorrelationId { get; init; }
    public DateTime OccurredAtUtc { get; init; }
    public DateTime EnqueuedAtUtc { get; init; }
    public OutboxStatus Status { get; set; }
    public int AttemptCount { get; set; }
    public DateTime? ClaimedAtUtc { get; set; }
    public string? ClaimedBy { get; set; }
    public string? LastError { get; set; }
    public DateTime? ProcessedAtUtc { get; set; }
    public DateTime? NextAttemptAtUtc { get; set; }
}
```

```csharp
// OutboxEnqueueOptions.cs
namespace Moongazing.OrionPatch.Models;
public sealed class OutboxEnqueueOptions
{
    public string? MessageType { get; init; }
    public string? CorrelationId { get; init; }
    public IReadOnlyDictionary<string, string>? Headers { get; init; }
    public DateTime? OccurredAtUtc { get; init; }
}
```

- [ ] **Step 4: Define the four interfaces**

```csharp
// IOutbox.cs
namespace Moongazing.OrionPatch.Abstractions;
public interface IOutbox
{
    void Enqueue<T>(T message, OutboxEnqueueOptions? options = null) where T : class;
}
```

```csharp
// IOutboxSink.cs
namespace Moongazing.OrionPatch.Abstractions;
public interface IOutboxSink
{
    Task SendAsync(OutboxEnvelope envelope, CancellationToken ct);
}
```

```csharp
// IOutboxStorage.cs
namespace Moongazing.OrionPatch.Abstractions;
public interface IOutboxStorage
{
    Task AppendAsync(IReadOnlyList<OutboxRow> rows, CancellationToken ct);
    Task<IReadOnlyList<OutboxRow>> ClaimNextAsync(int batchSize, string dispatcherIdentity, TimeSpan leaseDuration, CancellationToken ct);
    Task CompleteAsync(Guid rowId, DateTime processedAtUtc, CancellationToken ct);
    Task FailAsync(Guid rowId, string error, DateTime? nextAttemptAtUtc, bool deadLetter, CancellationToken ct);
    Task<long> QueueDepthAsync(CancellationToken ct);
}
```

```csharp
// IOutboxDispatcherClock.cs
namespace Moongazing.OrionPatch.Abstractions;
public interface IOutboxDispatcherClock
{
    DateTime UtcNow { get; }
    Task DelayAsync(TimeSpan duration, CancellationToken ct);
}
```

- [ ] **Step 5: Run the test, verify it passes**

Run: `dotnet test tests/Moongazing.OrionPatch.Tests --filter FullyQualifiedName~OutboxEnvelopeTests`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/Moongazing.OrionPatch tests/Moongazing.OrionPatch.Tests Moongazing.OrionPatch.sln
git commit -m "feat(core): outbox abstractions and models

Add IOutbox, IOutboxSink, IOutboxStorage, IOutboxDispatcherClock
contracts plus OutboxEnvelope/OutboxRow/OutboxEnqueueOptions/OutboxStatus
models. No behaviour yet; just the shapes the rest of the library will
implement against."
```

---

## Task 2: Options, telemetry, and backoff

**Files:**
- Create: `src/Moongazing.OrionPatch/Configuration/OrionPatchOptions.cs`
- Create: `src/Moongazing.OrionPatch/Configuration/BackoffStrategy.cs`
- Create: `src/Moongazing.OrionPatch/Telemetry/OrionPatchDiagnostics.cs`
- Create: `src/Moongazing.OrionPatch/Internal/SystemClock.cs`
- Create: `src/Moongazing.OrionPatch/Internal/DefaultDispatcherIdentity.cs`
- Create: `src/Moongazing.OrionPatch/Internal/MessageSerializer.cs`
- Create: `src/Moongazing.OrionPatch/Internal/MessageTypeNameResolver.cs`
- Test: `tests/Moongazing.OrionPatch.Tests/Configuration/BackoffStrategyTests.cs`
- Test: `tests/Moongazing.OrionPatch.Tests/Configuration/OrionPatchOptionsTests.cs`
- Test: `tests/Moongazing.OrionPatch.Tests/Internal/MessageTypeNameResolverTests.cs`

- [ ] **Step 1: Failing test for `BackoffStrategy.Exponential`**

```csharp
public class BackoffStrategyTests
{
    [Fact]
    public void Exponential_ShouldDouble_UntilMax()
    {
        var b = BackoffStrategy.Exponential(initial: TimeSpan.FromSeconds(1), max: TimeSpan.FromSeconds(30));
        Assert.Equal(TimeSpan.FromSeconds(1),  b(1));
        Assert.Equal(TimeSpan.FromSeconds(2),  b(2));
        Assert.Equal(TimeSpan.FromSeconds(4),  b(3));
        Assert.Equal(TimeSpan.FromSeconds(8),  b(4));
        Assert.Equal(TimeSpan.FromSeconds(16), b(5));
        Assert.Equal(TimeSpan.FromSeconds(30), b(6));
        Assert.Equal(TimeSpan.FromSeconds(30), b(20));
    }

    [Fact]
    public void Fixed_ShouldReturnSameDelay_ForEveryAttempt()
    {
        var b = BackoffStrategy.Fixed(TimeSpan.FromSeconds(2));
        Assert.Equal(TimeSpan.FromSeconds(2), b(1));
        Assert.Equal(TimeSpan.FromSeconds(2), b(99));
    }
}
```

Run, expect FAIL.

- [ ] **Step 2: Implement `BackoffStrategy`**

```csharp
namespace Moongazing.OrionPatch.Configuration;
public static class BackoffStrategy
{
    public static Func<int, TimeSpan> Exponential(TimeSpan initial, TimeSpan max) =>
        attempt =>
        {
            if (attempt <= 0) return TimeSpan.Zero;
            var ticks = initial.Ticks * (1L << Math.Min(attempt - 1, 30));
            return TimeSpan.FromTicks(Math.Min(ticks, max.Ticks));
        };

    public static Func<int, TimeSpan> Fixed(TimeSpan delay) => _ => delay;
}
```

Run, expect PASS.

- [ ] **Step 3: Failing test for `OrionPatchOptions` defaults**

```csharp
public class OrionPatchOptionsTests
{
    [Fact]
    public void Defaults_ShouldMatchSpec_WhenConstructed()
    {
        var o = new OrionPatchOptions();
        Assert.Equal(TimeSpan.FromSeconds(1), o.PollingInterval);
        Assert.Equal(50, o.BatchSize);
        Assert.Equal(5, o.MaxAttempts);
        Assert.Equal(TimeSpan.FromMinutes(2), o.LeaseDuration);
        Assert.True(o.DispatcherEnabled);
        Assert.NotNull(o.BackoffStrategy);
        Assert.Equal(TimeSpan.FromSeconds(1), o.BackoffStrategy(1));
        Assert.NotNull(o.DispatcherIdentityFactory);
    }
}
```

Run, expect FAIL.

- [ ] **Step 4: Implement `OrionPatchOptions`**

```csharp
namespace Moongazing.OrionPatch.Configuration;
public sealed class OrionPatchOptions
{
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(1);
    public int BatchSize { get; set; } = 50;
    public int MaxAttempts { get; set; } = 5;
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromMinutes(2);
    public bool DispatcherEnabled { get; set; } = true;
    public Func<int, TimeSpan> BackoffStrategy { get; set; } =
        Configuration.BackoffStrategy.Exponential(TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(30));
    public Func<string> DispatcherIdentityFactory { get; set; } =
        () => Internal.DefaultDispatcherIdentity.Create();
    public JsonSerializerOptions JsonOptions { get; set; } = new(JsonSerializerDefaults.Web);
}
```

Run, expect PASS.

- [ ] **Step 5: Failing test for `MessageTypeNameResolver`**

```csharp
public class MessageTypeNameResolverTests
{
    private sealed class SampleEvent { }

    [Fact]
    public void Resolve_ShouldReturnFullName_WhenNoOverride()
    {
        var r = new MessageTypeNameResolver();
        Assert.Equal(typeof(SampleEvent).FullName, r.Resolve(typeof(SampleEvent), options: null));
    }

    [Fact]
    public void Resolve_ShouldReturnOverride_WhenSet()
    {
        var r = new MessageTypeNameResolver();
        var opts = new OutboxEnqueueOptions { MessageType = "App.Custom" };
        Assert.Equal("App.Custom", r.Resolve(typeof(SampleEvent), opts));
    }
}
```

Run, expect FAIL.

- [ ] **Step 6: Implement `MessageTypeNameResolver`, `MessageSerializer`, `SystemClock`, `DefaultDispatcherIdentity`**

```csharp
namespace Moongazing.OrionPatch.Internal;
internal sealed class MessageTypeNameResolver
{
    public string Resolve(Type type, OutboxEnqueueOptions? options) =>
        options?.MessageType ?? type.FullName ?? type.Name;
}
```

```csharp
namespace Moongazing.OrionPatch.Internal;
internal sealed class MessageSerializer(JsonSerializerOptions jsonOptions)
{
    public string Serialize<T>(T value) where T : class =>
        JsonSerializer.Serialize(value, value.GetType(), jsonOptions);
}
```

```csharp
namespace Moongazing.OrionPatch.Internal;
internal sealed class SystemClock : IOutboxDispatcherClock
{
    public DateTime UtcNow => DateTime.UtcNow;
    public Task DelayAsync(TimeSpan duration, CancellationToken ct) => Task.Delay(duration, ct);
}
```

```csharp
namespace Moongazing.OrionPatch.Internal;
internal static class DefaultDispatcherIdentity
{
    public static string Create() =>
        $"{Environment.MachineName}/{Environment.ProcessId}";
}
```

Run BackoffStrategy + Options + MessageTypeNameResolver tests, expect PASS.

- [ ] **Step 7: Write `OrionPatchDiagnostics`**

```csharp
namespace Moongazing.OrionPatch.Telemetry;
public static class OrionPatchDiagnostics
{
    public const string SourceName = "Moongazing.OrionPatch";

    public static readonly ActivitySource ActivitySource = new(SourceName);
    public static readonly Meter Meter = new(SourceName);

    public static readonly Counter<long> Enqueued     = Meter.CreateCounter<long>("orionpatch.outbox.enqueued");
    public static readonly Counter<long> Dispatched   = Meter.CreateCounter<long>("orionpatch.outbox.dispatched");
    public static readonly Counter<long> Failed       = Meter.CreateCounter<long>("orionpatch.outbox.failed");
    public static readonly Counter<long> DeadLettered = Meter.CreateCounter<long>("orionpatch.outbox.deadlettered");
    public static readonly Counter<long> Attempts     = Meter.CreateCounter<long>("orionpatch.outbox.attempts");
    public static readonly Histogram<double> DispatchDuration = Meter.CreateHistogram<double>("orionpatch.outbox.dispatch.duration", unit: "ms");
}
```

(A separate test will run later validating instrument names; for this task just ensure it compiles.)

- [ ] **Step 8: Build and commit**

```bash
dotnet build src/Moongazing.OrionPatch
dotnet test tests/Moongazing.OrionPatch.Tests
git add .
git commit -m "feat(core): options, backoff, telemetry, internal helpers

OrionPatchOptions with documented defaults (1s polling, batch 50,
5 attempts, 2min lease, exponential 1s..30min backoff).
BackoffStrategy.Exponential/Fixed factories.
MessageTypeNameResolver, MessageSerializer, SystemClock,
DefaultDispatcherIdentity, OrionPatchDiagnostics (ActivitySource +
Meter + instruments)."
```

---

## Task 3: ChannelOutboxSink + DI

**Files:**
- Create: `src/Moongazing.OrionPatch/Channel/ChannelOutboxSink.cs`
- Create: `src/Moongazing.OrionPatch/Channel/ChannelOutboxSinkOptions.cs`
- Create: `src/Moongazing.OrionPatch/DependencyInjection/OrionPatchBuilder.cs`
- Create: `src/Moongazing.OrionPatch/DependencyInjection/OrionPatchServiceCollectionExtensions.cs`
- Create: `src/Moongazing.OrionPatch/DependencyInjection/OutboxBuilderExtensions.cs`
- Test: `tests/Moongazing.OrionPatch.Tests/Channel/ChannelOutboxSinkTests.cs`
- Test: `tests/Moongazing.OrionPatch.Tests/DependencyInjection/AddOrionPatchTests.cs`

- [ ] **Step 1: Failing test for `ChannelOutboxSink` round-trip**

```csharp
public class ChannelOutboxSinkTests
{
    [Fact]
    public async Task SendAsync_ShouldMakeEnvelopeReadable_WhenReaderConsumes()
    {
        var sink = new ChannelOutboxSink(new ChannelOutboxSinkOptions { Capacity = 8 });
        var env = new OutboxEnvelope(Guid.NewGuid(), "T", "{}", null, null, DateTime.UtcNow, 1);

        await sink.SendAsync(env, default);
        var read = await sink.Reader.ReadAsync();

        Assert.Equal(env.Id, read.Id);
    }

    [Fact]
    public async Task SendAsync_ShouldBlock_WhenChannelIsFullAndModeIsWait()
    {
        var sink = new ChannelOutboxSink(new ChannelOutboxSinkOptions
        {
            Capacity = 1,
            FullMode = BoundedChannelFullMode.Wait,
        });
        var env = new OutboxEnvelope(Guid.NewGuid(), "T", "{}", null, null, DateTime.UtcNow, 1);

        await sink.SendAsync(env, default);
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        await Assert.ThrowsAsync<OperationCanceledException>(() => sink.SendAsync(env, cts.Token));
    }
}
```

Run, expect FAIL.

- [ ] **Step 2: Implement `ChannelOutboxSink` + options**

```csharp
namespace Moongazing.OrionPatch.Channel;
public sealed class ChannelOutboxSinkOptions
{
    public int Capacity { get; init; } = 1000;
    public BoundedChannelFullMode FullMode { get; init; } = BoundedChannelFullMode.Wait;
}

public sealed class ChannelOutboxSink : IOutboxSink
{
    private readonly Channel<OutboxEnvelope> _channel;

    public ChannelOutboxSink(ChannelOutboxSinkOptions options)
    {
        _channel = System.Threading.Channels.Channel.CreateBounded<OutboxEnvelope>(
            new BoundedChannelOptions(options.Capacity)
            {
                FullMode = options.FullMode,
                SingleReader = false,
                SingleWriter = false,
            });
    }

    public ChannelReader<OutboxEnvelope> Reader => _channel.Reader;

    public Task SendAsync(OutboxEnvelope envelope, CancellationToken ct) =>
        _channel.Writer.WriteAsync(envelope, ct).AsTask();
}
```

Run, expect PASS.

- [ ] **Step 3: Failing test for `AddOrionPatch().UseChannelSink(...)`**

```csharp
public class AddOrionPatchTests
{
    [Fact]
    public void AddOrionPatch_ShouldRegisterOptionsAndChannelSink_WhenUseChannelSinkCalled()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddOrionPatch().UseChannelSink(o => o.Capacity = 256);

        var sp = services.BuildServiceProvider();
        var opts = sp.GetRequiredService<IOptions<OrionPatchOptions>>().Value;
        Assert.NotNull(opts);

        var sink = sp.GetRequiredService<IOutboxSink>();
        Assert.IsType<ChannelOutboxSink>(sink);
    }
}
```

Run, expect FAIL.

- [ ] **Step 4: Implement builder + DI extensions**

```csharp
namespace Moongazing.OrionPatch.DependencyInjection;
public sealed class OrionPatchBuilder(IServiceCollection services)
{
    public IServiceCollection Services { get; } = services;
}

public static class OrionPatchServiceCollectionExtensions
{
    public static OrionPatchBuilder AddOrionPatch(
        this IServiceCollection services,
        Action<OrionPatchOptions>? configure = null)
    {
        services.AddOptions();
        if (configure is not null) services.Configure(configure);
        services.TryAddSingleton<IOutboxDispatcherClock, SystemClock>();
        services.TryAddSingleton<MessageTypeNameResolver>();
        services.TryAddSingleton(sp => new MessageSerializer(
            sp.GetRequiredService<IOptions<OrionPatchOptions>>().Value.JsonOptions));
        return new OrionPatchBuilder(services);
    }
}

public static class OutboxBuilderExtensions
{
    public static OrionPatchBuilder UseSink<TSink>(this OrionPatchBuilder b)
        where TSink : class, IOutboxSink
    {
        b.Services.AddSingleton<IOutboxSink, TSink>();
        return b;
    }

    public static OrionPatchBuilder UseChannelSink(this OrionPatchBuilder b, Action<ChannelOutboxSinkOptions>? configure = null)
    {
        var opts = new ChannelOutboxSinkOptions();
        configure?.Invoke(opts);
        b.Services.AddSingleton(opts);
        b.Services.AddSingleton<ChannelOutboxSink>();
        b.Services.AddSingleton<IOutboxSink>(sp => sp.GetRequiredService<ChannelOutboxSink>());
        return b;
    }
}
```

Run, expect PASS.

- [ ] **Step 5: Commit**

```bash
git add src/Moongazing.OrionPatch tests/Moongazing.OrionPatch.Tests
git commit -m "feat(core): ChannelOutboxSink + AddOrionPatch DI"
```

---

## Task 4: Dispatcher hosted service (against in-memory storage)

**Files:**
- Create: `src/Moongazing.OrionPatch/Hosting/OutboxDispatcherHostedService.cs`
- Modify: `src/Moongazing.OrionPatch/DependencyInjection/OrionPatchServiceCollectionExtensions.cs` (register hosted service when `DispatcherEnabled`)
- Test: `tests/Moongazing.OrionPatch.Tests/Hosting/OutboxDispatcherHostedServiceTests.cs`
- Test helpers: a private `InMemoryStorage` + `CapturingSink` local to the test file (lives in the test project, not the Testing package — that's Task 7).

- [ ] **Step 1: Failing test — dispatcher claims, dispatches, completes**

```csharp
public class OutboxDispatcherHostedServiceTests
{
    [Fact]
    public async Task ExecuteAsync_ShouldClaimDispatchAndComplete_WhenStorageHasRows()
    {
        var storage = new InMemoryStorage();
        await storage.AppendAsync(new[]
        {
            NewRow(Guid.NewGuid(), "T", "{}")
        }, default);

        var sink = new CapturingSink();
        var clock = new TestClock();

        var options = new OrionPatchOptions { PollingInterval = TimeSpan.FromMilliseconds(10) };
        var svc = new OutboxDispatcherHostedService(
            storage,
            sink,
            Options.Create(options),
            clock,
            NullLogger<OutboxDispatcherHostedService>.Instance);

        using var cts = new CancellationTokenSource();
        var run = svc.StartAsync(cts.Token);

        await WaitFor(() => sink.Dispatched.Count == 1, TimeSpan.FromSeconds(2));

        await cts.CancelAsync();
        await svc.StopAsync(default);

        Assert.Single(sink.Dispatched);
        Assert.Single(storage.Rows.Where(r => r.Status == OutboxStatus.Processed));
    }

    [Fact]
    public async Task ExecuteAsync_ShouldFailRowWithRetry_WhenSinkThrows()
    {
        var storage = new InMemoryStorage();
        var rowId = Guid.NewGuid();
        await storage.AppendAsync(new[] { NewRow(rowId, "T", "{}") }, default);
        var sink = new ThrowingSink();
        var options = new OrionPatchOptions
        {
            PollingInterval = TimeSpan.FromMilliseconds(10),
            MaxAttempts = 3,
        };

        var svc = new OutboxDispatcherHostedService(storage, sink, Options.Create(options), new SystemClock(), NullLogger<OutboxDispatcherHostedService>.Instance);

        using var cts = new CancellationTokenSource();
        await svc.StartAsync(cts.Token);

        await WaitFor(() => storage.Rows.Single(r => r.Id == rowId).AttemptCount >= 1, TimeSpan.FromSeconds(2));

        await cts.CancelAsync();
        await svc.StopAsync(default);

        var row = storage.Rows.Single(r => r.Id == rowId);
        Assert.True(row.AttemptCount >= 1);
        Assert.NotNull(row.LastError);
        Assert.NotNull(row.NextAttemptAtUtc);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldDeadLetter_WhenAttemptsExceedMax()
    {
        // Pre-condition: row.AttemptCount == MaxAttempts; sink throws.
        // Expect Status flips to DeadLettered after the next attempt.
        // (Full body: same shape as above with an asserter on Status.)
    }

    private static OutboxRow NewRow(Guid id, string type, string payload) => new()
    {
        Id = id,
        MessageType = type,
        Payload = payload,
        OccurredAtUtc = DateTime.UtcNow,
        EnqueuedAtUtc = DateTime.UtcNow,
        Status = OutboxStatus.Pending,
    };

    private static async Task WaitFor(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition()) return;
            await Task.Delay(20);
        }
        throw new TimeoutException("Condition was not satisfied.");
    }
}
```

The `InMemoryStorage`, `CapturingSink`, `ThrowingSink`, `TestClock` are private helpers in this same test file.

Run, expect FAIL.

- [ ] **Step 2: Implement `OutboxDispatcherHostedService`**

```csharp
namespace Moongazing.OrionPatch.Hosting;
public sealed class OutboxDispatcherHostedService : BackgroundService
{
    private readonly IOutboxStorage _storage;
    private readonly IOutboxSink _sink;
    private readonly IOptionsMonitor<OrionPatchOptions> _optionsMonitor;
    private readonly IOutboxDispatcherClock _clock;
    private readonly ILogger<OutboxDispatcherHostedService> _logger;

    public OutboxDispatcherHostedService(
        IOutboxStorage storage,
        IOutboxSink sink,
        IOptions<OrionPatchOptions> options,
        IOutboxDispatcherClock clock,
        ILogger<OutboxDispatcherHostedService> logger)
        : this(storage, sink, new MonotonicMonitor(options), clock, logger) { }

    internal OutboxDispatcherHostedService(
        IOutboxStorage storage,
        IOutboxSink sink,
        IOptionsMonitor<OrionPatchOptions> options,
        IOutboxDispatcherClock clock,
        ILogger<OutboxDispatcherHostedService> logger)
    {
        _storage = storage;
        _sink = sink;
        _optionsMonitor = options;
        _clock = clock;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var identity = _optionsMonitor.CurrentValue.DispatcherIdentityFactory();
        while (!stoppingToken.IsCancellationRequested)
        {
            var opts = _optionsMonitor.CurrentValue;
            try
            {
                var batch = await _storage.ClaimNextAsync(opts.BatchSize, identity, opts.LeaseDuration, stoppingToken);
                if (batch.Count == 0)
                {
                    await _clock.DelayAsync(opts.PollingInterval, stoppingToken);
                    continue;
                }

                foreach (var row in batch)
                {
                    await DispatchOneAsync(row, opts, stoppingToken);
                }
            }
            catch (OperationCanceledException) { /* shutting down */ }
            catch (Exception ex)
            {
                _logger.LogError(ex, "OrionPatch dispatcher loop failure; backing off");
                await _clock.DelayAsync(opts.PollingInterval, stoppingToken);
            }
        }
    }

    private async Task DispatchOneAsync(OutboxRow row, OrionPatchOptions opts, CancellationToken ct)
    {
        var attempt = row.AttemptCount + 1;
        OrionPatchDiagnostics.Attempts.Add(1);
        var sw = Stopwatch.StartNew();
        using var activity = OrionPatchDiagnostics.ActivitySource.StartActivity("OrionPatch.Dispatch");
        activity?.SetTag("orionpatch.message.type", row.MessageType);
        activity?.SetTag("orionpatch.attempt", attempt);
        try
        {
            var headers = row.HeadersJson is null
                ? null
                : JsonSerializer.Deserialize<Dictionary<string, string>>(row.HeadersJson, opts.JsonOptions);
            var envelope = new OutboxEnvelope(
                row.Id, row.MessageType, row.Payload, headers,
                row.CorrelationId, row.OccurredAtUtc, attempt);

            await _sink.SendAsync(envelope, ct);
            await _storage.CompleteAsync(row.Id, _clock.UtcNow, ct);
            OrionPatchDiagnostics.Dispatched.Add(1);
            OrionPatchDiagnostics.DispatchDuration.Record(sw.Elapsed.TotalMilliseconds);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            var deadLetter = attempt >= opts.MaxAttempts;
            var next = deadLetter ? (DateTime?)null : _clock.UtcNow.Add(opts.BackoffStrategy(attempt));
            await _storage.FailAsync(row.Id, Truncate(ex.ToString(), 4000), next, deadLetter, ct);
            if (deadLetter) OrionPatchDiagnostics.DeadLettered.Add(1);
            else OrionPatchDiagnostics.Failed.Add(1);
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
        }
    }

    private static string Truncate(string s, int max) => s.Length <= max ? s : s[..max];

    private sealed class MonotonicMonitor(IOptions<OrionPatchOptions> options) : IOptionsMonitor<OrionPatchOptions>
    {
        public OrionPatchOptions CurrentValue => options.Value;
        public OrionPatchOptions Get(string? name) => options.Value;
        public IDisposable? OnChange(Action<OrionPatchOptions, string?> listener) => null;
    }
}
```

- [ ] **Step 3: Register hosted service in DI when `DispatcherEnabled`**

In `OrionPatchServiceCollectionExtensions.AddOrionPatch`, after building the options and before returning, add a `HostedService` registration guarded by an early-bound options snapshot:

```csharp
services.AddSingleton<IHostedService>(sp =>
{
    var opts = sp.GetRequiredService<IOptions<OrionPatchOptions>>().Value;
    if (!opts.DispatcherEnabled)
        return new NoOpHostedService();
    return ActivatorUtilities.CreateInstance<OutboxDispatcherHostedService>(sp);
});
```

`NoOpHostedService` is a tiny internal sealed class with empty `StartAsync`/`StopAsync`.

(Add `Microsoft.Extensions.Hosting.Abstractions` PackageReference to the core csproj.)

- [ ] **Step 4: Run all tests**

Run: `dotnet test tests/Moongazing.OrionPatch.Tests`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add .
git commit -m "feat(core): OutboxDispatcherHostedService with retry + dead-letter

Background loop: claim batch from storage, dispatch each envelope
through the sink, complete on success, fail with exponential backoff
on exception, dead-letter after MaxAttempts. Telemetry counters and
dispatch-duration histogram populated on each path."
```

---

## Task 5: EF Core entity + interceptor (enqueue path)

**Files:**
- Create: `src/Moongazing.OrionPatch.EntityFrameworkCore/Moongazing.OrionPatch.EntityFrameworkCore.csproj`
- Create: `src/Moongazing.OrionPatch.EntityFrameworkCore/Entities/OutboxRow.cs` (EF entity, separate from core's models class — same name but EF-decorated; see note below).
- Create: `src/Moongazing.OrionPatch.EntityFrameworkCore/Configuration/OutboxEntityConfiguration.cs`
- Create: `src/Moongazing.OrionPatch.EntityFrameworkCore/OrionPatchDbContextExtensions.cs`
- Create: `src/Moongazing.OrionPatch.EntityFrameworkCore/EfCoreOutbox.cs`
- Create: `src/Moongazing.OrionPatch.EntityFrameworkCore/OrionPatchSaveChangesInterceptor.cs`
- Create: `src/Moongazing.OrionPatch.EntityFrameworkCore/DependencyInjection/OrionPatchEntityFrameworkCoreBuilderExtensions.cs`
- Test: `tests/Moongazing.OrionPatch.EntityFrameworkCore.Tests/Moongazing.OrionPatch.EntityFrameworkCore.Tests.csproj`
- Test: `tests/Moongazing.OrionPatch.EntityFrameworkCore.Tests/EnqueueInterceptorTests.cs`

**Note on the two `OutboxRow`:** the EF entity in this project IS the same DTO as core's `Models.OutboxRow`. To avoid duplicating, the EF package references core and uses `Models.OutboxRow` directly with a non-attribute `IEntityTypeConfiguration` mapping. (No EF attributes on the core type, so core stays EF-free.)

- [ ] **Step 1: Write the EF csproj**

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <PackageId>OrionPatch.EntityFrameworkCore</PackageId>
    <Description>EF Core storage backend for OrionPatch. Adds the OrionPatch_Outbox table with provider-aware competing-consumers claim (SKIP LOCKED on supported providers, optimistic-concurrency fallback for SQLite).</Description>
    <PackageTags>outbox;ef-core;entity-framework;orionpatch;transactional</PackageTags>
    <PackageIcon>icon.png</PackageIcon>
    <PackageReadmeFile>docs/README.md</PackageReadmeFile>
  </PropertyGroup>
  <ItemGroup>
    <ProjectReference Include="..\Moongazing.OrionPatch\Moongazing.OrionPatch.csproj" />
    <PackageReference Include="Microsoft.EntityFrameworkCore" />
    <PackageReference Include="Microsoft.EntityFrameworkCore.Relational" />
  </ItemGroup>
  <ItemGroup>
    <None Include="..\..\docs\icon.png" Pack="true" PackagePath="" Visible="false" />
    <None Include="docs/README.md" Pack="true" PackagePath="docs/" />
  </ItemGroup>
</Project>
```

- [ ] **Step 2: Write `OutboxEntityConfiguration`**

```csharp
internal sealed class OutboxEntityConfiguration : IEntityTypeConfiguration<OutboxRow>
{
    public void Configure(EntityTypeBuilder<OutboxRow> b)
    {
        b.ToTable("OrionPatch_Outbox");
        b.HasKey(x => x.Id);
        b.Property(x => x.MessageType).HasMaxLength(256).IsRequired();
        b.Property(x => x.Payload).IsRequired();
        b.Property(x => x.HeadersJson);
        b.Property(x => x.CorrelationId).HasMaxLength(128);
        b.Property(x => x.OccurredAtUtc).IsRequired();
        b.Property(x => x.EnqueuedAtUtc).IsRequired();
        b.Property(x => x.Status).IsRequired();
        b.Property(x => x.AttemptCount).IsRequired();
        b.Property(x => x.ClaimedAtUtc);
        b.Property(x => x.ClaimedBy).HasMaxLength(128);
        b.Property(x => x.LastError);
        b.Property(x => x.ProcessedAtUtc);
        b.Property(x => x.NextAttemptAtUtc);
        b.Property<byte[]>("RowVersion").IsRowVersion();

        b.HasIndex(x => new { x.Status, x.NextAttemptAtUtc })
            .HasDatabaseName("IX_OrionPatch_Outbox_Status_NextAttempt");
        b.HasIndex(x => x.ClaimedAtUtc)
            .HasDatabaseName("IX_OrionPatch_Outbox_ClaimedAt");
    }
}
```

`OrionPatchDbContextExtensions`:

```csharp
public static class OrionPatchDbContextExtensions
{
    public static ModelBuilder ApplyOrionPatchConfiguration(this ModelBuilder b)
    {
        b.ApplyConfiguration(new OutboxEntityConfiguration());
        return b;
    }
}
```

- [ ] **Step 3: Failing test — interceptor enqueues row in the user's transaction**

```csharp
public class EnqueueInterceptorTests
{
    [Fact]
    public async Task SaveChanges_ShouldPersistOutboxRow_WhenEnqueued()
    {
        await using var db = TestDb.Create();

        var outbox = new EfCoreOutbox(db, new MessageTypeNameResolver(), new MessageSerializer(new()));
        outbox.Enqueue(new OrderConfirmed(Guid.NewGuid(), 42));

        db.Add(new Sample { Id = Guid.NewGuid(), Name = "x" });
        await db.SaveChangesAsync();

        var rows = await db.Set<OutboxRow>().ToListAsync();
        Assert.Single(rows);
        Assert.Equal(typeof(OrderConfirmed).FullName, rows[0].MessageType);
        Assert.Contains("42", rows[0].Payload);
    }

    [Fact]
    public async Task Rollback_ShouldNotPersistOutboxRow_WhenSaveChangesFails()
    {
        await using var db = TestDb.Create();
        var outbox = new EfCoreOutbox(db, new MessageTypeNameResolver(), new MessageSerializer(new()));
        outbox.Enqueue(new OrderConfirmed(Guid.NewGuid(), 1));

        // Force a save failure by adding an invalid Sample (e.g. null required Name on a model with [Required]).
        // Or wrap in a manual transaction and roll back.
        await using (var tx = await db.Database.BeginTransactionAsync())
        {
            db.Add(new Sample { Id = Guid.NewGuid(), Name = "x" });
            await db.SaveChangesAsync();
            await tx.RollbackAsync();
        }

        var rows = await db.Set<OutboxRow>().ToListAsync();
        Assert.Empty(rows);
    }
}
```

`TestDb.Create` builds an in-memory SQLite `AppDbContext` with `ApplyOrionPatchConfiguration` + a tiny `Sample` entity, registers `OrionPatchSaveChangesInterceptor`, and ensures the schema is created.

Run, expect FAIL.

- [ ] **Step 4: Implement `EfCoreOutbox` and `OrionPatchSaveChangesInterceptor`**

```csharp
public sealed class EfCoreOutbox(
    DbContext db,
    MessageTypeNameResolver typeResolver,
    MessageSerializer serializer) : IOutbox
{
    internal List<OutboxRow> Buffer { get; } = new();

    public void Enqueue<T>(T message, OutboxEnqueueOptions? options = null) where T : class
    {
        var now = options?.OccurredAtUtc ?? DateTime.UtcNow;
        var row = new OutboxRow
        {
            Id = Guid.NewGuid(),
            MessageType = typeResolver.Resolve(typeof(T), options),
            Payload = serializer.Serialize(message),
            HeadersJson = options?.Headers is null ? null
                : JsonSerializer.Serialize(options.Headers),
            CorrelationId = options?.CorrelationId,
            OccurredAtUtc = now,
            EnqueuedAtUtc = now,
            Status = OutboxStatus.Pending,
            AttemptCount = 0,
            NextAttemptAtUtc = now,
        };
        Buffer.Add(row);
    }
}

public sealed class OrionPatchSaveChangesInterceptor : SaveChangesInterceptor
{
    public override InterceptionResult<int> SavingChanges(DbContextEventData eventData, InterceptionResult<int> result)
    {
        Flush(eventData.Context);
        return base.SavingChanges(eventData, result);
    }

    public override ValueTask<InterceptionResult<int>> SavingChangesAsync(DbContextEventData eventData, InterceptionResult<int> result, CancellationToken ct = default)
    {
        Flush(eventData.Context);
        return base.SavingChangesAsync(eventData, result, ct);
    }

    private static void Flush(DbContext? db)
    {
        if (db is null) return;
        var outbox = db.GetService<IOutbox>() as EfCoreOutbox;
        if (outbox is null || outbox.Buffer.Count == 0) return;
        db.AddRange(outbox.Buffer);
        outbox.Buffer.Clear();
    }
}
```

(Note: the interceptor uses `db.GetService<IOutbox>()` to resolve the scoped outbox bound to this DbContext. The DI helper in Task 7 wires `IOutbox` as Scoped over the DbContext.)

- [ ] **Step 5: Run tests, expect PASS**

- [ ] **Step 6: Commit**

```bash
git add .
git commit -m "feat(efcore): outbox row entity, interceptor, transactional enqueue"
```

---

## Task 6: EF Core storage (claim/complete/fail/dead-letter)

**Files:**
- Create: `src/Moongazing.OrionPatch.EntityFrameworkCore/EfCoreOutboxStorage.cs`
- Create: `src/Moongazing.OrionPatch.EntityFrameworkCore/Internal/ProviderClaimStrategy.cs`
- Create: `src/Moongazing.OrionPatch.EntityFrameworkCore/Internal/RowVersionClaimStrategy.cs`
- Create: `src/Moongazing.OrionPatch.EntityFrameworkCore/Internal/SkipLockedClaimStrategy.cs`
- Test: `tests/Moongazing.OrionPatch.EntityFrameworkCore.Tests/EfCoreOutboxStorageTests.cs`

- [ ] **Step 1: Failing test — claim returns pending row and marks it Claimed**

```csharp
public class EfCoreOutboxStorageTests
{
    [Fact]
    public async Task ClaimNextAsync_ShouldReturnPendingRow_AndFlipStatusToClaimed()
    {
        await using var db = TestDb.Create();
        db.Add(NewPendingRow());
        await db.SaveChangesAsync();

        var storage = new EfCoreOutboxStorage(db, ClaimStrategyFor(db));
        var batch = await storage.ClaimNextAsync(batchSize: 10, "dispatcher-1", TimeSpan.FromMinutes(1), default);

        Assert.Single(batch);
        var reloaded = await db.Set<OutboxRow>().AsNoTracking().FirstAsync();
        Assert.Equal(OutboxStatus.Claimed, reloaded.Status);
        Assert.Equal("dispatcher-1", reloaded.ClaimedBy);
    }

    [Fact]
    public async Task CompleteAsync_ShouldSetProcessed_WhenCalled()
    {
        // ...
    }

    [Fact]
    public async Task FailAsync_ShouldIncrementAttempt_AndSetNextAttempt_WhenNotDeadLetter()
    {
        // ...
    }

    [Fact]
    public async Task FailAsync_ShouldSetDeadLettered_WhenFlagged()
    {
        // ...
    }

    [Fact]
    public async Task ClaimNextAsync_ShouldRespectLeaseExpiry_WhenClaimedRowIsStale()
    {
        // Seed Claimed row with ClaimedAtUtc older than lease; assert it can be re-claimed.
    }

    [Fact]
    public async Task QueueDepthAsync_ShouldCountPendingRowsOnly_WhenInvoked()
    {
        // ...
    }
}
```

- [ ] **Step 2: Implement `EfCoreOutboxStorage` + the two claim strategies**

(Full implementation in step body; key points:
- `AppendAsync` calls `db.AddRange` + `SaveChangesAsync` — used by the testing package; production uses the interceptor path.
- `ClaimNextAsync` delegates to the `IClaimStrategy`.
- `SkipLockedClaimStrategy` builds a single `UPDATE ... SET Status=Claimed ... WHERE Id IN (SELECT Id FROM OrionPatch_Outbox WHERE (Status=Pending OR (Status=Claimed AND ClaimedAtUtc<@leaseExpiry)) AND (NextAttemptAtUtc IS NULL OR NextAttemptAtUtc<=NOW) ORDER BY EnqueuedAtUtc LIMIT @batchSize FOR UPDATE SKIP LOCKED) RETURNING *` via `ExecuteSqlInterpolatedAsync` per provider dialect; resolves dialect from `db.Database.ProviderName`.
- `RowVersionClaimStrategy` is the SQLite path: SELECT candidate ids, optimistic UPDATE with `RowVersion` predicate, skip rows that lost the race.)

- [ ] **Step 3: Run tests, expect PASS**

- [ ] **Step 4: Commit**

```bash
git add .
git commit -m "feat(efcore): EfCoreOutboxStorage with provider-aware claim

Claim strategy resolved by provider name:
- SQL Server, PostgreSQL, MySQL/MariaDB use SKIP LOCKED.
- SQLite (and any unrecognized provider) falls back to optimistic
  concurrency via RowVersion.

Lease expiry is respected: rows in Claimed status older than the lease
become re-claimable. Complete/Fail/DeadLetter mutations atomic via the
DbContext."
```

---

## Task 7: DI wiring (`UseEntityFrameworkCore<T>`)

**Files:**
- Create: `src/Moongazing.OrionPatch.EntityFrameworkCore/DependencyInjection/OrionPatchEntityFrameworkCoreBuilderExtensions.cs`
- Test: `tests/Moongazing.OrionPatch.EntityFrameworkCore.Tests/UseEntityFrameworkCoreTests.cs`
- Test: `tests/Moongazing.OrionPatch.EntityFrameworkCore.Tests/EndToEndDispatchTests.cs`

- [ ] **Step 1: Failing test — end-to-end enqueue + dispatch with EF Core storage and Channel sink**

```csharp
public class EndToEndDispatchTests
{
    [Fact]
    public async Task FullCycle_ShouldDeliverEnqueuedMessageToChannelSink_WhenDispatcherRuns()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<AppDb>(o => o.UseSqlite("DataSource=:memory:"));
        services.AddOrionPatch(o => o.PollingInterval = TimeSpan.FromMilliseconds(50))
            .UseEntityFrameworkCore<AppDb>()
            .UseChannelSink();

        var sp = services.BuildServiceProvider();
        using (var scope = sp.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<AppDb>();
            await db.Database.OpenConnectionAsync();
            await db.Database.EnsureCreatedAsync();
            var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();
            outbox.Enqueue(new OrderConfirmed(Guid.NewGuid(), 7));
            await db.SaveChangesAsync();
        }

        var sink = sp.GetRequiredService<ChannelOutboxSink>();
        var hosted = sp.GetServices<IHostedService>().OfType<OutboxDispatcherHostedService>().Single();
        await hosted.StartAsync(default);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var env = await sink.Reader.ReadAsync(cts.Token);

        Assert.Equal(typeof(OrderConfirmed).FullName, env.MessageType);
        await hosted.StopAsync(default);
    }
}
```

- [ ] **Step 2: Implement `UseEntityFrameworkCore<TDbContext>`**

```csharp
public static class OrionPatchEntityFrameworkCoreBuilderExtensions
{
    public static OrionPatchBuilder UseEntityFrameworkCore<TDbContext>(this OrionPatchBuilder b)
        where TDbContext : DbContext
    {
        b.Services.AddScoped<EfCoreOutbox>(sp =>
            new EfCoreOutbox(
                sp.GetRequiredService<TDbContext>(),
                sp.GetRequiredService<MessageTypeNameResolver>(),
                sp.GetRequiredService<MessageSerializer>()));
        b.Services.AddScoped<IOutbox>(sp => sp.GetRequiredService<EfCoreOutbox>());

        b.Services.AddScoped<EfCoreOutboxStorage>(sp =>
            new EfCoreOutboxStorage(
                sp.GetRequiredService<TDbContext>(),
                ProviderClaimStrategy.For(sp.GetRequiredService<TDbContext>())));
        b.Services.AddScoped<IOutboxStorage>(sp => sp.GetRequiredService<EfCoreOutboxStorage>());

        b.Services.AddSingleton<OrionPatchSaveChangesInterceptor>();
        b.Services.AddDbContextOptions<TDbContext>(); // helper that adds the interceptor — implementation in step body
        return b;
    }
}
```

(The "add interceptor" helper hooks into `OnConfiguring` via an `IDbContextOptionsConfiguration<TDbContext>`; documented in the body. Alternative: tell users to call `optionsBuilder.AddInterceptors(sp.GetRequiredService<OrionPatchSaveChangesInterceptor>())` in their own `AddDbContext` configuration — the README shows the manual pattern; the helper is sugar.)

- [ ] **Step 3: Run all tests, expect PASS**

- [ ] **Step 4: Commit**

```bash
git add .
git commit -m "feat(efcore): UseEntityFrameworkCore DI wiring + end-to-end test

OrionPatch services bind to the consumer's DbContext as Scoped, the
interceptor registers as Singleton (stateless), and an integration
test exercises enqueue -> SaveChanges -> dispatcher -> Channel sink
against in-memory SQLite to validate the whole loop."
```

---

## Task 8: Testing package

**Files:**
- Create: `src/Moongazing.OrionPatch.Testing/Moongazing.OrionPatch.Testing.csproj`
- Create: `src/Moongazing.OrionPatch.Testing/InMemoryOutboxStorage.cs`
- Create: `src/Moongazing.OrionPatch.Testing/InMemoryOutbox.cs`
- Create: `src/Moongazing.OrionPatch.Testing/DeterministicDispatcher.cs`
- Create: `src/Moongazing.OrionPatch.Testing/CapturingOutboxSink.cs`
- Create: `src/Moongazing.OrionPatch.Testing/TestClock.cs`
- Create: `src/Moongazing.OrionPatch.Testing/OutboxAssertions.cs`
- Create: `src/Moongazing.OrionPatch.Testing/DependencyInjection/OrionPatchTestingBuilderExtensions.cs`
- Test: `tests/Moongazing.OrionPatch.Testing.Tests/Moongazing.OrionPatch.Testing.Tests.csproj`
- Test: `tests/Moongazing.OrionPatch.Testing.Tests/DeterministicDispatcherTests.cs`
- Test: `tests/Moongazing.OrionPatch.Testing.Tests/CapturingOutboxSinkTests.cs`

(Each helper gets a TDD-shaped test before implementation. The deterministic dispatcher is a synchronous variant of the hosted service that does exactly one claim/dispatch pass when `DispatchOnceAsync` is called — predictable for tests.)

- [ ] **Step 1: csproj (same shape as core), referencing core**
- [ ] **Step 2: failing test for `DeterministicDispatcher.DispatchOnceAsync` — single message round trip**
- [ ] **Step 3: implement `InMemoryOutboxStorage` (`ConcurrentDictionary<Guid, OutboxRow>` + thread-safe claim/complete/fail/depth)**
- [ ] **Step 4: implement `InMemoryOutbox` (writes directly to `InMemoryOutboxStorage`, no DbContext)**
- [ ] **Step 5: implement `CapturingOutboxSink` (thread-safe `List<OutboxEnvelope>`)**
- [ ] **Step 6: implement `TestClock` (settable `UtcNow`, `DelayAsync` honors a `Task.CompletedTask` short-circuit when `TimeSpan.Zero`)**
- [ ] **Step 7: implement `OutboxAssertions` (`AssertDispatched<T>(predicate)`, `AssertDeadLettered<T>`, etc.)**
- [ ] **Step 8: implement `UseInMemory()` builder extension**
- [ ] **Step 9: run all tests, expect PASS, commit**

```bash
git commit -m "feat(testing): in-memory storage, deterministic dispatcher, capturing sink + assertions"
```

---

## Task 9: Sample console app

**Files:**
- Create: `sample/Moongazing.OrionPatch.Sample/Moongazing.OrionPatch.Sample.csproj`
- Create: `sample/Moongazing.OrionPatch.Sample/Program.cs`
- Create: `sample/Moongazing.OrionPatch.Sample/SampleDbContext.cs`
- Create: `sample/Moongazing.OrionPatch.Sample/OrderConfirmed.cs`

- [ ] **Step 1: minimal `Program.cs`** — generic host, SQLite in-memory `SampleDb`, `AddOrionPatch().UseEntityFrameworkCore<SampleDb>().UseChannelSink()`, a small consumer that reads from `ChannelOutboxSink.Reader` and prints to stdout, then publishes 3 messages and exits cleanly.
- [ ] **Step 2: build the sample, run it once locally, confirm output**

  ```
  Enqueued OrderConfirmed(00000000-0000-0000-0000-..., 100)
  Dispatched OrderConfirmed(00000000-0000-0000-0000-..., 100)
  ...
  ```

- [ ] **Step 3: commit**

```bash
git add sample/
git commit -m "sample: minimal end-to-end console host showing enqueue -> dispatch"
```

---

## Task 10: README + per-project READMEs + roadmap polish

**Files:**
- Polish: `README.md`
- Create: `src/Moongazing.OrionPatch/docs/README.md`
- Create: `src/Moongazing.OrionPatch.EntityFrameworkCore/docs/README.md`
- Create: `src/Moongazing.OrionPatch.Testing/docs/README.md`
- Polish: `ROADMAP.md` (already drafted in Task 0)

- [ ] **Step 1: NuGet READMEs** — each per-project `docs/README.md` is a focused page (250-400 lines max) showing the 30-second quick start and 1-2 typical recipes for that specific package. Pattern lifted from OrionLock's per-package readme.
- [ ] **Step 2: Repo README** — flesh out the comparison table, "When to use OrionPatch vs MassTransit vs Wolverine" section, telemetry table, troubleshooting tips for the most common at-least-once footgun.
- [ ] **Step 3: ROADMAP** — verify it mirrors the spec's release plan.
- [ ] **Step 4: Commit**

```bash
git add README.md ROADMAP.md src/*/docs/README.md
git commit -m "docs: repo README + per-package NuGet READMEs + roadmap polish"
```

---

## Task 11: Logo deploy

**Pre-condition:** user has dropped `OrionPatchNewLogo.png` into `OrionGuard/OrionGuard/docs/superpowers/`.

- [ ] **Step 1: Clean + resize via the PowerShell script pattern used for the four previous logos**

```powershell
Add-Type -AssemblyName System.Drawing
# (LockBits + colour-key alpha-clean + HighQualityBicubic resize to 256x256.)
$src = [System.Drawing.Bitmap]::FromFile("...\OrionPatchNewLogo.png")
# kill near-white + low-alpha pixels (transparent background fix)
# resize to 256x256, save to:
#   OrionPatch/docs/logo.png
#   OrionPatch/docs/icon.png
```

- [ ] **Step 2: confirm each is <30 KB and 256x256, transparent alpha**
- [ ] **Step 3: commit**

```bash
git add docs/logo.png docs/icon.png
git commit -m "chore: deploy v0.1.0 logo

Minimalist family-style envelope logo in Moongazing indigo (#312E81)
matching the rest of the Orion family. 256x256, transparent alpha,
~15 KB."
```

---

## Task 12: First release — v0.1.0

- [ ] **Step 1: Run full CI matrix locally**

```bash
dotnet restore Moongazing.OrionPatch.sln
dotnet build  Moongazing.OrionPatch.sln --configuration Release --no-restore
dotnet test   Moongazing.OrionPatch.sln --configuration Release --no-build --verbosity normal
```

All three legs green; no warnings.

- [ ] **Step 2: Update `CHANGELOG.md` with the v0.1.0 entry**

The full feature list from the spec, in Keep-a-Changelog format.

- [ ] **Step 3: PR + merge**

```bash
git checkout -b release/v0.1.0
git push -u origin release/v0.1.0
gh pr create --title "chore(release): v0.1.0 - initial release" --body "<release notes>"
gh pr merge --squash --delete-branch
git checkout main && git pull
```

- [ ] **Step 4: Tag + GitHub release (triggers `publish` job in CI)**

```bash
git tag -a v0.1.0 -m "v0.1.0 - initial release"
git push origin v0.1.0
gh release create v0.1.0 --title "v0.1.0 - Initial release" --notes "<same release notes>"
```

- [ ] **Step 5: Confirm NuGet listing**

`OrionPatch`, `OrionPatch.EntityFrameworkCore`, `OrionPatch.Testing` all listed at v0.1.0 on nuget.org within ~10 minutes.

- [ ] **Step 6: Apply branch protection to `main`**

Use the `gh api` JSON-input pattern proven on OrionLock and OrionKey.

- [ ] **Step 7: Cross-link** — add OrionPatch to the "Family" section of the four other repos' READMEs.

---

## Done state

A `git tag v0.1.0` exists on `tunahanaliozturk/OrionPatch`. Three packages live on NuGet. CI passes on net8/9/10. ROADMAP committed alongside the source. Branch protection on `main`. The Orion family is now five mature packages cross-linked from each other's READMEs.
