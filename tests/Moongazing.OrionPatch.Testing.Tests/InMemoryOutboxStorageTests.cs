namespace Moongazing.OrionPatch.Testing.Tests;

using Moongazing.OrionPatch.Models;
using Xunit;

public class InMemoryOutboxStorageTests
{
    private static OutboxRow NewPending(DateTime? enqueuedAtUtc = null, DateTime? nextAttemptAtUtc = null)
    {
        var now = enqueuedAtUtc ?? DateTime.UtcNow;
        return new OutboxRow
        {
            Id = Guid.NewGuid(),
            MessageType = "T",
            Payload = "{}",
            OccurredAtUtc = now,
            EnqueuedAtUtc = now,
            Status = OutboxStatus.Pending,
            NextAttemptAtUtc = nextAttemptAtUtc,
        };
    }

    [Fact]
    public async Task AppendAsync_ShouldPersist_WhenRowsSupplied()
    {
        var storage = new InMemoryOutboxStorage();
        var row = NewPending();

        await storage.AppendAsync(new[] { row });

        Assert.Single(storage.Rows);
        Assert.Equal(row.Id, storage.Rows.First().Id);
    }

    [Fact]
    public async Task ClaimNextAsync_ShouldReturnPendingRow_AndFlipStatusToClaimed()
    {
        var storage = new InMemoryOutboxStorage();
        var row = NewPending();
        await storage.AppendAsync(new[] { row });

        var claimed = await storage.ClaimNextAsync(10, "dispatcher-A", TimeSpan.FromMinutes(1));

        Assert.Single(claimed);
        Assert.Equal(row.Id, claimed[0].Id);
        Assert.Equal(OutboxStatus.Claimed, claimed[0].Status);
        Assert.Equal("dispatcher-A", claimed[0].ClaimedBy);
        Assert.NotNull(claimed[0].ClaimedAtUtc);
    }

    [Fact]
    public async Task ClaimNextAsync_ShouldReclaim_WhenLeaseExpired()
    {
        var storage = new InMemoryOutboxStorage();
        var row = NewPending();
        await storage.AppendAsync(new[] { row });

        // First claim by dispatcher-A under a very short lease that we then force-expire.
        var firstClaim = await storage.ClaimNextAsync(10, "dispatcher-A", TimeSpan.FromMinutes(1));
        Assert.Single(firstClaim);

        // Move the row's claim into the past so the lease is expired.
        var stored = storage.Rows.Single(r => r.Id == row.Id);
        stored.ClaimedAtUtc = DateTime.UtcNow.AddHours(-1);

        var secondClaim = await storage.ClaimNextAsync(10, "dispatcher-B", TimeSpan.FromMinutes(1));

        Assert.Single(secondClaim);
        Assert.Equal(row.Id, secondClaim[0].Id);
        Assert.Equal("dispatcher-B", secondClaim[0].ClaimedBy);
    }

    [Fact]
    public async Task CompleteAsync_ShouldFlipToProcessed_WhenCalled()
    {
        var storage = new InMemoryOutboxStorage();
        var row = NewPending();
        await storage.AppendAsync(new[] { row });
        await storage.ClaimNextAsync(10, "d", TimeSpan.FromMinutes(1));

        var processedAt = new DateTime(2026, 5, 24, 12, 0, 0, DateTimeKind.Utc);
        await storage.CompleteAsync(row.Id, processedAt);

        var stored = storage.Rows.Single(r => r.Id == row.Id);
        Assert.Equal(OutboxStatus.Processed, stored.Status);
        Assert.Equal(processedAt, stored.ProcessedAtUtc);
        Assert.Null(stored.ClaimedAtUtc);
        Assert.Null(stored.ClaimedBy);
    }

    [Fact]
    public async Task FailAsync_ShouldIncrementAttempt_AndResetClaim_WhenCalled()
    {
        var storage = new InMemoryOutboxStorage();
        var row = NewPending();
        await storage.AppendAsync(new[] { row });
        await storage.ClaimNextAsync(10, "d", TimeSpan.FromMinutes(1));

        var nextAttempt = DateTime.UtcNow.AddSeconds(5);
        await storage.FailAsync(row.Id, "boom", nextAttempt);

        var stored = storage.Rows.Single(r => r.Id == row.Id);
        Assert.Equal(OutboxStatus.Pending, stored.Status);
        Assert.Equal(1, stored.AttemptCount);
        Assert.Equal("boom", stored.LastError);
        Assert.Equal(nextAttempt, stored.NextAttemptAtUtc);
        Assert.Null(stored.ClaimedAtUtc);
        Assert.Null(stored.ClaimedBy);
    }

    [Fact]
    public async Task DeadLetterAsync_ShouldFlipToDeadLettered_WhenCalled()
    {
        var storage = new InMemoryOutboxStorage();
        var row = NewPending();
        await storage.AppendAsync(new[] { row });
        await storage.ClaimNextAsync(10, "d", TimeSpan.FromMinutes(1));

        await storage.DeadLetterAsync(row.Id, "terminal");

        var stored = storage.Rows.Single(r => r.Id == row.Id);
        Assert.Equal(OutboxStatus.DeadLettered, stored.Status);
        Assert.Equal(1, stored.AttemptCount);
        Assert.Equal("terminal", stored.LastError);
        Assert.Null(stored.ClaimedAtUtc);
        Assert.Null(stored.ClaimedBy);
    }

    [Fact]
    public void ParameterlessConstructor_ShouldExistInMetadata_AsZeroParameterCtor()
    {
        // codex P1 / binary-compat: v0.2.x assemblies were compiled against a compiler-generated
        // public .ctor() with ZERO parameters. The v0.3.0 bool-parameter constructor (even with an
        // optional default) is a DIFFERENT metadata signature, so an optional default only preserves
        // SOURCE compat - a binary compiled against v0.2.x would throw MissingMethodException.
        // Assert the zero-parameter .ctor() is physically present in metadata so the minor release
        // stays BINARY-compatible. GetConstructor(Type.EmptyTypes) resolves by metadata signature
        // (it does NOT synthesize the bool ctor's optional default), and ctor.Invoke proves it
        // actually constructs - exactly the bind a v0.2.x-compiled `new InMemoryOutboxStorage()` does.
        var ctor = typeof(InMemoryOutboxStorage).GetConstructor(Type.EmptyTypes);

        Assert.NotNull(ctor);
        var instance = Assert.IsType<InMemoryOutboxStorage>(ctor!.Invoke(null));
        Assert.NotNull(instance);
    }

    [Fact]
    public async Task ParameterlessConstructor_ShouldBehaveAsArchiveModeDefault_NotPurge()
    {
        // The binary-compat parameterless overload must delegate to the new ctor with purgeOnArchive:false,
        // i.e. behave identically to the documented default (reaped rows are archived, not purged).
        var storage = new InMemoryOutboxStorage();
        var processedAt = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var row = NewPending(enqueuedAtUtc: processedAt);
        await storage.AppendAsync(new[] { row });
        await storage.ClaimNextAsync(10, "d", TimeSpan.FromMinutes(1));
        await storage.CompleteAsync(row.Id, processedAt);

        var reaped = await storage.ArchiveProcessedAsync(TimeSpan.Zero, processedAt.AddHours(1));

        Assert.Equal(1, reaped);
        // Archive mode (default): the reaped row is retained in the archive, not discarded.
        Assert.Single(storage.ArchivedRows);
        Assert.Equal(row.Id, storage.ArchivedRows.First().Id);
    }

    [Fact]
    public async Task QueueDepthAsync_ShouldCountPending_WhenInvoked()
    {
        var storage = new InMemoryOutboxStorage();
        var pending1 = NewPending();
        var pending2 = NewPending();
        var processed = NewPending();
        await storage.AppendAsync(new[] { pending1, pending2, processed });

        await storage.ClaimNextAsync(10, "d", TimeSpan.FromMinutes(1));
        await storage.CompleteAsync(processed.Id, DateTime.UtcNow);

        var depth = await storage.QueueDepthAsync();

        // The two non-completed rows are now Claimed, not Pending, so depth = 0.
        // Re-enqueue a fresh pending to verify the pending counter works.
        await storage.AppendAsync(new[] { NewPending() });
        var depth2 = await storage.QueueDepthAsync();

        Assert.Equal(0, depth);
        Assert.Equal(1, depth2);
    }
}
