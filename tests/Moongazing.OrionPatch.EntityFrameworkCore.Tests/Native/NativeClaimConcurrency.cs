namespace Moongazing.OrionPatch.EntityFrameworkCore.Tests.Native;

using System.Collections.Concurrent;
using Microsoft.EntityFrameworkCore;
using Moongazing.OrionPatch.EntityFrameworkCore;
using Moongazing.OrionPatch.Models;
using Xunit;

/// <summary>
/// Provider-agnostic driver for the native-claim competing-consumers test. Seeds a fixed set of
/// due rows, then races N claimers, each on its own DbContext / connection, gated to start together
/// so the claim statements genuinely contend. Asserts every row is claimed by exactly one claimer.
/// </summary>
internal static class NativeClaimConcurrency
{
    /// <summary>
    /// Run the competing-consumers assertion: <paramref name="claimerCount"/> dispatchers drain
    /// <paramref name="rowCount"/> due rows under contention and no row is ever double-claimed.
    /// </summary>
    /// <param name="newContext">Factory producing a fresh DbContext bound to its own connection.</param>
    /// <param name="rowCount">Number of due rows to seed.</param>
    /// <param name="claimerCount">Number of concurrent dispatchers.</param>
    /// <param name="batchSize">Per-claim batch size.</param>
    public static async Task AssertExclusiveClaimAsync(
        Func<NativeClaimDbContext> newContext,
        int rowCount = 200,
        int claimerCount = 8,
        int batchSize = 7)
    {
        // Clean slate, then seed `rowCount` due pending rows.
        await using (var seed = newContext())
        {
            await seed.Set<OutboxRow>().ExecuteDeleteAsync();
            var now = DateTime.UtcNow;
            for (var i = 0; i < rowCount; i++)
            {
                seed.Add(new OutboxRow
                {
                    Id = Guid.NewGuid(),
                    MessageType = "T",
                    Payload = "{}",
                    OccurredAtUtc = now,
                    EnqueuedAtUtc = now.AddMilliseconds(i),
                    Status = OutboxStatus.Pending,
                });
            }
            await seed.SaveChangesAsync();
        }

        // All claimers block on this gate, then fire simultaneously (a deterministic start barrier,
        // not a sleep), so the claim statements actually race for the same rows.
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var claimedById = new ConcurrentDictionary<Guid, int>();
        var perClaimerCounts = new int[claimerCount];

        async Task RunClaimerAsync(int claimerId)
        {
            await gate.Task;
            await using var db = newContext();
            var storage = new EfCoreOutboxStorage(db);
            while (true)
            {
                var batch = await storage.ClaimNextAsync(batchSize, $"dispatcher-{claimerId}", TimeSpan.FromMinutes(5));
                if (batch.Count == 0)
                {
                    break;
                }

                foreach (var row in batch)
                {
                    // First writer wins; a second writer for the same id signals a double-claim.
                    claimedById.AddOrUpdate(row.Id, claimerId, (_, existing) => existing == claimerId ? existing : -1);
                    perClaimerCounts[claimerId]++;
                }
            }
        }

        var workers = Enumerable.Range(0, claimerCount).Select(RunClaimerAsync).ToArray();
        gate.SetResult();
        await Task.WhenAll(workers);

        // Every seeded row claimed exactly once, and the total equals the seed (no row lost, none
        // double-counted). The dictionary holds one entry per distinct id; -1 marks a contested id.
        Assert.Equal(rowCount, claimedById.Count);
        Assert.DoesNotContain(-1, claimedById.Values);
        Assert.Equal(rowCount, perClaimerCounts.Sum());

        // Confirm the persisted state agrees: all rows Claimed, each by exactly one dispatcher.
        await using var verify = newContext();
        var rows = await verify.Set<OutboxRow>().AsNoTracking().ToListAsync();
        Assert.Equal(rowCount, rows.Count);
        Assert.All(rows, r =>
        {
            Assert.Equal(OutboxStatus.Claimed, r.Status);
            Assert.NotNull(r.ClaimedBy);
        });
    }
}
