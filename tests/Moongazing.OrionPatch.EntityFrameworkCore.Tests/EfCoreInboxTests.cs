namespace Moongazing.OrionPatch.EntityFrameworkCore.Tests;

using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.EntityFrameworkCore;
using Moongazing.OrionPatch.EntityFrameworkCore.Configuration;
using Xunit;

[System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1859",
    Justification = "Tests deliberately reference the public IInbox interface to assert the contract.")]
public sealed class EfCoreInboxTests : IDisposable
{
    private sealed class TestContext : DbContext
    {
        public DbSet<InboxRow> Inbox => Set<InboxRow>();
        public TestContext(DbContextOptions<TestContext> options) : base(options) { }
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ApplyConfiguration(new InboxEntityConfiguration());
        }
    }

    private readonly SqliteConnection connection;
    private readonly TestContext context;

    public EfCoreInboxTests()
    {
        connection = new SqliteConnection("DataSource=:memory:");
        connection.Open();
        var options = new DbContextOptionsBuilder<TestContext>()
            .UseSqlite(connection)
            .Options;
        context = new TestContext(options);
        context.Database.EnsureCreated();
    }

    public void Dispose()
    {
        context.Dispose();
        connection.Dispose();
    }

    [Fact]
    public async Task First_TryAccept_returns_true_and_persists_row()
    {
        IInbox inbox = new EfCoreInbox(context);
        var id = Guid.NewGuid();

        var accepted = await inbox.TryAcceptAsync(id, CancellationToken.None);

        Assert.True(accepted);
        var row = await context.Inbox.SingleAsync();
        Assert.Equal(id, row.MessageId);
        Assert.Equal(string.Empty, row.Consumer);
    }

    [Fact]
    public async Task Duplicate_TryAccept_returns_false_and_does_not_double_insert()
    {
        IInbox inbox = new EfCoreInbox(context);
        var id = Guid.NewGuid();

        var first = await inbox.TryAcceptAsync(id, CancellationToken.None);
        var second = await inbox.TryAcceptAsync(id, CancellationToken.None);

        Assert.True(first);
        Assert.False(second);
        var count = await context.Inbox.CountAsync();
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task Two_consumers_dedup_independently_for_same_message_id()
    {
        IInbox a = new EfCoreInbox(context, consumer: "consumer-A");
        IInbox b = new EfCoreInbox(context, consumer: "consumer-B");
        var id = Guid.NewGuid();

        var firstA = await a.TryAcceptAsync(id, CancellationToken.None);
        var firstB = await b.TryAcceptAsync(id, CancellationToken.None);
        var dupA = await a.TryAcceptAsync(id, CancellationToken.None);

        Assert.True(firstA);
        Assert.True(firstB);
        Assert.False(dupA);

        var rows = await context.Inbox.OrderBy(r => r.Consumer).ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal("consumer-A", rows[0].Consumer);
        Assert.Equal("consumer-B", rows[1].Consumer);
    }

    [Fact]
    public async Task AcceptedAtUtc_uses_supplied_TimeProvider()
    {
        var pinned = new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);
        var clock = new PinnedTimeProvider(pinned);
        IInbox inbox = new EfCoreInbox(context, clock: clock);

        await inbox.TryAcceptAsync(Guid.NewGuid(), CancellationToken.None);

        var row = await context.Inbox.SingleAsync();
        Assert.Equal(pinned.UtcDateTime, row.AcceptedAtUtc);
    }

    [Fact]
    public async Task Persistence_failure_unrelated_to_uniqueness_rethrows()
    {
        // Drop the inbox table to simulate a missing-table / schema-mismatch scenario. The
        // first TryAcceptAsync hits a DbUpdateException; the existence query also fails, so
        // the original exception MUST propagate to the caller instead of being silently
        // classified as a duplicate.
        await context.Database.ExecuteSqlRawAsync("DROP TABLE OrionPatch_Inbox");
        IInbox inbox = new EfCoreInbox(context);

        await Assert.ThrowsAnyAsync<Exception>(
            () => inbox.TryAcceptAsync(Guid.NewGuid(), CancellationToken.None).AsTask());
    }

    private sealed class PinnedTimeProvider : TimeProvider
    {
        private readonly DateTimeOffset now;
        public PinnedTimeProvider(DateTimeOffset now) { this.now = now; }
        public override DateTimeOffset GetUtcNow() => now;
    }
}
