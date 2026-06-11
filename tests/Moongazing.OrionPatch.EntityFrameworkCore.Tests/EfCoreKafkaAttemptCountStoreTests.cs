namespace Moongazing.OrionPatch.EntityFrameworkCore.Tests;

using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Moongazing.OrionPatch.Kafka.Inbound;
using Xunit;

public sealed class EfCoreKafkaAttemptCountStoreTests : IAsyncLifetime
{
    private sealed class AttemptDbContext : DbContext
    {
        public AttemptDbContext(DbContextOptions<AttemptDbContext> options) : base(options) { }
        public DbSet<KafkaInboundAttempt> Attempts => Set<KafkaInboundAttempt>();
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);
            KafkaInboundAttempt.Configure(modelBuilder);
        }
    }

    private SqliteConnection connection = default!;
    private ServiceProvider services = default!;

    public async Task InitializeAsync()
    {
        connection = new SqliteConnection("DataSource=:memory:");
        await connection.OpenAsync();
        var collection = new ServiceCollection();
        collection.AddDbContext<AttemptDbContext>(o => o.UseSqlite(connection));
        services = collection.BuildServiceProvider();
        using var scope = services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<AttemptDbContext>();
        await db.Database.EnsureCreatedAsync();
    }

    public async Task DisposeAsync()
    {
        await services.DisposeAsync();
        await connection.DisposeAsync();
    }

    private EfCoreKafkaAttemptCountStore<AttemptDbContext> NewStore()
        => new(services.GetRequiredService<IServiceScopeFactory>());

    [Fact]
    public async Task GetAsync_returns_zero_for_unseen_envelope()
    {
        var sut = NewStore();
        var count = await sut.GetAsync(Guid.NewGuid(), CancellationToken.None);
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task SetAsync_inserts_new_row_on_first_call_and_updates_on_subsequent()
    {
        var sut = NewStore();
        var envelopeId = Guid.NewGuid();

        await sut.SetAsync(envelopeId, 1, CancellationToken.None);
        Assert.Equal(1, await sut.GetAsync(envelopeId, CancellationToken.None));

        await sut.SetAsync(envelopeId, 5, CancellationToken.None);
        Assert.Equal(5, await sut.GetAsync(envelopeId, CancellationToken.None));
    }

    [Fact]
    public async Task ClearAsync_removes_the_persisted_row()
    {
        var sut = NewStore();
        var envelopeId = Guid.NewGuid();
        await sut.SetAsync(envelopeId, 3, CancellationToken.None);
        Assert.Equal(3, await sut.GetAsync(envelopeId, CancellationToken.None));

        await sut.ClearAsync(envelopeId, CancellationToken.None);

        Assert.Equal(0, await sut.GetAsync(envelopeId, CancellationToken.None));
    }

    [Fact]
    public async Task Count_survives_store_instance_swap_simulating_consumer_restart()
    {
        var envelopeId = Guid.NewGuid();
        var first = NewStore();
        await first.SetAsync(envelopeId, 4, CancellationToken.None);

        // Construct a fresh store instance against the SAME backing database to
        // simulate the consumer restarting; the persisted count must be visible.
        var second = NewStore();
        Assert.Equal(4, await second.GetAsync(envelopeId, CancellationToken.None));
    }

    [Fact]
    public void Constructor_rejects_null_scopeFactory()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new EfCoreKafkaAttemptCountStore<AttemptDbContext>(null!));
    }

    [Fact]
    public void Configure_rejects_null_modelBuilder()
    {
        Assert.Throws<ArgumentNullException>(() => KafkaInboundAttempt.Configure(null!));
    }
}
