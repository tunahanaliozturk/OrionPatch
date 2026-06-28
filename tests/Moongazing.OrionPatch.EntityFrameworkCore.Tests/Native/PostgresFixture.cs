namespace Moongazing.OrionPatch.EntityFrameworkCore.Tests.Native;

using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Testcontainers.PostgreSql;
using Xunit;

/// <summary>
/// Boots a single PostgreSQL container for the native-claim concurrency suite. Only a genuinely
/// unavailable Docker environment produces a skip (see <see cref="DockerAvailability"/>); a
/// schema/connection/provider failure once the container is up fails the suite instead of masking
/// the regression as a Docker skip.
/// </summary>
public sealed class PostgresFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer container = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    /// <summary>Connection string for the running container, or null when Docker was unavailable.</summary>
    public string? ConnectionString { get; private set; }

    /// <summary>Non-null only when Docker itself was unavailable; tests use it as their skip reason.</summary>
    public string? SkipReason { get; private set; }

    public DbContextOptions<NativeClaimDbContext> NewOptions()
        => new DbContextOptionsBuilder<NativeClaimDbContext>()
            .UseNpgsql(ConnectionString)
            .Options;

    public async Task InitializeAsync()
    {
        SkipReason = await DockerAvailability.StartOrSkipAsync(
            startAsync: () => container.StartAsync(),
            setupAsync: async () =>
            {
                ConnectionString = container.GetConnectionString();

                // Create the OrionPatch schema once for the whole suite. A failure here is real and
                // must fail the suite, not skip it.
                var options = NewOptions();
                await using var db = new NativeClaimDbContext(options);
                await db.Database.EnsureCreatedAsync();
            });
    }

    public async Task DisposeAsync()
    {
        if (SkipReason is null)
        {
            await container.DisposeAsync();
        }
    }
}

/// <summary>xUnit collection so the PostgreSQL container is shared across the native-claim tests.</summary>
[CollectionDefinition(Name)]
public sealed class PostgresClaimGroup : ICollectionFixture<PostgresFixture>
{
    public const string Name = "postgres-native-claim";
}
