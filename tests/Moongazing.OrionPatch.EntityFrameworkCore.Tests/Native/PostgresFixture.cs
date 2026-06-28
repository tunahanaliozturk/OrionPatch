namespace Moongazing.OrionPatch.EntityFrameworkCore.Tests.Native;

using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Testcontainers.PostgreSql;
using Xunit;

/// <summary>
/// Boots a single PostgreSQL container for the native-claim concurrency suite. If Docker is
/// unreachable the start failure is captured in <see cref="SkipReason"/> so the tests skip with a
/// clear message instead of failing the local build; CI with Docker runs them for real.
/// </summary>
public sealed class PostgresFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer container = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    /// <summary>Connection string for the running container, or null when Docker was unavailable.</summary>
    public string? ConnectionString { get; private set; }

    /// <summary>Non-null when the container could not start; tests use it as their skip reason.</summary>
    public string? SkipReason { get; private set; }

    public DbContextOptions<NativeClaimDbContext> NewOptions()
        => new DbContextOptionsBuilder<NativeClaimDbContext>()
            .UseNpgsql(ConnectionString)
            .Options;

    public async Task InitializeAsync()
    {
        try
        {
            await container.StartAsync();
            ConnectionString = container.GetConnectionString();

            // Create the OrionPatch schema once for the whole suite.
            var options = NewOptions();
            await using var db = new NativeClaimDbContext(options);
            await db.Database.EnsureCreatedAsync();
        }
        catch (Exception ex)
        {
            SkipReason = $"PostgreSQL Testcontainer unavailable (Docker not running?): {ex.Message}";
        }
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
