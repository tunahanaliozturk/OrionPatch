namespace Moongazing.OrionPatch.EntityFrameworkCore.Tests.Native;

using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Testcontainers.MsSql;
using Xunit;

/// <summary>
/// Boots a single SQL Server container for the native-claim concurrency suite. As with
/// <see cref="PostgresFixture"/>, a Docker-unavailable start failure is captured in
/// <see cref="SkipReason"/> so the tests skip locally and run for real on CI.
/// </summary>
public sealed class SqlServerFixture : IAsyncLifetime
{
    private readonly MsSqlContainer container = new MsSqlBuilder()
        .WithImage("mcr.microsoft.com/mssql/server:2022-latest")
        .Build();

    /// <summary>Connection string for the running container, or null when Docker was unavailable.</summary>
    public string? ConnectionString { get; private set; }

    /// <summary>Non-null when the container could not start; tests use it as their skip reason.</summary>
    public string? SkipReason { get; private set; }

    public DbContextOptions<NativeClaimDbContext> NewOptions()
        => new DbContextOptionsBuilder<NativeClaimDbContext>()
            .UseSqlServer(ConnectionString)
            .Options;

    public async Task InitializeAsync()
    {
        try
        {
            await container.StartAsync();
            ConnectionString = container.GetConnectionString();

            var options = NewOptions();
            await using var db = new NativeClaimDbContext(options);
            await db.Database.EnsureCreatedAsync();
        }
        catch (Exception ex)
        {
            SkipReason = $"SQL Server Testcontainer unavailable (Docker not running?): {ex.Message}";
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

/// <summary>xUnit collection so the SQL Server container is shared across the native-claim tests.</summary>
[CollectionDefinition(Name)]
public sealed class SqlServerClaimGroup : ICollectionFixture<SqlServerFixture>
{
    public const string Name = "sqlserver-native-claim";
}
