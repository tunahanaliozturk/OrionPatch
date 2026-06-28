namespace Moongazing.OrionPatch.EntityFrameworkCore.Tests.Native;

using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Testcontainers.MsSql;
using Xunit;

/// <summary>
/// Boots a single SQL Server container for the native-claim concurrency suite, with
/// <c>READ_COMMITTED_SNAPSHOT</c> turned ON so the claim is exercised under the RCSI behaviour that
/// is the default on Azure SQL Database and common on SQL Server. RCSI is the regime in which a
/// naive <c>READPAST</c> would silently fail to skip locked rows, so testing under it is what proves
/// the <c>READCOMMITTEDLOCK</c> hint is doing its job.
/// </summary>
/// <remarks>
/// As with <see cref="PostgresFixture"/>, only a genuinely unavailable Docker environment skips the
/// suite (see <see cref="DockerAvailability"/>); a failure enabling RCSI, creating the schema, or
/// connecting once the container is up fails the suite rather than masking the regression.
/// </remarks>
public sealed class SqlServerFixture : IAsyncLifetime
{
    private readonly MsSqlContainer container = new MsSqlBuilder()
        .WithImage("mcr.microsoft.com/mssql/server:2022-latest")
        .Build();

    /// <summary>Connection string for the running container, or null when Docker was unavailable.</summary>
    public string? ConnectionString { get; private set; }

    /// <summary>Non-null only when Docker itself was unavailable; tests use it as their skip reason.</summary>
    public string? SkipReason { get; private set; }

    public DbContextOptions<NativeClaimDbContext> NewOptions()
        => new DbContextOptionsBuilder<NativeClaimDbContext>()
            .UseSqlServer(ConnectionString)
            .Options;

    /// <summary>Dedicated application database; RCSI is set here, never on <c>master</c>.</summary>
    private const string DatabaseName = "OrionPatchClaim";

    public async Task InitializeAsync()
    {
        SkipReason = await DockerAvailability.StartOrSkipAsync(
            startAsync: () => container.StartAsync(),
            setupAsync: async () =>
            {
                // The container connection string targets master. Create a dedicated database, enable
                // RCSI on it, then point the suite at it - we must never SET SINGLE_USER / RCSI on the
                // master system database.
                await using (var master = new NativeClaimDbContext(
                    new DbContextOptionsBuilder<NativeClaimDbContext>()
                        .UseSqlServer(container.GetConnectionString())
                        .Options))
                {
                    await master.Database.ExecuteSqlRawAsync(
                        $"IF DB_ID('{DatabaseName}') IS NULL CREATE DATABASE [{DatabaseName}];");

                    // ALTER DATABASE ... SET READ_COMMITTED_SNAPSHOT cannot run inside a user
                    // transaction and needs to be the only connection to the target database. The
                    // database is freshly created with no other sessions, so a plain ALTER suffices.
                    // Once on, the default READ COMMITTED session reads through row versions - the
                    // exact condition under which the claim's READCOMMITTEDLOCK hint must keep READPAST
                    // skipping locked rows.
                    await master.Database.ExecuteSqlRawAsync(
                        $"ALTER DATABASE [{DatabaseName}] SET READ_COMMITTED_SNAPSHOT ON;");
                }

                // Testcontainers.MsSql 3.10.0 hardcodes the connection string to master, so retarget
                // it to the dedicated database ourselves.
                ConnectionString = new SqlConnectionStringBuilder(container.GetConnectionString())
                {
                    InitialCatalog = DatabaseName,
                }.ConnectionString;

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

/// <summary>xUnit collection so the SQL Server container is shared across the native-claim tests.</summary>
[CollectionDefinition(Name)]
public sealed class SqlServerClaimGroup : ICollectionFixture<SqlServerFixture>
{
    public const string Name = "sqlserver-native-claim";
}
