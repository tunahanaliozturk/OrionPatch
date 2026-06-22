namespace Moongazing.OrionPatch.EntityFrameworkCore.Tests;

using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Moongazing.OrionPatch.EntityFrameworkCore;

internal static class TestDb
{
    public static async Task<AppDbContext> CreateAsync()
    {
        var connection = new SqliteConnection("DataSource=:memory:");
        await connection.OpenAsync();
        return await CreateAsync(connection);
    }

    /// <summary>
    /// Open an in-memory SQLite connection that outlives a single <see cref="AppDbContext"/>. The
    /// schema and data persist for as long as the connection is open, so several contexts created
    /// over the same connection (via <see cref="CreateAsync(SqliteConnection)"/>) share one database.
    /// Used to exercise cross-context behavior such as a crash-replayed terminal path running on a
    /// fresh scope. The caller owns the connection's lifetime.
    /// </summary>
    public static SqliteConnection OpenSharedConnection()
    {
        var connection = new SqliteConnection("DataSource=:memory:");
        connection.Open();
        return connection;
    }

    /// <summary>
    /// Create a context bound to the supplied connection. <see cref="DbContext.Database"/>'s
    /// <c>EnsureCreatedAsync</c> is idempotent, so calling this repeatedly over one shared
    /// connection reuses the already-created schema.
    /// </summary>
    public static async Task<AppDbContext> CreateAsync(SqliteConnection connection)
    {
        var options = new DbContextOptionsBuilder<AppDbContext>()
            .UseSqlite(connection)
            .AddInterceptors(new OrionPatchSaveChangesInterceptor())
            .Options;
        var db = new AppDbContext(options);
        await db.Database.EnsureCreatedAsync();
        return db;
    }
}

internal sealed class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Sample>(b =>
        {
            b.HasKey(x => x.Id);
            b.Property(x => x.Name).IsRequired();
        });
        modelBuilder.ApplyOrionPatchConfiguration();
    }
}
