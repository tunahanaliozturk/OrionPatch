namespace Moongazing.OrionPatch.EntityFrameworkCore.Claims;

using Microsoft.EntityFrameworkCore;

/// <summary>
/// Picks the right <see cref="IClaimStrategy"/> for the consumer's EF Core provider.
/// </summary>
internal static class ProviderClaimStrategy
{
    /// <summary>
    /// Resolve the strategy for the given <see cref="DbContext"/>'s database provider.
    /// </summary>
    /// <param name="db">DbContext whose <c>Database.ProviderName</c> drives the selection; must be non-null.</param>
    /// <returns>
    /// <see cref="SkipLockedClaimStrategy"/> with the matching <see cref="SqlDialect"/> for
    /// SQL Server, PostgreSQL, MySQL / MariaDB; <see cref="CompareAndSwapClaimStrategy"/> for
    /// SQLite and every unrecognized provider.
    /// </returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="db"/> is null.</exception>
    public static IClaimStrategy For(DbContext db)
    {
        ArgumentNullException.ThrowIfNull(db);
        var provider = db.Database.ProviderName ?? string.Empty;
        if (provider.StartsWith("Microsoft.EntityFrameworkCore.SqlServer", StringComparison.Ordinal))
        {
            return new SkipLockedClaimStrategy(SqlDialect.SqlServer);
        }
        if (provider.StartsWith("Npgsql.EntityFrameworkCore.PostgreSQL", StringComparison.Ordinal))
        {
            return new SkipLockedClaimStrategy(SqlDialect.PostgreSql);
        }
        if (provider.StartsWith("Pomelo.EntityFrameworkCore.MySql", StringComparison.Ordinal) ||
            provider.StartsWith("MySql.EntityFrameworkCore", StringComparison.Ordinal))
        {
            return new SkipLockedClaimStrategy(SqlDialect.MySql);
        }
        // SQLite and unrecognized providers use the portable compare-and-swap fallback.
        return new CompareAndSwapClaimStrategy();
    }
}
