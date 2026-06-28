namespace Moongazing.OrionPatch.EntityFrameworkCore.Tests.Native;

using Microsoft.EntityFrameworkCore;
using Moongazing.OrionPatch.EntityFrameworkCore;

/// <summary>
/// Minimal DbContext carrying only the OrionPatch schema, used by the native-claim integration
/// tests against real PostgreSQL / SQL Server containers.
/// </summary>
public sealed class NativeClaimDbContext(DbContextOptions<NativeClaimDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyOrionPatchConfiguration();
    }
}
