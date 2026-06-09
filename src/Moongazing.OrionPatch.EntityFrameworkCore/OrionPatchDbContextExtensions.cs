namespace Moongazing.OrionPatch.EntityFrameworkCore;

using Microsoft.EntityFrameworkCore;
using Moongazing.OrionPatch.EntityFrameworkCore.Configuration;

/// <summary>
/// Convenience extensions for wiring OrionPatch into an EF Core <see cref="DbContext"/>.
/// </summary>
public static class OrionPatchDbContextExtensions
{
    /// <summary>
    /// Apply the OrionPatch EF Core entity configurations (<see cref="Models.OutboxRow"/>
    /// and, from v0.2.3, <see cref="InboxRow"/>) to the supplied <see cref="ModelBuilder"/>.
    /// Call this from your DbContext's <c>OnModelCreating</c> override.
    /// </summary>
    /// <param name="modelBuilder">The model builder; must be non-null.</param>
    /// <returns>The same <see cref="ModelBuilder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="modelBuilder"/> is null.</exception>
    public static ModelBuilder ApplyOrionPatchConfiguration(this ModelBuilder modelBuilder)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);
        modelBuilder.ApplyConfiguration(new OutboxEntityConfiguration());
        modelBuilder.ApplyConfiguration(new InboxEntityConfiguration());
        return modelBuilder;
    }
}
