namespace Moongazing.OrionPatch.EntityFrameworkCore;

using Microsoft.EntityFrameworkCore;
using Moongazing.OrionPatch.EntityFrameworkCore.Configuration;

/// <summary>
/// Convenience extensions for wiring OrionPatch into an EF Core <see cref="DbContext"/>.
/// </summary>
public static class OrionPatchDbContextExtensions
{
    /// <summary>
    /// Apply the OrionPatch EF Core entity configurations to the supplied <see cref="ModelBuilder"/>.
    /// Call this from your DbContext's <c>OnModelCreating</c> override.
    /// </summary>
    /// <param name="modelBuilder">The model builder; must be non-null.</param>
    /// <returns>The same <see cref="ModelBuilder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="modelBuilder"/> is null.</exception>
    /// <remarks>
    /// Maps four tables:
    /// <list type="bullet">
    /// <item><c>OrionPatch_Outbox</c> (<see cref="Models.OutboxRow"/>) - the active outbox.</item>
    /// <item><c>OrionPatch_Inbox</c> (<see cref="InboxRow"/>, from v0.2.3) - inbox dedup.</item>
    /// <item><c>OrionPatch_DeadLetter</c> (<see cref="DeadLetterRow"/>, from v0.3.2) - the durable
    /// dead-letter destination behind <see cref="Abstractions.IDeadLetterStore"/>.</item>
    /// <item><c>OrionPatch_OutboxArchive</c> (<see cref="OutboxArchiveRow"/>, from v0.3.2) - the
    /// archive landing area behind <see cref="Abstractions.IOutboxArchivalStore"/> in archive mode.</item>
    /// </list>
    /// After adding the v0.3.2 tables, regenerate your DbContext migration (for example
    /// <c>dotnet ef migrations add OrionPatch_v0_3_2_DeadLetterAndArchive</c>) and apply it. The
    /// runtime never creates these tables; the two new ones are inert until the dead-letter store or
    /// the retention reaper is invoked.
    /// </remarks>
    public static ModelBuilder ApplyOrionPatchConfiguration(this ModelBuilder modelBuilder)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);
        modelBuilder.ApplyConfiguration(new OutboxEntityConfiguration());
        modelBuilder.ApplyConfiguration(new InboxEntityConfiguration());
        modelBuilder.ApplyConfiguration(new DeadLetterEntityConfiguration());
        modelBuilder.ApplyConfiguration(new OutboxArchiveEntityConfiguration());
        return modelBuilder;
    }
}
