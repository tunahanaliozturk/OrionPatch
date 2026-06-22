namespace Moongazing.OrionPatch.EntityFrameworkCore.Configuration;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

/// <summary>
/// EF Core mapping for <see cref="OutboxArchiveRow"/> to table <c>OrionPatch_OutboxArchive</c>.
/// This is the cold landing area for processed rows reaped past the retention window; it is never
/// on the dispatcher's claim path, so it carries only a <c>ProcessedAtUtc</c> index for retention
/// sweeps and audit range queries rather than the active outbox's status/lease covering indexes.
/// Property facets mirror <see cref="OutboxEntityConfiguration"/> so a reaped row round-trips faithfully.
/// </summary>
internal sealed class OutboxArchiveEntityConfiguration : IEntityTypeConfiguration<OutboxArchiveRow>
{
    /// <summary>Apply the entity-type configuration.</summary>
    /// <param name="builder">EF Core entity builder for <see cref="OutboxArchiveRow"/>; must be non-null.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="builder"/> is null.</exception>
    public void Configure(EntityTypeBuilder<OutboxArchiveRow> builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.ToTable("OrionPatch_OutboxArchive");
        builder.HasKey(x => x.Id);

        builder.Property(x => x.MessageType).HasMaxLength(256).IsRequired();
        builder.Property(x => x.Payload).IsRequired();
        builder.Property(x => x.HeadersJson);
        builder.Property(x => x.CorrelationId).HasMaxLength(128);
        builder.Property(x => x.OccurredAtUtc).IsRequired();
        builder.Property(x => x.EnqueuedAtUtc).IsRequired();
        builder.Property(x => x.AttemptCount).IsRequired();
        builder.Property(x => x.LastError);
        builder.Property(x => x.ProcessedAtUtc);

        builder.HasIndex(x => x.ProcessedAtUtc)
            .HasDatabaseName("IX_OrionPatch_OutboxArchive_ProcessedAt");
    }
}
