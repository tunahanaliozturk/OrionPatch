namespace Moongazing.OrionPatch.EntityFrameworkCore.Configuration;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

/// <summary>
/// EF Core mapping for <see cref="DeadLetterRow"/> to table <c>OrionPatch_DeadLetter</c>. The
/// primary key is the originating outbox row id, which makes dead-letter routing idempotent: a
/// replayed terminal path that re-inserts the same id violates the key and is treated by the
/// storage as the exactly-once no-op. Property facets (lengths, required) mirror
/// <see cref="OutboxEntityConfiguration"/> so a dead-lettered row round-trips without truncation.
/// </summary>
internal sealed class DeadLetterEntityConfiguration : IEntityTypeConfiguration<DeadLetterRow>
{
    /// <summary>Apply the entity-type configuration.</summary>
    /// <param name="builder">EF Core entity builder for <see cref="DeadLetterRow"/>; must be non-null.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="builder"/> is null.</exception>
    public void Configure(EntityTypeBuilder<DeadLetterRow> builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.ToTable("OrionPatch_DeadLetter");
        builder.HasKey(x => x.Id);

        builder.Property(x => x.MessageType).HasMaxLength(256).IsRequired();
        builder.Property(x => x.Payload).IsRequired();
        builder.Property(x => x.HeadersJson);
        builder.Property(x => x.CorrelationId).HasMaxLength(128);
        builder.Property(x => x.OccurredAtUtc).IsRequired();
        builder.Property(x => x.EnqueuedAtUtc).IsRequired();
        builder.Property(x => x.AttemptCount).IsRequired();
        builder.Property(x => x.FinalError).IsRequired();
        builder.Property(x => x.DeadLetteredAtUtc).IsRequired();

        // Triage reads page by recency: newest dead-letters first. Index DeadLetteredAtUtc so
        // GetDeadLetteredAsync's ordered, bounded read does not table-scan a large backlog.
        builder.HasIndex(x => x.DeadLetteredAtUtc)
            .HasDatabaseName("IX_OrionPatch_DeadLetter_DeadLetteredAt");
    }
}
