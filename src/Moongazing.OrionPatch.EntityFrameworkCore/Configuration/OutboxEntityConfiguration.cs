namespace Moongazing.OrionPatch.EntityFrameworkCore.Configuration;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Moongazing.OrionPatch.Models;

/// <summary>
/// EF Core mapping for <see cref="OutboxRow"/> to table <c>OrionPatch_Outbox</c>
/// with covering indexes for the dispatcher's polling and lease-expiry queries.
/// </summary>
internal sealed class OutboxEntityConfiguration : IEntityTypeConfiguration<OutboxRow>
{
    /// <summary>Apply the entity-type configuration.</summary>
    /// <param name="builder">EF Core entity builder for <see cref="OutboxRow"/>; must be non-null.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="builder"/> is null.</exception>
    public void Configure(EntityTypeBuilder<OutboxRow> builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.ToTable("OrionPatch_Outbox");
        builder.HasKey(x => x.Id);

        builder.Property(x => x.MessageType).HasMaxLength(256).IsRequired();
        builder.Property(x => x.Payload).IsRequired();
        builder.Property(x => x.HeadersJson);
        builder.Property(x => x.CorrelationId).HasMaxLength(128);
        builder.Property(x => x.OccurredAtUtc).IsRequired();
        builder.Property(x => x.EnqueuedAtUtc).IsRequired();
        builder.Property(x => x.Status).IsRequired();
        builder.Property(x => x.AttemptCount).IsRequired();
        builder.Property(x => x.ClaimedAtUtc);
        builder.Property(x => x.ClaimedBy).HasMaxLength(128);
        builder.Property(x => x.LastError);
        builder.Property(x => x.ProcessedAtUtc);
        builder.Property(x => x.NextAttemptAtUtc);

        // Optimistic-concurrency token for the SQLite fallback claim strategy in Task 6.
        builder.Property<byte[]>("RowVersion").IsRowVersion();

        builder.HasIndex(x => new { x.Status, x.NextAttemptAtUtc })
            .HasDatabaseName("IX_OrionPatch_Outbox_Status_NextAttempt");

        builder.HasIndex(x => x.ClaimedAtUtc)
            .HasDatabaseName("IX_OrionPatch_Outbox_ClaimedAt");
    }
}
