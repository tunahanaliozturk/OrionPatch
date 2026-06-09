namespace Moongazing.OrionPatch.EntityFrameworkCore.Configuration;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

/// <summary>
/// EF Core mapping for <see cref="InboxRow"/> to table <c>OrionPatch_Inbox</c>. The composite
/// primary key on (<c>MessageId</c>, <c>Consumer</c>) lets a single inbox table serve
/// multiple consumers without one consumer's accepted-set masking another's. When
/// <c>Consumer</c> is null, the row keys on <c>MessageId</c> alone for the default
/// single-consumer setup.
/// </summary>
internal sealed class InboxEntityConfiguration : IEntityTypeConfiguration<InboxRow>
{
    public void Configure(EntityTypeBuilder<InboxRow> builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.ToTable("OrionPatch_Inbox");
        builder.HasKey(x => new { x.MessageId, x.Consumer });

        builder.Property(x => x.MessageId).IsRequired();
        builder.Property(x => x.AcceptedAtUtc).IsRequired();
        builder.Property(x => x.Consumer).HasMaxLength(128);
    }
}
