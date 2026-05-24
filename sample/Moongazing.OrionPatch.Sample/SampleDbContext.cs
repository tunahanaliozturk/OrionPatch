namespace Moongazing.OrionPatch.Sample;

using Microsoft.EntityFrameworkCore;
using Moongazing.OrionPatch.EntityFrameworkCore;

internal sealed class SampleDbContext(DbContextOptions<SampleDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder) =>
        modelBuilder.ApplyOrionPatchConfiguration();
}
