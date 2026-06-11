namespace Moongazing.OrionPatch.Kafka.Inbound;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// EF Core-backed <see cref="IKafkaAttemptCountStore"/>. Survives consumer restarts so
/// poison-pill DLQ routing becomes transactional - the attempt counter for an envelope
/// id persists in <typeparamref name="TDbContext"/>'s table regardless of process churn.
/// Use this for production deployments where the v0.2.10
/// <see cref="InMemoryKafkaAttemptCountStore"/> best-effort behaviour is not acceptable.
/// </summary>
/// <typeparam name="TDbContext">
/// The consumer's <see cref="DbContext"/> that owns the
/// <see cref="KafkaInboundAttempt"/> table. Wire the entity in
/// <c>OnModelCreating</c> via <c>modelBuilder.Entity&lt;KafkaInboundAttempt&gt;(b => ...)</c>
/// or rely on the convenience <see cref="KafkaInboundAttempt.Configure"/> helper that
/// registers a sensible default mapping.
/// </typeparam>
public sealed class EfCoreKafkaAttemptCountStore<TDbContext> : IKafkaAttemptCountStore
    where TDbContext : DbContext
{
    private readonly IServiceScopeFactory scopeFactory;

    /// <summary>Construct with the consumer's <see cref="IServiceScopeFactory"/>.</summary>
    public EfCoreKafkaAttemptCountStore(IServiceScopeFactory scopeFactory)
    {
        ArgumentNullException.ThrowIfNull(scopeFactory);
        this.scopeFactory = scopeFactory;
    }

    /// <inheritdoc />
    public async ValueTask<int> GetAsync(Guid envelopeId, CancellationToken cancellationToken)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDbContext>();
        var row = await db.Set<KafkaInboundAttempt>()
            .AsNoTracking()
            .FirstOrDefaultAsync(a => a.EnvelopeId == envelopeId, cancellationToken)
            .ConfigureAwait(false);
        return row?.AttemptCount ?? 0;
    }

    /// <inheritdoc />
    public async ValueTask SetAsync(Guid envelopeId, int attemptCount, CancellationToken cancellationToken)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDbContext>();
        var existing = await db.Set<KafkaInboundAttempt>()
            .FirstOrDefaultAsync(a => a.EnvelopeId == envelopeId, cancellationToken)
            .ConfigureAwait(false);
        if (existing is null)
        {
            db.Set<KafkaInboundAttempt>().Add(new KafkaInboundAttempt
            {
                EnvelopeId = envelopeId,
                AttemptCount = attemptCount,
                LastUpdatedUtc = DateTime.UtcNow,
            });
        }
        else
        {
            existing.AttemptCount = attemptCount;
            existing.LastUpdatedUtc = DateTime.UtcNow;
        }
        await db.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask ClearAsync(Guid envelopeId, CancellationToken cancellationToken)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDbContext>();
        await db.Set<KafkaInboundAttempt>()
            .Where(a => a.EnvelopeId == envelopeId)
            .ExecuteDeleteAsync(cancellationToken)
            .ConfigureAwait(false);
    }
}

/// <summary>
/// Persisted attempt-count row. One per envelope id. Consumers wire this entity in
/// <c>OnModelCreating</c> via <see cref="Configure"/> or with a custom mapping.
/// </summary>
public sealed class KafkaInboundAttempt
{
    /// <summary>Envelope id (primary key).</summary>
    public Guid EnvelopeId { get; set; }

    /// <summary>Failed delivery attempts persisted so far.</summary>
    public int AttemptCount { get; set; }

    /// <summary>Most recent update timestamp (operator inspection).</summary>
    public DateTime LastUpdatedUtc { get; set; }

    /// <summary>
    /// Convenience method to register the default mapping. Call from
    /// <c>OnModelCreating(ModelBuilder modelBuilder)</c>:
    /// <code>
    /// protected override void OnModelCreating(ModelBuilder modelBuilder)
    /// {
    ///     base.OnModelCreating(modelBuilder);
    ///     KafkaInboundAttempt.Configure(modelBuilder);
    /// }
    /// </code>
    /// </summary>
    public static void Configure(ModelBuilder modelBuilder)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);
        modelBuilder.Entity<KafkaInboundAttempt>(b =>
        {
            b.ToTable("OrionPatchKafkaInboundAttempts");
            b.HasKey(a => a.EnvelopeId);
            b.Property(a => a.AttemptCount).IsRequired();
            b.Property(a => a.LastUpdatedUtc).IsRequired();
        });
    }
}
