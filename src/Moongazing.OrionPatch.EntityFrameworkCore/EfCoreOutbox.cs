namespace Moongazing.OrionPatch.EntityFrameworkCore;

using System.Runtime.CompilerServices;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Internal;
using Moongazing.OrionPatch.Models;

/// <summary>
/// EF Core-backed <see cref="IOutbox"/>. <see cref="Enqueue{T}"/> buffers an
/// <see cref="OutboxRow"/> per call; the bound <see cref="OrionPatchSaveChangesInterceptor"/>
/// flushes the buffer into the supplied <see cref="DbContext"/> during
/// <c>SavingChanges</c>/<c>SavingChangesAsync</c> so the rows commit atomically with
/// the consumer's other entity changes.
/// </summary>
/// <remarks>
/// <para>
/// Construction is performed by the DI helper (Task 7) or by the test harness via the
/// internal constructor; consumers do not instantiate this type directly. Each instance
/// binds itself to its <see cref="DbContext"/> via a <see cref="ConditionalWeakTable{TKey, TValue}"/>
/// so the interceptor can locate the buffered rows without taking a dependency on the
/// application <see cref="IServiceProvider"/>.
/// </para>
/// <para>
/// Enqueue inherits the bound <see cref="DbContext"/>'s thread-affinity. EF Core
/// documents <see cref="DbContext"/> as not safe for concurrent use; the same restriction
/// applies to this outbox. Concurrent <see cref="Enqueue{T}"/> calls on a single instance
/// produce undefined behavior.
/// </para>
/// </remarks>
public sealed class EfCoreOutbox : IOutbox
{
    private static readonly ConditionalWeakTable<DbContext, EfCoreOutbox> Binding = new();

    private readonly DbContext db;
    private readonly MessageTypeNameResolver typeResolver;
    private readonly MessageSerializer serializer;
    private readonly IOutboxTenantResolver tenantResolver;
    private readonly List<OutboxRow> buffer = new();
    private readonly List<OutboxRow> pendingFlush = new();

    /// <summary>
    /// Create the outbox bound to a specific <see cref="DbContext"/> instance.
    /// </summary>
    /// <param name="db">DbContext whose SaveChanges flushes the buffer; must be non-null.</param>
    /// <param name="typeResolver">Resolves the <c>MessageType</c> string for enqueued payloads; must be non-null.</param>
    /// <param name="serializer">JSON serializer for payloads; must be non-null.</param>
    /// <param name="tenantResolver">
    /// Resolves the ambient tenant identifier at enqueue time. Optional; defaults to
    /// <see cref="NullOutboxTenantResolver"/> so the v0.2.0 behaviour is preserved when
    /// nothing is registered.
    /// </param>
    /// <exception cref="ArgumentNullException">Thrown when any required argument is null.</exception>
    /// <remarks>
    /// This constructor is internal. Consumers obtain an <see cref="IOutbox"/> instance
    /// via the DI registration shipped in <c>OrionPatch.EntityFrameworkCore</c>'s
    /// <c>UseEntityFrameworkCore</c> helper; the constructor accepts the bound
    /// <see cref="DbContext"/> + internal core helpers and is not part of the public
    /// contract.
    /// </remarks>
    internal EfCoreOutbox(
        DbContext db,
        MessageTypeNameResolver typeResolver,
        MessageSerializer serializer,
        IOutboxTenantResolver? tenantResolver = null)
    {
        ArgumentNullException.ThrowIfNull(db);
        ArgumentNullException.ThrowIfNull(typeResolver);
        ArgumentNullException.ThrowIfNull(serializer);
        this.db = db;
        this.typeResolver = typeResolver;
        this.serializer = serializer;
        this.tenantResolver = tenantResolver ?? new NullOutboxTenantResolver();

        // AddOrUpdate handles the case where a previous outbox was bound to this DbContext
        // (e.g. resolved twice in a single scope). Last writer wins, matching scoped-DI semantics.
        Binding.AddOrUpdate(db, this);
    }

    /// <summary>Buffer access for <see cref="OrionPatchSaveChangesInterceptor"/>. Internal — not part of the public contract.</summary>
    internal IList<OutboxRow> Buffer => buffer;

    /// <summary>
    /// Rows currently attached to the change tracker awaiting <c>SavedChanges</c> confirmation.
    /// Populated by <see cref="OrionPatchSaveChangesInterceptor"/> during <c>SavingChanges</c>,
    /// cleared on success, drained back to <see cref="Buffer"/> on failure. Internal — not part of the public contract.
    /// </summary>
    internal IList<OutboxRow> PendingFlush => pendingFlush;

    /// <summary>The DbContext this outbox is bound to. Internal — used by the interceptor to verify the right context fires.</summary>
    internal DbContext DbContext => db;

    /// <summary>
    /// Look up the <see cref="EfCoreOutbox"/> bound to <paramref name="db"/>, or
    /// <see langword="null"/> if no outbox has been constructed for that context.
    /// </summary>
    /// <param name="db">DbContext to look up; must be non-null.</param>
    /// <returns>The bound outbox, or <see langword="null"/> if none.</returns>
    internal static EfCoreOutbox? Find(DbContext db)
    {
        ArgumentNullException.ThrowIfNull(db);
        return Binding.TryGetValue(db, out var found) ? found : null;
    }

    /// <inheritdoc/>
    public void Enqueue<T>(T message, OutboxEnqueueOptions? options = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(message);

        var occurredAt = options?.OccurredAtUtc ?? DateTime.UtcNow;
        // v0.2.30: EnqueuedAtUtc is the real outbox WRITE time, captured here, and is kept distinct
        // from OccurredAtUtc (which the caller may backdate via OutboxEnqueueOptions.OccurredAtUtc to
        // reflect when the domain event happened). Stamping the actual write time keeps the
        // enqueue-based telemetry (pickup_lag_ms, dead_letter.age_ms) measuring outbox dwell rather
        // than the event backdate, and makes FIFO claim ordering reflect true enqueue order.
        var enqueuedAt = DateTime.UtcNow;

        // Tenant stamping (v0.2.1): if the registered IOutboxTenantResolver yields a value AND
        // the caller has not already supplied "tenant-id" in Headers, attach it. Caller
        // wins on conflict so explicit per-enqueue overrides remain authoritative.
        var headers = options?.Headers;
        var ambientTenant = tenantResolver.Resolve();
        if (!string.IsNullOrEmpty(ambientTenant)
            && (headers is null || !headers.ContainsKey(IOutboxTenantResolver.TenantHeaderName)))
        {
            var merged = headers is null
                ? new Dictionary<string, string>(StringComparer.Ordinal)
                : new Dictionary<string, string>(headers, StringComparer.Ordinal);
            merged[IOutboxTenantResolver.TenantHeaderName] = ambientTenant!;
            headers = merged;
        }

        var row = new OutboxRow
        {
            Id = Guid.NewGuid(),
            MessageType = typeResolver.Resolve(typeof(T), options),
            Payload = serializer.Serialize(message),
            HeadersJson = headers is null
                ? null
                : JsonSerializer.Serialize(headers, serializer.Options),
            CorrelationId = options?.CorrelationId,
            OccurredAtUtc = occurredAt,
            EnqueuedAtUtc = enqueuedAt,
            Status = OutboxStatus.Pending,
            AttemptCount = 0,
            NextAttemptAtUtc = occurredAt,
        };
        buffer.Add(row);
    }
}
