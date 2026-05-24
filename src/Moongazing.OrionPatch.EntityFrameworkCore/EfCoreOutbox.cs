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
/// Construction is performed by the DI helper (Task 7) or by the test harness via the
/// internal constructor; consumers do not instantiate this type directly. Each instance
/// binds itself to its <see cref="DbContext"/> via a <see cref="ConditionalWeakTable{TKey, TValue}"/>
/// so the interceptor can locate the buffered rows without taking a dependency on the
/// application <see cref="IServiceProvider"/>.
/// </remarks>
public sealed class EfCoreOutbox : IOutbox
{
    private static readonly ConditionalWeakTable<DbContext, EfCoreOutbox> Binding = new();

    private readonly DbContext db;
    private readonly MessageTypeNameResolver typeResolver;
    private readonly MessageSerializer serializer;
    private readonly List<OutboxRow> buffer = new();

    /// <summary>
    /// Create the outbox bound to a specific <see cref="DbContext"/> instance.
    /// </summary>
    /// <param name="db">DbContext whose SaveChanges flushes the buffer; must be non-null.</param>
    /// <param name="typeResolver">Resolves the <c>MessageType</c> string for enqueued payloads; must be non-null.</param>
    /// <param name="serializer">JSON serializer for payloads; must be non-null.</param>
    /// <exception cref="ArgumentNullException">Thrown when any argument is null.</exception>
    internal EfCoreOutbox(DbContext db, MessageTypeNameResolver typeResolver, MessageSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(db);
        ArgumentNullException.ThrowIfNull(typeResolver);
        ArgumentNullException.ThrowIfNull(serializer);
        this.db = db;
        this.typeResolver = typeResolver;
        this.serializer = serializer;

        // AddOrUpdate handles the case where a previous outbox was bound to this DbContext
        // (e.g. resolved twice in a single scope). Last writer wins, matching scoped-DI semantics.
        Binding.AddOrUpdate(db, this);
    }

    /// <summary>Buffer access for <see cref="OrionPatchSaveChangesInterceptor"/>. Internal — not part of the public contract.</summary>
    internal IList<OutboxRow> Buffer => buffer;

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
        var row = new OutboxRow
        {
            Id = Guid.NewGuid(),
            MessageType = typeResolver.Resolve(typeof(T), options),
            Payload = serializer.Serialize(message),
            HeadersJson = options?.Headers is null
                ? null
                : JsonSerializer.Serialize(options.Headers),
            CorrelationId = options?.CorrelationId,
            OccurredAtUtc = occurredAt,
            EnqueuedAtUtc = occurredAt,
            Status = OutboxStatus.Pending,
            AttemptCount = 0,
            NextAttemptAtUtc = occurredAt,
        };
        buffer.Add(row);
    }
}
