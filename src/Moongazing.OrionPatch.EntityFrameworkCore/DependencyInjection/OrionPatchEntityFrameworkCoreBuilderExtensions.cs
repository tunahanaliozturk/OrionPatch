namespace Moongazing.OrionPatch.EntityFrameworkCore.DependencyInjection;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.DependencyInjection;
using Moongazing.OrionPatch.Internal;

/// <summary>
/// <see cref="OrionPatchBuilder"/> extensions for wiring the EF Core storage backend
/// (<see cref="EfCoreOutbox"/>, <see cref="EfCoreOutboxStorage"/>, and the
/// <see cref="OrionPatchSaveChangesInterceptor"/>) for a specific consumer
/// <see cref="DbContext"/>.
/// </summary>
public static class OrionPatchEntityFrameworkCoreBuilderExtensions
{
    /// <summary>
    /// Register OrionPatch's EF Core storage backend bound to <typeparamref name="TDbContext"/>.
    /// </summary>
    /// <typeparam name="TDbContext">
    /// The consumer's <see cref="DbContext"/>. Must already be registered with
    /// <see cref="EntityFrameworkServiceCollectionExtensions.AddDbContext{TContext}(IServiceCollection, Action{DbContextOptionsBuilder}, ServiceLifetime, ServiceLifetime)"/>
    /// or one of its overloads.
    /// </typeparam>
    /// <param name="builder">
    /// Builder returned from <c>AddOrionPatch</c>
    /// (<see cref="OrionPatchServiceCollectionExtensions"/>); must be non-null.
    /// </param>
    /// <returns>The same <paramref name="builder"/> for fluent chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="builder"/> is null.</exception>
    /// <remarks>
    /// <para>
    /// Registers:
    /// <list type="bullet">
    /// <item><see cref="OrionPatchSaveChangesInterceptor"/> as a singleton (stateless).</item>
    /// <item><see cref="EfCoreOutbox"/> and <see cref="IOutbox"/> as scoped, both resolving to
    /// the same per-scope instance bound to <typeparamref name="TDbContext"/>.</item>
    /// <item><see cref="EfCoreOutboxStorage"/> and <see cref="IOutboxStorage"/> as scoped,
    /// both resolving to the same per-scope instance bound to <typeparamref name="TDbContext"/>.</item>
    /// </list>
    /// </para>
    /// <para>
    /// Consumers must also wire the interceptor into the DbContext options themselves by
    /// calling <see cref="UseOrionPatch(DbContextOptionsBuilder, IServiceProvider)"/> inside
    /// their <c>AddDbContext</c> registration. EF Core 8 does not expose
    /// <c>IDbContextOptionsConfiguration&lt;T&gt;</c>, so the wiring cannot be done
    /// implicitly from this extension.
    /// </para>
    /// <para>
    /// v0.1.0 supports one OrionPatch-bound DbContext per host. Calling this method twice
    /// registers duplicate factories for <see cref="IOutbox"/> and <see cref="IOutboxStorage"/>;
    /// the second call's registration wins for service resolution, but the first DbContext's
    /// outbox becomes unreachable through the abstraction. First-class multi-DbContext support
    /// is on the v0.2 roadmap. Until then, call this method exactly once per host.
    /// </para>
    /// </remarks>
    public static OrionPatchBuilder UseEntityFrameworkCore<TDbContext>(this OrionPatchBuilder builder)
        where TDbContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Services.TryAddSingleton<OrionPatchSaveChangesInterceptor>();

        builder.Services.AddScoped(sp => new EfCoreOutbox(
            sp.GetRequiredService<TDbContext>(),
            sp.GetRequiredService<MessageTypeNameResolver>(),
            sp.GetRequiredService<MessageSerializer>()));
        builder.Services.AddScoped<IOutbox>(sp => sp.GetRequiredService<EfCoreOutbox>());

        builder.Services.AddScoped(sp => new EfCoreOutboxStorage(
            sp.GetRequiredService<TDbContext>()));
        builder.Services.AddScoped<IOutboxStorage>(sp => sp.GetRequiredService<EfCoreOutboxStorage>());

        return builder;
    }

    /// <summary>
    /// Add OrionPatch's <see cref="OrionPatchSaveChangesInterceptor"/> to a
    /// <see cref="DbContextOptionsBuilder"/>. Call this inside the
    /// <c>(sp, options) =&gt; ...</c> overload of <c>AddDbContext</c> so the same
    /// interceptor instance registered by
    /// <see cref="UseEntityFrameworkCore{TDbContext}(OrionPatchBuilder)"/> is bound
    /// to every DbContext instance the container hands out.
    /// </summary>
    /// <param name="builder">The <see cref="DbContextOptionsBuilder"/> being configured; must be non-null.</param>
    /// <param name="serviceProvider">
    /// The <see cref="IServiceProvider"/> supplied to the <c>AddDbContext</c> callback;
    /// resolves the singleton <see cref="OrionPatchSaveChangesInterceptor"/>. Must be non-null.
    /// </param>
    /// <returns>The same <paramref name="builder"/> for fluent chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="builder"/> or <paramref name="serviceProvider"/> is null.</exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the <see cref="OrionPatchSaveChangesInterceptor"/> has not been
    /// registered. Ensure
    /// <see cref="UseEntityFrameworkCore{TDbContext}(OrionPatchBuilder)"/> ran before
    /// the container was built.
    /// </exception>
    /// <remarks>
    /// Call this method exactly once per <see cref="DbContextOptionsBuilder"/>. EF Core's
    /// <c>AddInterceptors</c> accumulates; calling <c>UseOrionPatch</c> twice would attach the
    /// same interceptor instance to the options twice and the interceptor would fire twice per
    /// save. The second invocation finds an empty buffer and no-ops, but it doubles the
    /// interceptor's dispatch cost on every <c>SaveChanges</c>.
    /// </remarks>
    public static DbContextOptionsBuilder UseOrionPatch(
        this DbContextOptionsBuilder builder,
        IServiceProvider serviceProvider)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(serviceProvider);

        builder.AddInterceptors(serviceProvider.GetRequiredService<OrionPatchSaveChangesInterceptor>());
        return builder;
    }
}
