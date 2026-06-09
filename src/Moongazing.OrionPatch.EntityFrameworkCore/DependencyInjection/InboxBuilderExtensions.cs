namespace Moongazing.OrionPatch.EntityFrameworkCore.DependencyInjection;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.DependencyInjection;

/// <summary>
/// DI helpers for the EF Core-backed <see cref="IInbox"/>.
/// </summary>
public static class InboxBuilderExtensions
{
    /// <summary>
    /// Register <see cref="EfCoreInbox"/> bound to <typeparamref name="TDbContext"/> as the
    /// scoped <see cref="IInbox"/> implementation. Replaces any previously-registered
    /// <see cref="IInbox"/>.
    /// </summary>
    /// <typeparam name="TDbContext">The consumer's <see cref="DbContext"/> type.</typeparam>
    /// <param name="builder">The OrionPatch builder.</param>
    /// <param name="consumer">
    /// Optional consumer name. When supplied, the dedup row keys on
    /// (<c>MessageId</c>, <c>Consumer</c>) so two consumers can share the same inbox table
    /// without one consumer's accepted-set masking another's.
    /// </param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    public static OrionPatchBuilder UseEntityFrameworkCoreInbox<TDbContext>(
        this OrionPatchBuilder builder,
        string? consumer = null)
        where TDbContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Replace any prior IInbox registration (default InMemoryInbox or another impl) so
        // the EF Core inbox wins. Use AddScoped so the inbox shares the DbContext's lifetime.
        builder.Services.RemoveAll<IInbox>();
        builder.Services.AddScoped<IInbox>(sp =>
            new EfCoreInbox(sp.GetRequiredService<TDbContext>(), consumer));

        return builder;
    }
}
