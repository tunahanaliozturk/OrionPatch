namespace Moongazing.OrionPatch.DependencyInjection;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Configuration;
using Moongazing.OrionPatch.Hosting;
using Moongazing.OrionPatch.Internal;

/// <summary>
/// <see cref="IServiceCollection"/> entry points for OrionPatch.
/// </summary>
public static class OrionPatchServiceCollectionExtensions
{
    /// <summary>
    /// Register OrionPatch core services: <see cref="OrionPatchOptions"/>, clock, message
    /// serializer, and the message-type-name resolver. Returns an
    /// <see cref="OrionPatchBuilder"/> for chaining sink and storage registrations.
    /// </summary>
    /// <param name="services">Host service collection.</param>
    /// <param name="configure">Optional callback to mutate <see cref="OrionPatchOptions"/>.</param>
    /// <returns>An <see cref="OrionPatchBuilder"/> for fluent chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="services"/> is null.</exception>
    public static OrionPatchBuilder AddOrionPatch(
        this IServiceCollection services,
        Action<OrionPatchOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddOptions();
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.TryAddSingleton<IOutboxDispatcherClock, SystemClock>();
        services.TryAddSingleton(_ => Moongazing.OrionPatch.Configuration.MessageTypeRegistry.Empty);
        services.TryAddSingleton<MessageTypeNameResolver>();
        services.TryAddSingleton(sp =>
            new MessageSerializer(sp.GetRequiredService<IOptions<OrionPatchOptions>>().Value.JsonOptions));

        services.AddSingleton<IHostedService>(sp =>
        {
            var opts = sp.GetRequiredService<IOptions<OrionPatchOptions>>().Value;
            if (!opts.DispatcherEnabled)
            {
                return new NoOpHostedService();
            }
            // v0.2.20: explicit resolution so both observability hooks
            // (IDeadLetterSink, IOutboxDispatchObserver) can be registered
            // independently. ActivatorUtilities.CreateInstance falls back to a shorter
            // ctor if any single nullable parameter has no registered service, which
            // would silently drop one hook when the consumer registered only the other.
            return new OutboxDispatcherHostedService(
                sp.GetRequiredService<Abstractions.IOutboxStorage>(),
                sp.GetRequiredService<Abstractions.IOutboxSink>(),
                sp.GetRequiredService<IOptions<Configuration.OrionPatchOptions>>(),
                sp.GetRequiredService<Abstractions.IOutboxDispatcherClock>(),
                sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<OutboxDispatcherHostedService>>(),
                sp.GetService<Abstractions.IDeadLetterSink>(),
                sp.GetService<Abstractions.IOutboxDispatchObserver>());
        });

        return new OrionPatchBuilder(services);
    }
}
