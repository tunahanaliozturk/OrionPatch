using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Moongazing.OrionPatch.Abstractions;
using RabbitMQ.Client;

namespace Moongazing.OrionPatch.RabbitMQ;

/// <summary>
/// DI helpers for the RabbitMQ publisher sink.
/// </summary>
public static class RabbitMqOutboxSinkServiceCollectionExtensions
{
    /// <summary>
    /// Register <see cref="RabbitMqOutboxSink"/> as the singleton
    /// <see cref="IOutboxSink"/>. When <see cref="RabbitMqOutboxSinkOptions.ConnectionString"/>
    /// is set, the helper also registers a singleton <see cref="IConnection"/> built from
    /// that connection string; otherwise the caller must register an <see cref="IConnection"/>
    /// themselves (e.g., for connection sharing across multiple sinks or for an existing
    /// RabbitMQ.Client wiring).
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Configuration callback.</param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    public static IServiceCollection AddOrionPatchRabbitMqSink(
        this IServiceCollection services,
        Action<RabbitMqOutboxSinkOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.Configure(configure);

        // Probe options for a connection string. If supplied, the helper wires a
        // ConnectionFactory-backed singleton IConnection; otherwise we trust the caller.
        var probe = new RabbitMqOutboxSinkOptions();
        configure(probe);
        if (!string.IsNullOrWhiteSpace(probe.ConnectionString))
        {
            var connString = probe.ConnectionString;
            services.TryAddSingleton<IConnection>(_ =>
            {
                var factory = new ConnectionFactory
                {
                    Uri = new Uri(connString),
                    // DispatchConsumersAsync MUST be true so the AsyncEventingBasicConsumer
                    // used by RabbitMqOutboxConsumer (v0.2.5) raises Received as an async
                    // event. With this flag false, the async handler runs on the
                    // synchronous dispatch loop and any await inside it deadlocks the
                    // consumer; the sink path is unaffected either way.
                    DispatchConsumersAsync = true,
                };
                return factory.CreateConnection();
            });
        }

        services.AddSingleton<IOutboxSink, RabbitMqOutboxSink>();

        return services;
    }

    /// <summary>
    /// Register the <see cref="RabbitMqOutboxConsumer"/> hosted service alongside an
    /// <typeparamref name="THandler"/> that handles each first-delivery envelope. The
    /// handler is registered scoped so it can take scoped collaborators (DbContext,
    /// repositories) as constructor parameters; each delivery uses a fresh scope.
    /// </summary>
    /// <remarks>
    /// PREREQUISITE: an <see cref="IConnection"/> must already be registered in DI. The
    /// recommended path is to call <see cref="AddOrionPatchRabbitMqSink"/> first with a
    /// connection string (which auto-wires the connection with
    /// <c>DispatchConsumersAsync = true</c>, which the async consumer needs); alternatively
    /// register your own <see cref="IConnection"/> singleton and ensure its underlying
    /// <see cref="ConnectionFactory.DispatchConsumersAsync"/> is true, otherwise the
    /// AsyncEventingBasicConsumer's async event handler deadlocks the dispatch loop.
    /// </remarks>
    /// <typeparam name="THandler">Consumer-supplied handler.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Consumer configuration callback.</param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    public static IServiceCollection AddOrionPatchRabbitMqConsumer<THandler>(
        this IServiceCollection services,
        Action<RabbitMqOutboxConsumerOptions> configure)
        where THandler : class, IOrionPatchMessageHandler
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.Configure(configure);
        services.AddScoped<IOrionPatchMessageHandler, THandler>();
        services.AddHostedService<RabbitMqOutboxConsumer>();
        return services;
    }
}
