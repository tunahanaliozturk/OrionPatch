namespace Moongazing.OrionPatch.DependencyInjection;

using Microsoft.Extensions.DependencyInjection;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Channel;

/// <summary>
/// Fluent extensions on <see cref="OrionPatchBuilder"/> for registering sinks.
/// </summary>
public static class OutboxBuilderExtensions
{
    /// <summary>
    /// Register a custom <typeparamref name="TSink"/> as the singleton <see cref="IOutboxSink"/>.
    /// </summary>
    /// <typeparam name="TSink">Concrete sink type implementing <see cref="IOutboxSink"/>.</typeparam>
    /// <param name="builder">Builder returned from <see cref="OrionPatchServiceCollectionExtensions.AddOrionPatch"/>.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="builder"/> is null.</exception>
    public static OrionPatchBuilder UseSink<TSink>(this OrionPatchBuilder builder)
        where TSink : class, IOutboxSink
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.AddSingleton<IOutboxSink, TSink>();
        return builder;
    }

    /// <summary>
    /// Register the in-process <see cref="ChannelOutboxSink"/> as the singleton
    /// <see cref="IOutboxSink"/>. The same instance is also resolvable as the concrete
    /// <see cref="ChannelOutboxSink"/> so consumers can grab <see cref="ChannelOutboxSink.Reader"/>.
    /// </summary>
    /// <param name="builder">Builder returned from <see cref="OrionPatchServiceCollectionExtensions.AddOrionPatch"/>.</param>
    /// <param name="configure">Optional callback to tune <see cref="ChannelOutboxSinkOptions"/>; defaults are applied when omitted.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="builder"/> is null.</exception>
    public static OrionPatchBuilder UseChannelSink(
        this OrionPatchBuilder builder,
        Action<ChannelOutboxSinkOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var options = new ChannelOutboxSinkOptions();
        configure?.Invoke(options);

        // Register the options instance and the concrete sink as one singleton,
        // then expose the same instance through the IOutboxSink contract.
        builder.Services.AddSingleton(options);
        builder.Services.AddSingleton<ChannelOutboxSink>();
        builder.Services.AddSingleton<IOutboxSink>(sp => sp.GetRequiredService<ChannelOutboxSink>());

        return builder;
    }
}
