namespace Moongazing.OrionPatch.Channel;

using System.Threading.Channels;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Models;

/// <summary>
/// In-process <see cref="IOutboxSink"/> backed by a bounded <see cref="System.Threading.Channels.Channel{T}"/>.
/// Useful for monoliths (in-process pub/sub fan-out) and unit tests. Zero external dependency.
/// Concrete broker sinks (RabbitMQ, Azure Service Bus, Kafka) live in opt-in sub-packages on
/// the v0.2+ roadmap; this sink is the only one shipped at v0.1.0.
/// </summary>
public sealed class ChannelOutboxSink : IOutboxSink
{
    private readonly Channel<OutboxEnvelope> channel;

    /// <summary>Create a sink with the supplied options.</summary>
    /// <param name="options">Capacity + full-mode configuration; must be non-null.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="options"/> is null.</exception>
    public ChannelOutboxSink(ChannelOutboxSinkOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        channel = System.Threading.Channels.Channel.CreateBounded<OutboxEnvelope>(
            new BoundedChannelOptions(options.Capacity)
            {
                FullMode = options.FullMode,
                SingleReader = false,
                SingleWriter = false,
            });
    }

    /// <summary>Reader side of the channel. Consumers drain envelopes through this.</summary>
    public ChannelReader<OutboxEnvelope> Reader => channel.Reader;

    /// <inheritdoc/>
    public Task SendAsync(OutboxEnvelope envelope, CancellationToken cancellationToken = default) =>
        channel.Writer.WriteAsync(envelope, cancellationToken).AsTask();
}
