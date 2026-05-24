namespace Moongazing.OrionPatch.Channels;

using System.Threading.Channels;

/// <summary>
/// Tuning knobs for <see cref="ChannelOutboxSink"/>.
/// </summary>
public sealed class ChannelOutboxSinkOptions
{
    /// <summary>Maximum number of envelopes held in the channel before back-pressure kicks in. Default: 1000.</summary>
    public int Capacity { get; init; } = 1000;

    /// <summary>How the writer behaves when the channel is full. Default: <see cref="BoundedChannelFullMode.Wait"/> (block the producer).</summary>
    public BoundedChannelFullMode FullMode { get; init; } = BoundedChannelFullMode.Wait;
}
