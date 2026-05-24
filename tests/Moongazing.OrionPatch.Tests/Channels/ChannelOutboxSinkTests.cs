namespace Moongazing.OrionPatch.Tests.Channels;

using System.Threading.Channels;
using Moongazing.OrionPatch.Channels;
using Moongazing.OrionPatch.Models;
using Xunit;

public class ChannelOutboxSinkTests
{
    [Fact]
    public async Task SendAsync_ShouldMakeEnvelopeReadable_WhenReaderConsumes()
    {
        var sink = new ChannelOutboxSink(new ChannelOutboxSinkOptions { Capacity = 8 });
        var envelope = new OutboxEnvelope(
            Guid.NewGuid(), "T", "{}", null, null, DateTime.UtcNow, 1);

        await sink.SendAsync(envelope, CancellationToken.None);
        var read = await sink.Reader.ReadAsync();

        Assert.Equal(envelope.Id, read.Id);
    }

    [Fact]
    public async Task SendAsync_ShouldBlock_WhenChannelIsFullAndModeIsWait()
    {
        var sink = new ChannelOutboxSink(new ChannelOutboxSinkOptions
        {
            Capacity = 1,
            FullMode = BoundedChannelFullMode.Wait,
        });
        var envelope = new OutboxEnvelope(
            Guid.NewGuid(), "T", "{}", null, null, DateTime.UtcNow, 1);

        await sink.SendAsync(envelope, CancellationToken.None);
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        await Assert.ThrowsAsync<OperationCanceledException>(
            () => sink.SendAsync(envelope, cts.Token));
    }

    [Fact]
    public void Constructor_ShouldThrow_WhenOptionsIsNull()
    {
        Assert.Throws<ArgumentNullException>(() => new ChannelOutboxSink(null!));
    }
}
