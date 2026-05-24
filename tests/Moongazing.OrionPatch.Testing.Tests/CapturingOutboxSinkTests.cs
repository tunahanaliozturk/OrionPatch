namespace Moongazing.OrionPatch.Testing.Tests;

using Moongazing.OrionPatch.Models;
using Xunit;

public class CapturingOutboxSinkTests
{
    private static OutboxEnvelope NewEnvelope() => new(
        Guid.NewGuid(), "T", "{}", null, null, DateTime.UtcNow, 1);

    [Fact]
    public async Task SendAsync_ShouldRecordEnvelope_WhenCalled()
    {
        var sink = new CapturingOutboxSink();
        var envelope = NewEnvelope();

        await sink.SendAsync(envelope);

        Assert.Single(sink.Sent);
        Assert.Equal(envelope.Id, sink.Sent[0].Id);
    }

    [Fact]
    public async Task Sent_ShouldExposeReceivedEnvelopes_WhenAccessed()
    {
        var sink = new CapturingOutboxSink();
        var first = NewEnvelope();
        var second = NewEnvelope();

        await sink.SendAsync(first);
        await sink.SendAsync(second);

        Assert.Equal(2, sink.Sent.Count);
        Assert.Equal(first.Id, sink.Sent[0].Id);
        Assert.Equal(second.Id, sink.Sent[1].Id);
    }

    [Fact]
    public async Task Clear_ShouldResetRecordedEnvelopes_WhenCalled()
    {
        var sink = new CapturingOutboxSink();
        await sink.SendAsync(NewEnvelope());
        Assert.Single(sink.Sent);

        sink.Clear();

        Assert.Empty(sink.Sent);
    }
}
