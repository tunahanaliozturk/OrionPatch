namespace Moongazing.OrionPatch.Tests.Hosting;

using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Models;
using Xunit;

public sealed class DeadLetterSinkTests
{
    [Fact]
    public async Task NullDeadLetterSink_OnDeadLetteredAsync_completes_without_throwing()
    {
        var sut = new NullDeadLetterSink();

        await sut.OnDeadLetteredAsync(System.Guid.NewGuid(), null, "err", attemptCount: 5, CancellationToken.None);
    }

    [Fact]
    public async Task Custom_sink_receives_the_expected_row_id_and_error()
    {
        System.Guid? captured = null;
        string? capturedError = null;
        var sut = new CapturingSink((id, env, err) =>
        {
            captured = id;
            capturedError = err;
        });

        var rowId = System.Guid.NewGuid();
        await sut.OnDeadLetteredAsync(rowId, null, "fatal-payload", attemptCount: 7, CancellationToken.None);

        Assert.Equal(rowId, captured);
        Assert.Equal("fatal-payload", capturedError);
    }

    private sealed class CapturingSink : IDeadLetterSink
    {
        private readonly System.Action<System.Guid, OutboxEnvelope?, string> capture;
        public CapturingSink(System.Action<System.Guid, OutboxEnvelope?, string> capture) => this.capture = capture;
        public Task OnDeadLetteredAsync(System.Guid rowId, OutboxEnvelope? envelope, string errorMessage, int attemptCount, CancellationToken cancellationToken)
        {
            capture(rowId, envelope, errorMessage);
            return Task.CompletedTask;
        }
    }
}
