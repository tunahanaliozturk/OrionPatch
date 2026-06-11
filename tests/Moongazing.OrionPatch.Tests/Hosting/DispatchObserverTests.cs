namespace Moongazing.OrionPatch.Tests.Hosting;

using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Models;
using Xunit;

public sealed class DispatchObserverTests
{
    [Fact]
    public async Task NullOutboxDispatchObserver_OnDispatchedAsync_completes_without_throwing()
    {
        var sut = new NullOutboxDispatchObserver();
        var envelope = new OutboxEnvelope(
            System.Guid.NewGuid(), "Demo.Event", "{}", null, null, System.DateTime.UtcNow, 1);

        await sut.OnDispatchedAsync(envelope, attemptCount: 1, dispatchDurationMs: 5.5, System.Threading.CancellationToken.None);
    }

    [Fact]
    public async Task Custom_observer_receives_envelope_attempt_and_duration()
    {
        OutboxEnvelope? capturedEnvelope = null;
        int capturedAttempt = -1;
        double capturedDuration = -1;
        var sut = new CapturingObserver((env, attempt, duration) =>
        {
            capturedEnvelope = env;
            capturedAttempt = attempt;
            capturedDuration = duration;
        });

        var envelope = new OutboxEnvelope(
            System.Guid.NewGuid(), "Demo.Event", "{}", null, "corr-1", System.DateTime.UtcNow, 3);

        await sut.OnDispatchedAsync(envelope, attemptCount: 3, dispatchDurationMs: 42.5, System.Threading.CancellationToken.None);

        Assert.NotNull(capturedEnvelope);
        Assert.Equal(envelope.Id, capturedEnvelope!.Id);
        Assert.Equal(3, capturedAttempt);
        Assert.Equal(42.5, capturedDuration);
    }

    private sealed class CapturingObserver : IOutboxDispatchObserver
    {
        private readonly System.Action<OutboxEnvelope, int, double> capture;
        public CapturingObserver(System.Action<OutboxEnvelope, int, double> capture) => this.capture = capture;
        public Task OnDispatchedAsync(OutboxEnvelope envelope, int attemptCount, double dispatchDurationMs, System.Threading.CancellationToken cancellationToken)
        {
            capture(envelope, attemptCount, dispatchDurationMs);
            return Task.CompletedTask;
        }
    }
}
