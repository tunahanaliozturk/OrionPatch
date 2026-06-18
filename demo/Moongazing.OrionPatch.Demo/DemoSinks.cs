using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Models;

namespace Moongazing.OrionPatch.Demo;

/// <summary>
/// A sink that throws on its first <paramref name="failuresBeforeSuccess"/> attempts for any
/// envelope id, then succeeds. Models a flaky downstream (broker timeout, transient 503) so the
/// dispatcher's retry + backoff path can be observed end to end.
/// </summary>
public sealed class FlakyOutboxSink(int failuresBeforeSuccess) : IOutboxSink
{
    private readonly Dictionary<Guid, int> attemptsSeen = new();

    public Task SendAsync(OutboxEnvelope envelope, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        attemptsSeen.TryGetValue(envelope.Id, out var seen);
        seen++;
        attemptsSeen[envelope.Id] = seen;

        if (seen <= failuresBeforeSuccess)
        {
            Console.WriteLine(
                $"    sink: attempt {envelope.AttemptNumber} for {envelope.MessageType} FAILED (simulated transient fault)");
            throw new InvalidOperationException("simulated transient downstream failure");
        }

        Console.WriteLine(
            $"    sink: attempt {envelope.AttemptNumber} for {envelope.MessageType} SUCCEEDED");
        return Task.CompletedTask;
    }
}

/// <summary>
/// A sink that always throws. Used to demonstrate the dead-letter terminal state once the
/// dispatcher exhausts <c>OrionPatchOptions.MaxAttempts</c>.
/// </summary>
public sealed class AlwaysFailingOutboxSink : IOutboxSink
{
    public Task SendAsync(OutboxEnvelope envelope, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        Console.WriteLine(
            $"    sink: attempt {envelope.AttemptNumber} for {envelope.MessageType} FAILED (permanent fault)");
        throw new InvalidOperationException("permanent downstream failure");
    }
}
