namespace Moongazing.OrionPatch.Hosting;

using Microsoft.Extensions.Hosting;

/// <summary>
/// Stand-in <see cref="IHostedService"/> registered when
/// <see cref="Configuration.OrionPatchOptions.DispatcherEnabled"/> is false. Lets writer-only
/// replicas use <c>AddOrionPatch</c> without spinning the dispatcher background loop.
/// </summary>
internal sealed class NoOpHostedService : IHostedService
{
    /// <summary>No-op start; returns a completed task.</summary>
    /// <param name="cancellationToken">Cancellation token (ignored).</param>
    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>No-op stop; returns a completed task.</summary>
    /// <param name="cancellationToken">Cancellation token (ignored).</param>
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
