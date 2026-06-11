namespace Moongazing.OrionPatch.Kafka;

using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

/// <summary>
/// <see cref="IHealthCheck"/> that probes the configured Kafka broker by listing its
/// metadata. Returns <see cref="HealthStatus.Healthy"/> when the metadata call returns
/// within <see cref="KafkaProducerHealthCheckOptions.Timeout"/>, <see cref="HealthStatus.Unhealthy"/>
/// otherwise. Pairs with the v0.2.12+ outbound producer so the consumer's <c>/health</c>
/// probe downgrades when the broker becomes unreachable BEFORE the outbox starts piling
/// up failed produces.
/// </summary>
public sealed class KafkaProducerHealthCheck : IHealthCheck, IDisposable
{
    private readonly KafkaOutboxSinkOptions producerOptions;
    private readonly KafkaProducerHealthCheckOptions checkOptions;
    private IAdminClient? adminClient;
    private readonly object gate = new();
    private volatile bool disposed;

    public KafkaProducerHealthCheck(
        IOptions<KafkaOutboxSinkOptions> producerOptions,
        IOptions<KafkaProducerHealthCheckOptions> checkOptions)
    {
        ArgumentNullException.ThrowIfNull(producerOptions);
        ArgumentNullException.ThrowIfNull(checkOptions);
        this.producerOptions = producerOptions.Value;
        this.checkOptions = checkOptions.Value;
        this.checkOptions.Validate();
    }

    /// <inheritdoc />
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        // The native AdminClient.GetMetadata call is synchronous and ignores the
        // ASP.NET health-check cancellationToken. Honor it by offloading the call to a
        // worker thread (Task.Run) and racing the probe Task against the cancellation
        // token. If cancellation wins the metadata Task is left to complete in the
        // background; the AdminClient handles its own teardown on Dispose.
        var probe = Task.Run(() =>
        {
            var admin = GetOrCreateAdmin();
            return admin.GetMetadata(checkOptions.Timeout);
        }, cancellationToken);

        var cancellation = Task.Delay(System.Threading.Timeout.InfiniteTimeSpan, cancellationToken);
        var done = await Task.WhenAny(probe, cancellation).ConfigureAwait(false);
        if (done == cancellation)
        {
            // Caller's deadline beat the probe. Surface Unhealthy with a clear message
            // rather than throwing TaskCanceledException out of the health pipeline.
            return HealthCheckResult.Unhealthy(
                "Kafka broker metadata probe cancelled (caller deadline hit before the broker responded).",
                data: new System.Collections.Generic.Dictionary<string, object>
                {
                    ["bootstrap"] = producerOptions.BootstrapServers,
                });
        }
        try
        {
            var metadata = await probe.ConfigureAwait(false);
            var data = new System.Collections.Generic.Dictionary<string, object>
            {
                ["brokers"] = metadata.Brokers.Count,
                ["topics"] = metadata.Topics.Count,
                ["bootstrap"] = producerOptions.BootstrapServers,
            };
            return HealthCheckResult.Healthy(
                $"Kafka broker metadata returned {metadata.Brokers.Count} brokers.",
                data: data);
        }
#pragma warning disable CA1031 // health-check probe collapses ANY broker-side fault into Unhealthy
        catch (Exception ex)
#pragma warning restore CA1031
        {
            return HealthCheckResult.Unhealthy(
                $"Kafka broker metadata probe failed: {ex.GetType().Name}: {ex.Message}",
                exception: ex,
                data: new System.Collections.Generic.Dictionary<string, object>
                {
                    ["bootstrap"] = producerOptions.BootstrapServers,
                });
        }
    }

    private IAdminClient GetOrCreateAdmin()
    {
        ObjectDisposedException.ThrowIf(disposed, this);
        if (adminClient is not null)
        {
            return adminClient;
        }
        lock (gate)
        {
            ObjectDisposedException.ThrowIf(disposed, this);
            if (adminClient is not null)
            {
                return adminClient;
            }
            var config = new AdminClientConfig
            {
                BootstrapServers = producerOptions.BootstrapServers,
            };
            adminClient = new AdminClientBuilder(config).Build();
            return adminClient;
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        lock (gate)
        {
            disposed = true;
            adminClient?.Dispose();
            adminClient = null;
        }
    }
}

/// <summary>Options for <see cref="KafkaProducerHealthCheck"/>.</summary>
public sealed class KafkaProducerHealthCheckOptions
{
    /// <summary>Metadata-probe timeout. Default 3 seconds - enough for a healthy local broker, fast enough to surface a degraded one before the consumer's /health probe times out.</summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(3);

    internal void Validate()
    {
        if (Timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(Timeout), Timeout,
                "KafkaProducerHealthCheckOptions.Timeout must be positive.");
        }
    }
}
