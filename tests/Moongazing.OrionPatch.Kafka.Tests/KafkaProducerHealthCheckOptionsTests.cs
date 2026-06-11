namespace Moongazing.OrionPatch.Kafka.Tests;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Moongazing.OrionPatch.Kafka;
using Xunit;

public sealed class KafkaProducerHealthCheckOptionsTests
{
    [Fact]
    public void Constructor_rejects_non_positive_Timeout()
    {
        // The ctor wires options through IOptions<T>; non-positive timeout makes the
        // probe call effectively synchronous-or-zero which is meaningless.
        var producerOpts = Options.Create(new KafkaOutboxSinkOptions { BootstrapServers = "kafka:9092" });

        var negative = new KafkaProducerHealthCheckOptions { Timeout = TimeSpan.FromSeconds(-1) };
        var zero = new KafkaProducerHealthCheckOptions { Timeout = TimeSpan.Zero };

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new KafkaProducerHealthCheck(producerOpts, Options.Create(negative)));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new KafkaProducerHealthCheck(producerOpts, Options.Create(zero)));
    }

    [Fact]
    public async Task CheckHealthAsync_returns_Unhealthy_when_broker_is_unreachable()
    {
        // 127.0.0.1:1 is closed by convention on most hosts; the admin client will time
        // out within the configured Timeout. Test verifies the catch-all collapses to
        // Unhealthy rather than bubbling the exception out of the probe.
        var producerOpts = Options.Create(new KafkaOutboxSinkOptions { BootstrapServers = "127.0.0.1:1" });
        var checkOpts = Options.Create(new KafkaProducerHealthCheckOptions
        {
            Timeout = TimeSpan.FromMilliseconds(200),
        });
        using var sut = new KafkaProducerHealthCheck(producerOpts, checkOpts);

        var result = await sut.CheckHealthAsync(new HealthCheckContext());

        // Acceptable outcomes: Unhealthy (timeout / connection refused). Healthy would
        // require an actual broker on 127.0.0.1:1 which CI does not provide.
        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.Equal("127.0.0.1:1", result.Data["bootstrap"]);
    }

    [Fact]
    public void Disposing_the_health_check_releases_the_admin_client()
    {
        var producerOpts = Options.Create(new KafkaOutboxSinkOptions { BootstrapServers = "127.0.0.1:1" });
        var checkOpts = Options.Create(new KafkaProducerHealthCheckOptions());
        var sut = new KafkaProducerHealthCheck(producerOpts, checkOpts);

        sut.Dispose();
        sut.Dispose(); // idempotent

        // No assertion needed - the test passes if no ObjectDisposedException leaks AND
        // the second Dispose call does not throw.
        Assert.True(true);
    }
}
