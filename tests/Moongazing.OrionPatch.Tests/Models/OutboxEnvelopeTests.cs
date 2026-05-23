using Moongazing.OrionPatch.Models;
using Xunit;

namespace Moongazing.OrionPatch.Tests.Models;

public class OutboxEnvelopeTests
{
    [Fact]
    public void Constructor_ShouldExposeAllValues_WhenAllProvided()
    {
        var id = Guid.NewGuid();
        var headers = new Dictionary<string, string> { ["k"] = "v" };
        var env = new OutboxEnvelope(
            Id: id,
            MessageType: "App.OrderConfirmed",
            Payload: "{\"orderId\":1}",
            Headers: headers,
            CorrelationId: "corr-1",
            OccurredAtUtc: new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc),
            AttemptNumber: 1);

        Assert.Equal(id, env.Id);
        Assert.Equal("App.OrderConfirmed", env.MessageType);
        Assert.Equal("{\"orderId\":1}", env.Payload);
        Assert.NotNull(env.Headers);
        Assert.Equal("v", env.Headers!["k"]);
        Assert.Equal("corr-1", env.CorrelationId);
        Assert.Equal(DateTimeKind.Utc, env.OccurredAtUtc.Kind);
        Assert.Equal(1, env.AttemptNumber);
    }

    [Fact]
    public void Constructor_ShouldAllowNullHeadersAndCorrelationId_WhenOmitted()
    {
        var env = new OutboxEnvelope(
            Id: Guid.NewGuid(),
            MessageType: "T",
            Payload: "{}",
            Headers: null,
            CorrelationId: null,
            OccurredAtUtc: DateTime.UtcNow,
            AttemptNumber: 1);

        Assert.Null(env.Headers);
        Assert.Null(env.CorrelationId);
    }
}
