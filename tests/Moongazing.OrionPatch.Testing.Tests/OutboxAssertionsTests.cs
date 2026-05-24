namespace Moongazing.OrionPatch.Testing.Tests;

using System.Text.Json;
using Moongazing.OrionPatch.Models;
using Xunit;

public class OutboxAssertionsTests
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private sealed record TestMessage(string Greeting);

    [Fact]
    public async Task AssertDispatched_ShouldReturnEnvelope_WhenTypeMatchesAndPredicatePasses()
    {
        var sink = new CapturingOutboxSink();
        var typeName = typeof(TestMessage).FullName!;
        var payload = JsonSerializer.Serialize(new TestMessage("hello"), JsonOptions);
        var envelope = new OutboxEnvelope(Guid.NewGuid(), typeName, payload, null, null, DateTime.UtcNow, 1);
        await sink.SendAsync(envelope);

        var matched = sink.AssertDispatched<TestMessage>(m => m.Greeting == "hello");

        Assert.Equal(envelope.Id, matched.Id);
    }

    [Fact]
    public async Task AssertDispatched_ShouldThrow_WhenNoMatch()
    {
        var sink = new CapturingOutboxSink();
        await sink.SendAsync(new OutboxEnvelope(Guid.NewGuid(), "Other", "{}", null, null, DateTime.UtcNow, 1));

        Assert.Throws<InvalidOperationException>(() => sink.AssertDispatched<TestMessage>());
    }

    [Fact]
    public async Task AssertDeadLettered_ShouldReturnRow_WhenStorageHasDeadLetteredRow()
    {
        var storage = new InMemoryOutboxStorage();
        var row = new OutboxRow
        {
            Id = Guid.NewGuid(),
            MessageType = "T",
            Payload = "{}",
            OccurredAtUtc = DateTime.UtcNow,
            EnqueuedAtUtc = DateTime.UtcNow,
            Status = OutboxStatus.Pending,
        };
        await storage.AppendAsync(new[] { row });
        await storage.ClaimNextAsync(10, "d", TimeSpan.FromMinutes(1));
        await storage.DeadLetterAsync(row.Id, "boom");

        var matched = storage.AssertDeadLettered();

        Assert.Equal(row.Id, matched.Id);
    }

    [Fact]
    public void AssertDeadLettered_ShouldThrow_WhenNoMatch()
    {
        var storage = new InMemoryOutboxStorage();
        Assert.Throws<InvalidOperationException>(() => storage.AssertDeadLettered());
    }
}
