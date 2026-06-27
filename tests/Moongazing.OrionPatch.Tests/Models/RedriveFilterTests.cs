namespace Moongazing.OrionPatch.Tests.Models;

using Moongazing.OrionPatch.Models;
using Xunit;

public sealed class RedriveFilterTests
{
    private static readonly DateTime Now = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private static DeadLetteredMessage Message(string type, DateTime deadLetteredAt) => new()
    {
        Id = Guid.NewGuid(),
        MessageType = type,
        Payload = "{}",
        FinalError = "e",
        OccurredAtUtc = Now,
        EnqueuedAtUtc = Now,
        DeadLetteredAtUtc = deadLetteredAt,
        AttemptCount = 5,
    };

    [Fact]
    public void All_matches_every_message()
    {
        Assert.True(RedriveFilter.All.Matches(Message("anything", Now)));
    }

    [Fact]
    public void MessageType_is_ordinal_exact()
    {
        var filter = new RedriveFilter(MessageType: "Order");
        Assert.True(filter.Matches(Message("Order", Now)));
        Assert.False(filter.Matches(Message("order", Now)));
        Assert.False(filter.Matches(Message("OrderShipped", Now)));
    }

    [Fact]
    public void Window_is_half_open_from_inclusive_to_exclusive()
    {
        var filter = new RedriveFilter(
            DeadLetteredAtOrAfterUtc: Now,
            DeadLetteredBeforeUtc: Now.AddHours(1));

        Assert.True(filter.Matches(Message("T", Now)));                 // lower bound inclusive
        Assert.True(filter.Matches(Message("T", Now.AddMinutes(30))));  // inside
        Assert.False(filter.Matches(Message("T", Now.AddHours(1))));    // upper bound exclusive
        Assert.False(filter.Matches(Message("T", Now.AddMinutes(-1)))); // before lower bound
    }

    [Fact]
    public void Facets_combine_with_and()
    {
        var filter = new RedriveFilter(MessageType: "T", DeadLetteredAtOrAfterUtc: Now);
        Assert.True(filter.Matches(Message("T", Now)));
        Assert.False(filter.Matches(Message("T", Now.AddMinutes(-1)))); // type matches, window does not
        Assert.False(filter.Matches(Message("X", Now)));                // window matches, type does not
    }
}
