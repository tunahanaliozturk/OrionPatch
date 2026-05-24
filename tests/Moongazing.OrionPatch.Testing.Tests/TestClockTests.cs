namespace Moongazing.OrionPatch.Testing.Tests;

using Xunit;

public class TestClockTests
{
    [Fact]
    public void UtcNow_ShouldReturnInitialValue_WhenConstructed()
    {
        var initial = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var clock = new TestClock(initial);

        Assert.Equal(initial, clock.UtcNow);
    }

    [Fact]
    public void Advance_ShouldShiftClockForward_ByTheGivenDuration()
    {
        var initial = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var clock = new TestClock(initial);

        clock.Advance(TimeSpan.FromHours(2));

        Assert.Equal(initial.AddHours(2), clock.UtcNow);
    }

    [Fact]
    public void Set_ShouldOverrideClock_WhenCalled()
    {
        var clock = new TestClock(new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        var newValue = new DateTime(2030, 6, 15, 12, 0, 0, DateTimeKind.Utc);

        clock.Set(newValue);

        Assert.Equal(newValue, clock.UtcNow);
    }

    [Fact]
    public async Task DelayAsync_ShouldCompleteImmediately_WhenZero()
    {
        var clock = new TestClock();

        var task = clock.DelayAsync(TimeSpan.Zero);

        Assert.True(task.IsCompletedSuccessfully);
        await task;
    }

    [Fact]
    public void Advance_ShouldThrow_WhenDurationIsNegative()
    {
        var clock = new TestClock();
        Assert.Throws<ArgumentOutOfRangeException>(() => clock.Advance(TimeSpan.FromSeconds(-1)));
    }
}
