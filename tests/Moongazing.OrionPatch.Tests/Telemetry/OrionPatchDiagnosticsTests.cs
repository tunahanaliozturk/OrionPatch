namespace Moongazing.OrionPatch.Tests.Telemetry;

using Moongazing.OrionPatch.Telemetry;
using Xunit;

public class OrionPatchDiagnosticsTests
{
    [Fact]
    public void SourceName_ShouldBeTheFamilyConvention_WhenAccessed()
    {
        Assert.Equal("Moongazing.OrionPatch", OrionPatchDiagnostics.SourceName);
    }

    [Fact]
    public void InstrumentNames_ShouldMatchSpec_WhenAccessed()
    {
        Assert.Equal("orionpatch.outbox.enqueued", OrionPatchDiagnostics.Enqueued.Name);
        Assert.Equal("orionpatch.outbox.dispatched", OrionPatchDiagnostics.Dispatched.Name);
        Assert.Equal("orionpatch.outbox.failed", OrionPatchDiagnostics.Failed.Name);
        Assert.Equal("orionpatch.outbox.deadlettered", OrionPatchDiagnostics.DeadLettered.Name);
        Assert.Equal("orionpatch.outbox.attempts", OrionPatchDiagnostics.Attempts.Name);
        Assert.Equal("orionpatch.outbox.dispatch.duration", OrionPatchDiagnostics.DispatchDuration.Name);
    }
}
