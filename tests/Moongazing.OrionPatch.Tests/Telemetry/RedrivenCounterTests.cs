namespace Moongazing.OrionPatch.Tests.Telemetry;

using System.Diagnostics.Metrics;
using Moongazing.OrionPatch.Telemetry;
using Xunit;

public sealed class RedrivenCounterTests
{
    private static System.Collections.Generic.List<long> Capture(System.Action act)
    {
        var samples = new System.Collections.Generic.List<long>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == OrionPatchDiagnostics.SourceName
                && instrument.Name == "orionpatch.outbox.dead_letter.redriven")
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, val, _, _) =>
        {
            lock (samples) { samples.Add(val); }
        });
        listener.Start();

        act();

        lock (samples) { return new System.Collections.Generic.List<long>(samples); }
    }

    [Fact]
    public void RecordRedriven_emits_for_a_positive_count()
    {
        var samples = Capture(() => OrionPatchDiagnostics.RecordRedriven(3));
        Assert.Contains(3L, samples);
    }

    [Fact]
    public void RecordRedriven_ignores_non_positive_counts()
    {
        var samples = Capture(() =>
        {
            OrionPatchDiagnostics.RecordRedriven(0);
            OrionPatchDiagnostics.RecordRedriven(-5);
        });
        Assert.Empty(samples);
    }
}
