namespace Moongazing.OrionPatch.Tests.Telemetry;

using System.Diagnostics.Metrics;
using Moongazing.OrionPatch.Telemetry;
using Xunit;

public sealed class DeadLetterAgeHistogramTests
{
    private static System.Collections.Generic.List<double> Capture(System.Action act)
    {
        var samples = new System.Collections.Generic.List<double>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == OrionPatchDiagnostics.SourceName
                && instrument.Name == "orionpatch.outbox.dead_letter.age_ms")
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<double>((_, val, _, _) =>
        {
            lock (samples) { samples.Add(val); }
        });
        listener.Start();

        act();

        lock (samples) { return new System.Collections.Generic.List<double>(samples); }
    }

    [Fact]
    public void RecordDeadLetterAge_emits_for_a_positive_age()
    {
        var samples = Capture(() => OrionPatchDiagnostics.RecordDeadLetterAge(1500.0));
        Assert.Contains(1500.0, samples);
    }

    [Fact]
    public void RecordDeadLetterAge_clamps_negative_to_zero()
    {
        var samples = Capture(() => OrionPatchDiagnostics.RecordDeadLetterAge(-42.0));
        Assert.Contains(0.0, samples);
        Assert.DoesNotContain(-42.0, samples);
    }
}
