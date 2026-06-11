namespace Moongazing.OrionPatch.Tests.Telemetry;

using System.Diagnostics.Metrics;
using Moongazing.OrionPatch.Telemetry;
using Xunit;

public sealed class AttemptsPerRowHistogramTests
{
    [Fact]
    public void RecordAttemptsPerRow_emits_for_positive_count()
    {
        var samples = new System.Collections.Generic.List<int>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == OrionPatchDiagnostics.SourceName
                && instrument.Name == "orionpatch.outbox.attempts_per_row")
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<int>((_, val, _, _) =>
        {
            lock (samples) { samples.Add(val); }
        });
        listener.Start();

        OrionPatchDiagnostics.RecordAttemptsPerRow(4);

        lock (samples) { Assert.Contains(4, samples); }
    }

    [Fact]
    public void RecordAttemptsPerRow_ignores_zero_and_negative()
    {
        var samples = new System.Collections.Generic.List<int>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == OrionPatchDiagnostics.SourceName
                && instrument.Name == "orionpatch.outbox.attempts_per_row")
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<int>((_, val, _, _) =>
        {
            lock (samples) { samples.Add(val); }
        });
        listener.Start();

        OrionPatchDiagnostics.RecordAttemptsPerRow(0);
        OrionPatchDiagnostics.RecordAttemptsPerRow(-2);

        lock (samples)
        {
            Assert.DoesNotContain(0, samples);
            Assert.DoesNotContain(-2, samples);
        }
    }
}
