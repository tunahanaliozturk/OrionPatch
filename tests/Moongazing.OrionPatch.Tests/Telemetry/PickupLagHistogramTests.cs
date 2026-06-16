namespace Moongazing.OrionPatch.Tests.Telemetry;

using System.Diagnostics.Metrics;
using Moongazing.OrionPatch.Telemetry;
using Xunit;

public sealed class PickupLagHistogramTests
{
    private static MeterListener StartListener(System.Collections.Generic.List<double> samples)
    {
        var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == OrionPatchDiagnostics.SourceName
                && instrument.Name == "orionpatch.outbox.dispatch.pickup_lag_ms")
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<double>((_, val, _, _) =>
        {
            lock (samples) { samples.Add(val); }
        });
        listener.Start();
        return listener;
    }

    [Fact]
    public void RecordPickupLag_emits_for_positive_milliseconds()
    {
        var samples = new System.Collections.Generic.List<double>();
        using var listener = StartListener(samples);

        OrionPatchDiagnostics.RecordPickupLag(640.0);

        lock (samples) { Assert.Contains(640.0, samples); }
    }

    [Fact]
    public void RecordPickupLag_clamps_negative_to_zero()
    {
        var samples = new System.Collections.Generic.List<double>();
        using var listener = StartListener(samples);

        OrionPatchDiagnostics.RecordPickupLag(-50.0);

        lock (samples) { Assert.Contains(0.0, samples); }
    }
}
