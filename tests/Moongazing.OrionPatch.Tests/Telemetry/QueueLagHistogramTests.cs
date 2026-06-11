namespace Moongazing.OrionPatch.Tests.Telemetry;

using System.Diagnostics.Metrics;
using Moongazing.OrionPatch.Telemetry;
using Xunit;

public sealed class QueueLagHistogramTests
{
    [Fact]
    public void RecordQueueLag_emits_for_positive_milliseconds()
    {
        var samples = new System.Collections.Generic.List<double>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == OrionPatchDiagnostics.SourceName
                && instrument.Name == "orionpatch.outbox.queue_lag")
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<double>((_, val, _, _) =>
        {
            lock (samples) { samples.Add(val); }
        });
        listener.Start();

        OrionPatchDiagnostics.RecordQueueLag(750.0);

        lock (samples) { Assert.Contains(750.0, samples); }
    }

    [Fact]
    public void RecordQueueLag_clamps_negative_to_zero()
    {
        var samples = new System.Collections.Generic.List<double>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == OrionPatchDiagnostics.SourceName
                && instrument.Name == "orionpatch.outbox.queue_lag")
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<double>((_, val, _, _) =>
        {
            lock (samples) { samples.Add(val); }
        });
        listener.Start();

        OrionPatchDiagnostics.RecordQueueLag(-100.0);

        lock (samples) { Assert.Contains(0.0, samples); }
    }
}
