namespace Moongazing.OrionPatch.Tests.Telemetry;

using System.Diagnostics.Metrics;
using Moongazing.OrionPatch.Telemetry;
using Xunit;

public sealed class EnvelopeBytesHistogramTests
{
    [Fact]
    public void RecordEnvelopeBytes_emits_for_positive_size()
    {
        var samples = new System.Collections.Generic.List<int>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == OrionPatchDiagnostics.SourceName
                && instrument.Name == "orionpatch.outbox.dispatch.envelope_bytes")
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<int>((_, val, _, _) =>
        {
            lock (samples) { samples.Add(val); }
        });
        listener.Start();

        OrionPatchDiagnostics.RecordEnvelopeBytes(2048);

        lock (samples) { Assert.Contains(2048, samples); }
    }

    [Fact]
    public void RecordEnvelopeBytes_ignores_zero_and_negative()
    {
        var samples = new System.Collections.Generic.List<int>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == OrionPatchDiagnostics.SourceName
                && instrument.Name == "orionpatch.outbox.dispatch.envelope_bytes")
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<int>((_, val, _, _) =>
        {
            lock (samples) { samples.Add(val); }
        });
        listener.Start();

        OrionPatchDiagnostics.RecordEnvelopeBytes(0);
        OrionPatchDiagnostics.RecordEnvelopeBytes(-10);

        lock (samples)
        {
            Assert.DoesNotContain(0, samples);
            Assert.DoesNotContain(-10, samples);
        }
    }
}
