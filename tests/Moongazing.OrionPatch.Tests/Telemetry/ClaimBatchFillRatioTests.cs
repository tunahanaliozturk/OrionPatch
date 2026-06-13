namespace Moongazing.OrionPatch.Tests.Telemetry;

using System.Diagnostics.Metrics;
using Moongazing.OrionPatch.Telemetry;
using Xunit;

public sealed class ClaimBatchFillRatioTests
{
    private static System.Collections.Generic.List<double> Capture(System.Action act)
    {
        var samples = new System.Collections.Generic.List<double>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == OrionPatchDiagnostics.SourceName
                && instrument.Name == "orionpatch.outbox.claim.batch_fill_ratio")
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
    public void Half_full_batch_records_0_5()
    {
        var s = Capture(() => OrionPatchDiagnostics.RecordClaimBatchFillRatio(claimed: 50, batchSize: 100));
        Assert.Contains(0.5, s);
    }

    [Fact]
    public void Full_batch_records_1()
    {
        var s = Capture(() => OrionPatchDiagnostics.RecordClaimBatchFillRatio(claimed: 100, batchSize: 100));
        Assert.Contains(1.0, s);
    }

    [Fact]
    public void Over_full_claimed_is_clamped_to_1()
    {
        // Defensive: a storage backend that over-claims must not emit > 1.
        var s = Capture(() => OrionPatchDiagnostics.RecordClaimBatchFillRatio(claimed: 150, batchSize: 100));
        Assert.Contains(1.0, s);
        Assert.DoesNotContain(s, v => v > 1.0);
    }

    [Fact]
    public void Zero_claimed_or_zero_batch_does_not_emit()
    {
        var s = Capture(() =>
        {
            OrionPatchDiagnostics.RecordClaimBatchFillRatio(claimed: 0, batchSize: 100);
            OrionPatchDiagnostics.RecordClaimBatchFillRatio(claimed: 10, batchSize: 0);
        });
        Assert.Empty(s);
    }
}
