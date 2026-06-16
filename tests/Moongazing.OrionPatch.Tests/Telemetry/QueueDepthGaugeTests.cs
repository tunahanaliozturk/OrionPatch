namespace Moongazing.OrionPatch.Tests.Telemetry;

using System.Diagnostics.Metrics;
using Moongazing.OrionPatch.Telemetry;
using Xunit;

[Collection("DispatcherQueueDepth")]
public sealed class QueueDepthGaugeTests
{
    [Fact]
    public void Gauge_reports_the_value_set_via_SetQueueDepth()
    {
        long observed = -1;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == OrionPatchDiagnostics.SourceName
                && instrument.Name == "orionpatch.outbox.queue_depth")
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, val, _, _) =>
        {
            System.Threading.Interlocked.Exchange(ref observed, val);
        });
        // Force static init so the gauge instrument exists before the listener starts
        // (v0.7.23 Audit coderabbit lesson).
        System.Runtime.CompilerServices.RuntimeHelpers.RunClassConstructor(
            typeof(OrionPatchDiagnostics).TypeHandle);
        listener.Start();

        OrionPatchDiagnostics.SetQueueDepth(77L);
        listener.RecordObservableInstruments();

        Assert.Equal(77L, System.Threading.Interlocked.Read(ref observed));
    }
}
