namespace Moongazing.OrionPatch.Tests.Telemetry;

using System.Diagnostics.Metrics;
using Moongazing.OrionPatch.Telemetry;
using Xunit;

public sealed class IdlePollCounterTests
{
    [Fact]
    public void RecordIdlePoll_increments_the_counter()
    {
        var total = 0L;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == OrionPatchDiagnostics.SourceName
                && instrument.Name == "orionpatch.outbox.poll.idle")
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, val, _, _) =>
        {
            System.Threading.Interlocked.Add(ref total, val);
        });
        listener.Start();

        OrionPatchDiagnostics.RecordIdlePoll();
        OrionPatchDiagnostics.RecordIdlePoll();

        Assert.Equal(2L, System.Threading.Interlocked.Read(ref total));
    }
}
