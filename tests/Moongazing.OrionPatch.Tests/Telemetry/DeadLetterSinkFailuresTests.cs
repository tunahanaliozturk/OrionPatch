namespace Moongazing.OrionPatch.Tests.Telemetry;

using System.Diagnostics.Metrics;
using Moongazing.OrionPatch.Telemetry;
using Xunit;

public sealed class DeadLetterSinkFailuresTests
{
    [Fact]
    public void RecordDeadLetterSinkFailure_emits_a_measurement_tagged_with_exception_type()
    {
        var samples = new System.Collections.Generic.List<(string exceptionType, long val)>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == OrionPatchDiagnostics.SourceName
                && instrument.Name == "orionpatch.outbox.dead_letter_sink_failures")
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, val, tags, _) =>
        {
            string type = string.Empty;
            foreach (var t in tags)
            {
                if (t.Key == "exception_type" && t.Value is string s) { type = s; }
            }
            lock (samples) { samples.Add((type, val)); }
        });
        listener.Start();

        OrionPatchDiagnostics.RecordDeadLetterSinkFailure("HttpRequestException");

        lock (samples)
        {
            Assert.Contains(samples, s => s.exceptionType == "HttpRequestException" && s.val == 1);
        }
    }
}
