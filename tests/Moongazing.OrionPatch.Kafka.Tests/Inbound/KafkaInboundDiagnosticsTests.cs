namespace Moongazing.OrionPatch.Kafka.Tests.Inbound;

using System.Diagnostics.Metrics;
using Moongazing.OrionPatch.Kafka.Inbound;
using Xunit;

public sealed class KafkaInboundDiagnosticsTests
{
    private static (System.Collections.Generic.List<(string topic, long val)> samples, MeterListener listener)
        Subscribe(string instrumentName)
    {
        var samples = new System.Collections.Generic.List<(string, long)>();
        var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == KafkaInboundDiagnostics.MeterName
                && instrument.Name == instrumentName)
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, val, tags, _) =>
        {
            string topic = string.Empty;
            foreach (var t in tags)
            {
                if (t.Key == "topic" && t.Value is string s) { topic = s; }
            }
            lock (samples) { samples.Add((topic, val)); }
        });
        listener.Start();
        return (samples, listener);
    }

    [Fact]
    public void RecordAttemptSet_emits_a_measurement_tagged_with_the_topic()
    {
        var (samples, listener) = Subscribe("orionpatch.kafka.inbound.attempt_set");
        using var _ = listener;

        KafkaInboundDiagnostics.RecordAttemptSet("orders");

        lock (samples)
        {
            Assert.Contains(samples, s => s.topic == "orders" && s.val == 1);
        }
    }

    [Fact]
    public void RecordDlqRouted_emits_a_measurement_tagged_with_topic_and_dlq()
    {
        var samples = new System.Collections.Generic.List<(string topic, string dlq, long val)>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == KafkaInboundDiagnostics.MeterName
                && instrument.Name == "orionpatch.kafka.inbound.dlq_routed")
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, val, tags, _) =>
        {
            string topic = string.Empty, dlq = string.Empty;
            foreach (var t in tags)
            {
                if (t.Key == "topic" && t.Value is string s) { topic = s; }
                if (t.Key == "dlq" && t.Value is string d) { dlq = d; }
            }
            lock (samples) { samples.Add((topic, dlq, val)); }
        });
        listener.Start();

        KafkaInboundDiagnostics.RecordDlqRouted("orders", "orders.dlq");

        lock (samples)
        {
            Assert.Contains(samples, s => s.topic == "orders" && s.dlq == "orders.dlq" && s.val == 1);
        }
    }
}
