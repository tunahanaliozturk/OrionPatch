namespace Moongazing.OrionPatch.Kafka.Tests.Inbound;

using System.Diagnostics.Metrics;
using Moongazing.OrionPatch.Kafka.Inbound;
using Xunit;

public sealed class KafkaInboundProcessedTests
{
    [Fact]
    public void RecordProcessed_emits_with_topic_tag()
    {
        var samples = new System.Collections.Generic.List<(string topic, long val)>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == KafkaInboundDiagnostics.MeterName
                && instrument.Name == "orionpatch.kafka.inbound.processed")
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

        KafkaInboundDiagnostics.RecordProcessed("orders");

        lock (samples) Assert.Contains(samples, s => s.topic == "orders" && s.val == 1);
    }

    [Fact]
    public void RecordProcessingDuration_emits_with_topic_tag()
    {
        var samples = new System.Collections.Generic.List<(string topic, double val)>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == KafkaInboundDiagnostics.MeterName
                && instrument.Name == "orionpatch.kafka.inbound.processing_duration_ms")
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<double>((_, val, tags, _) =>
        {
            string topic = string.Empty;
            foreach (var t in tags)
            {
                if (t.Key == "topic" && t.Value is string s) { topic = s; }
            }
            lock (samples) { samples.Add((topic, val)); }
        });
        listener.Start();

        KafkaInboundDiagnostics.RecordProcessingDuration("orders", 42.5);

        lock (samples) Assert.Contains(samples, s => s.topic == "orders" && s.val == 42.5);
    }
}
