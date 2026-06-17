namespace Moongazing.OrionPatch.Benchmarks;

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Moongazing.OrionPatch.Configuration;

/// <summary>
/// Measures the two halves of the message-type-name registry: building the immutable registry
/// (which snapshots into <see cref="System.Collections.Frozen.FrozenDictionary{TKey,TValue}"/>,
/// a one-time startup cost) and resolving names on the enqueue/dispatch hot path
/// (<see cref="MessageTypeRegistry.ResolveLogicalName"/> /
/// <see cref="MessageTypeRegistry.ResolveClrType"/>, called once per message).
/// </summary>
[MemoryDiagnoser]
[SimpleJob(RuntimeMoniker.Net80)]
[SimpleJob(RuntimeMoniker.Net90)]
public class MessageTypeRegistryBenchmarks
{
    private sealed class OrderShipped { }
    private sealed class OrderCancelled { }
    private sealed class PaymentCaptured { }

    private readonly MessageTypeRegistry registry = new MessageTypeRegistryBuilder()
        .Map<OrderShipped>("OrderShipped")
        .Map<OrderCancelled>("OrderCancelled")
        .Map<PaymentCaptured>("PaymentCaptured")
        .Build();

    [Benchmark]
    public MessageTypeRegistry BuildRegistry() =>
        new MessageTypeRegistryBuilder()
            .Map<OrderShipped>("OrderShipped")
            .Map<OrderCancelled>("OrderCancelled")
            .Map<PaymentCaptured>("PaymentCaptured")
            .Build();

    [Benchmark]
    public string? ResolveLogicalName() => registry.ResolveLogicalName(typeof(OrderShipped));

    [Benchmark]
    public Type? ResolveClrType() => registry.ResolveClrType("PaymentCaptured");
}
