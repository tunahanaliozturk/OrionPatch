namespace Moongazing.OrionPatch.Tests;

using Xunit;

/// <summary>
/// Serializes the tests that read or write the process-global outbox queue-depth gauge.
/// <c>QueueDepthGaugeTests</c> sets <c>OrionPatchDiagnostics.SetQueueDepth</c> and asserts the
/// exact value the gauge reports back, while every test that runs the real
/// <c>OutboxDispatcherHostedService</c> calls <c>SetQueueDepth</c> each poll cycle. Because xUnit
/// parallelizes across test classes by default, those writers race the gauge read. Placing both
/// classes in one collection with parallelization disabled makes them run in isolation, so the
/// gauge observation is deterministic. The new pickup-lag behavioral tests live in the same
/// hosted-service class and therefore inherit the isolation too.
/// </summary>
[CollectionDefinition("DispatcherQueueDepth", DisableParallelization = true)]
#pragma warning disable CA1711 // xUnit collection-definition marker classes conventionally end in 'Collection'.
public sealed class DispatcherQueueDepthCollection
#pragma warning restore CA1711
{
}
