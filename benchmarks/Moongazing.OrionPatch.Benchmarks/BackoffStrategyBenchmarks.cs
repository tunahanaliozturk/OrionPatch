namespace Moongazing.OrionPatch.Benchmarks;

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Moongazing.OrionPatch.Configuration;

/// <summary>
/// Measures the per-attempt cost of evaluating the dispatcher's retry backoff delegates.
/// <see cref="BackoffStrategy.Exponential(System.TimeSpan, System.TimeSpan)"/> runs on every
/// failed dispatch to compute NextAttemptAtUtc, so its delegate-invocation and arithmetic cost
/// (including the overflow-saturation branch) sits on the retry hot path.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(RuntimeMoniker.Net80)]
[SimpleJob(RuntimeMoniker.Net90)]
public class BackoffStrategyBenchmarks
{
    private readonly Func<int, TimeSpan> exponential =
        BackoffStrategy.Exponential(TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(30));

    private readonly Func<int, TimeSpan> fixedDelay =
        BackoffStrategy.Fixed(TimeSpan.FromSeconds(5));

    /// <summary>Attempt numbers spanning the early ramp, the saturation cap, and the overflow guard.</summary>
    [Params(1, 5, 30, 100)]
    public int Attempt { get; set; }

    [Benchmark]
    public TimeSpan ExponentialFactory() =>
        BackoffStrategy.Exponential(TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(30))(Attempt);

    [Benchmark]
    public TimeSpan ExponentialEvaluate() => exponential(Attempt);

    [Benchmark]
    public TimeSpan FixedEvaluate() => fixedDelay(Attempt);
}
