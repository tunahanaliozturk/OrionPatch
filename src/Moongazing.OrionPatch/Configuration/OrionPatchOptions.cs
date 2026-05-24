namespace Moongazing.OrionPatch.Configuration;

using System.Text.Json;
using Moongazing.OrionPatch.Internal;

/// <summary>
/// All tunable knobs for OrionPatch. Bind from configuration via the standard
/// <see cref="Microsoft.Extensions.Options.IOptions{TOptions}"/> pipeline.
/// </summary>
public sealed class OrionPatchOptions
{
    /// <summary>How often the dispatcher polls storage when no rows are claimed.</summary>
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Maximum rows claimed per polling iteration.</summary>
    public int BatchSize { get; set; } = 50;

    /// <summary>Maximum dispatch attempts before the row is dead-lettered.</summary>
    public int MaxAttempts { get; set; } = 5;

    /// <summary>How long a claim is held before another dispatcher may steal it.</summary>
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromMinutes(2);

    /// <summary>When false, the background dispatcher is not started (writer-only replicas).</summary>
    public bool DispatcherEnabled { get; set; } = true;

    /// <summary>
    /// Attempt-number to delay mapping. Default: exponential doubling 1s..30min.
    /// See <see cref="Configuration.BackoffStrategy"/> for built-in factories.
    /// </summary>
    public Func<int, TimeSpan> BackoffStrategy { get; set; } =
        Configuration.BackoffStrategy.Exponential(TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(30));

    /// <summary>
    /// Returns the identity string stamped onto claimed rows. Default: <c>{MachineName}/{ProcessId}</c>.
    /// </summary>
    public Func<string> DispatcherIdentityFactory { get; set; } = DefaultDispatcherIdentity.Create;

    /// <summary>
    /// <see cref="JsonSerializerOptions"/> used for payload serialization at enqueue and
    /// deserialization at dispatch.
    /// </summary>
    public JsonSerializerOptions JsonOptions { get; set; } = new(JsonSerializerDefaults.Web);
}
