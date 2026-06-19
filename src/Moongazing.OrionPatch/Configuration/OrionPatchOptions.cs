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
    /// v0.3.0 retention window for successfully dispatched (<c>Processed</c>) rows. A row remains
    /// in the hot outbox for this long after dispatch, then becomes eligible to be archived or
    /// purged by <see cref="Abstractions.IOutboxArchivalStore.ArchiveProcessedAsync"/>. Default
    /// 7 days. <see cref="TimeSpan.Zero"/> reaps processed rows on the next maintenance pass.
    /// Pending, Claimed, and DeadLettered rows are never affected by this window.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when set to a negative value.</exception>
    public TimeSpan ArchiveRetention
    {
        get => _archiveRetention;
        set
        {
            if (value < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(value), value, "ArchiveRetention must be non-negative.");
            }
            _archiveRetention = value;
        }
    }

    private TimeSpan _archiveRetention = TimeSpan.FromDays(7);

    private Func<int, TimeSpan> _backoffStrategy =
        Configuration.BackoffStrategy.Exponential(TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(30));

    /// <summary>
    /// Attempt-number to delay mapping. Default: exponential doubling 1s..30min.
    /// See <see cref="Configuration.BackoffStrategy"/> for built-in factories.
    /// </summary>
    /// <exception cref="ArgumentNullException">Thrown when set to <see langword="null"/>.</exception>
    public Func<int, TimeSpan> BackoffStrategy
    {
        get => _backoffStrategy;
        set
        {
            ArgumentNullException.ThrowIfNull(value);
            _backoffStrategy = value;
        }
    }

    private Func<string> _dispatcherIdentityFactory = DefaultDispatcherIdentity.Create;

    /// <summary>
    /// Produces the identity string stamped onto claimed rows. Evaluated once when the
    /// dispatcher's background loop starts; the resulting string is reused for every claim
    /// made by this dispatcher instance. Default: <c>{MachineName}/{ProcessId}</c>.
    /// </summary>
    /// <exception cref="ArgumentNullException">Thrown when set to <see langword="null"/>.</exception>
    public Func<string> DispatcherIdentityFactory
    {
        get => _dispatcherIdentityFactory;
        set
        {
            ArgumentNullException.ThrowIfNull(value);
            _dispatcherIdentityFactory = value;
        }
    }

    private JsonSerializerOptions _jsonOptions = new(JsonSerializerDefaults.Web);

    /// <summary>
    /// <see cref="JsonSerializerOptions"/> used for payload serialization at enqueue and
    /// deserialization at dispatch.
    /// </summary>
    /// <remarks>
    /// Configure this instance before the host starts. Once the dispatcher serializes its first
    /// payload, <see cref="System.Text.Json.JsonSerializerOptions"/> freezes and any further
    /// mutation throws <see cref="System.InvalidOperationException"/>. Either fully configure the
    /// instance during DI setup, or replace it wholesale with a new instance (which is permitted
    /// via the setter).
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown when set to <see langword="null"/>.</exception>
    public JsonSerializerOptions JsonOptions
    {
        get => _jsonOptions;
        set
        {
            ArgumentNullException.ThrowIfNull(value);
            _jsonOptions = value;
        }
    }
}
