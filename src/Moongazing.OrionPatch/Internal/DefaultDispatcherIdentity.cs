namespace Moongazing.OrionPatch.Internal;

/// <summary>
/// Default dispatcher identity factory: <c>{MachineName}/{ProcessId}</c>.
/// Used as the seed for <see cref="Configuration.OrionPatchOptions.DispatcherIdentityFactory"/>.
/// </summary>
internal static class DefaultDispatcherIdentity
{
    /// <summary>Build the default identity string for this process.</summary>
    /// <returns>A string of the form <c>{MachineName}/{ProcessId}</c>.</returns>
    public static string Create() => $"{Environment.MachineName}/{Environment.ProcessId}";
}
