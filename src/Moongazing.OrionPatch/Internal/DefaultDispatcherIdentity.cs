namespace Moongazing.OrionPatch.Internal;

/// <summary>
/// Default dispatcher identity factory: <c>{MachineName}/{ProcessId}</c>.
/// Used as the seed for <see cref="Configuration.OrionPatchOptions.DispatcherIdentityFactory"/>.
/// </summary>
internal static class DefaultDispatcherIdentity
{
    /// <summary>Build the default identity string for this process.</summary>
    /// <returns>A string of the form <c>{MachineName}/{ProcessId}</c>.</returns>
    public static string Create()
    {
        string machine;
        try
        {
            machine = Environment.MachineName;
        }
        catch (InvalidOperationException)
        {
            // Hardened sandboxes (some serverless / locked-down container images)
            // refuse Environment.MachineName. Fall back to a stable short id so
            // OrionPatch still starts. Users who need real identity can override
            // via OrionPatchOptions.DispatcherIdentityFactory.
            machine = $"host-{Guid.NewGuid():N}"[..13];
        }

        return $"{machine}/{Environment.ProcessId}";
    }
}
