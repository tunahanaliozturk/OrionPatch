namespace Moongazing.OrionPatch.Kafka.Inbound;

using System.Reflection;

/// <summary>
/// Resolves the OpenTelemetry <see cref="System.Diagnostics.Metrics.Meter"/> version for this
/// assembly from its own <see cref="AssemblyInformationalVersionAttribute"/>, so the metric
/// version tracks the package version automatically instead of drifting against a hardcoded
/// string. Reads only THIS assembly's version; it never reaches into another assembly.
/// </summary>
internal static class MeterVersion
{
    /// <summary>The version string to pass as the Meter version argument.</summary>
    public static string Value { get; } = Resolve();

    private static string Resolve()
    {
        var asm = typeof(MeterVersion).Assembly;
        var info = asm.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
        if (!string.IsNullOrEmpty(info))
        {
            var plus = info.IndexOf('+');
            return plus >= 0 ? info[..plus] : info;
        }
        return asm.GetName().Version?.ToString(3) ?? "0.0.0";
    }
}
