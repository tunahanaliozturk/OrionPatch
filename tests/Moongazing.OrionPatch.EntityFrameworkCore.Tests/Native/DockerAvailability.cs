namespace Moongazing.OrionPatch.EntityFrameworkCore.Tests.Native;

using System;
using System.Diagnostics;
using System.Threading.Tasks;

/// <summary>
/// Decides, honestly, whether a container-backed fixture should skip (Docker genuinely unavailable)
/// or run. The earlier fixtures wrapped the whole of <c>InitializeAsync</c> in a single <c>try</c> and
/// turned <em>any</em> exception into a skip reason, which masked real regressions: a broken schema, a
/// bad connection string, or a provider fault would be reported as "Docker not running" and silently
/// skip the native-claim suite on CI, where Docker is in fact available.
/// </summary>
/// <remarks>
/// <para>
/// The contract here is a clean split. First a pre-flight probe asks the Docker daemon directly
/// whether it is reachable (a <c>docker version</c> server query). Only when that probe fails - Docker
/// absent or its daemon down - do we skip. This is deliberately independent of any Testcontainers
/// exception type: Testcontainers 3.x signals a missing runtime as a plain <see cref="Exception"/>
/// indistinguishable from a genuine misconfiguration, so keying the skip off the daemon probe instead
/// of off the start exception is what keeps the decision honest.
/// </para>
/// <para>
/// Once the probe says Docker is up, the container start and all post-start setup (schema creation,
/// RCSI enablement, the claim itself) must <em>fail</em> the test rather than skip it - they cannot be
/// Docker-availability problems any more, so a failure there is a real regression.
/// </para>
/// </remarks>
internal static class DockerAvailability
{
    /// <summary>
    /// Skip when Docker is unavailable; otherwise start the container and run setup, letting any
    /// failure propagate.
    /// </summary>
    /// <param name="startAsync">Starts the underlying container. Only reached when Docker is available.</param>
    /// <param name="setupAsync">Post-start setup (schema, RCSI). Failures here are real and must propagate.</param>
    /// <returns>The skip reason when Docker is unavailable; otherwise <c>null</c> (the suite must run).</returns>
    public static async Task<string?> StartOrSkipAsync(Func<Task> startAsync, Func<Task> setupAsync)
    {
        ArgumentNullException.ThrowIfNull(startAsync);
        ArgumentNullException.ThrowIfNull(setupAsync);

        if (!await IsDockerAvailableAsync().ConfigureAwait(false))
        {
            return "Docker is unavailable, skipping native-claim integration test (the Docker daemon did not respond).";
        }

        // Docker is up. From here a failure is a real regression and must surface as a test failure,
        // not a skip.
        await startAsync().ConfigureAwait(false);
        await setupAsync().ConfigureAwait(false);
        return null;
    }

    /// <summary>
    /// Probe the Docker daemon by running <c>docker version --format {{.Server.Version}}</c>. A zero
    /// exit code with a non-empty server version means the daemon is reachable. A missing <c>docker</c>
    /// executable, a non-zero exit, or a timeout all mean unavailable.
    /// </summary>
    private static async Task<bool> IsDockerAvailableAsync()
    {
        try
        {
            using var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = "docker",
                    Arguments = "version --format \"{{.Server.Version}}\"",
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true,
                },
            };

            if (!process.Start())
            {
                return false;
            }

            var stdoutTask = process.StandardOutput.ReadToEndAsync();
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            try
            {
                await process.WaitForExitAsync(timeout.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                TryKill(process);
                return false;
            }

            if (process.ExitCode != 0)
            {
                return false;
            }

            var serverVersion = (await stdoutTask.ConfigureAwait(false)).Trim();
            return serverVersion.Length > 0;
        }
        catch (Exception ex) when (ex is System.ComponentModel.Win32Exception or InvalidOperationException)
        {
            // docker executable not on PATH, or it could not be launched.
            return false;
        }
    }

    private static void TryKill(Process process)
    {
        try
        {
            process.Kill(entireProcessTree: true);
        }
        catch (Exception ex) when (ex is InvalidOperationException or System.ComponentModel.Win32Exception or NotSupportedException)
        {
            // Process already exited or cannot be killed; nothing more to do.
        }
    }
}
