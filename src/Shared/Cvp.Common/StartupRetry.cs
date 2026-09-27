using Microsoft.Extensions.Logging;

namespace Cvp.Common;

/// <summary>
/// Retries a startup dependency check (schema bootstrap, connectivity probe)
/// with backoff instead of crashing the process — needed because Compose
/// starts containers concurrently, so a dependency (e.g. Postgres) may still
/// be starting up when this service's first request to it fires.
/// </summary>
public static class StartupRetry
{
    public static async Task ExecuteAsync(
        Func<Task> action,
        ILogger logger,
        int maxAttempts = 10,
        TimeSpan? delay = null)
    {
        var interval = delay ?? TimeSpan.FromSeconds(2);

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                await action();
                return;
            }
            catch (Exception ex) when (attempt < maxAttempts)
            {
                logger.LogWarning(
                    ex,
                    "Startup dependency not ready (attempt {Attempt}/{MaxAttempts}), retrying in {Delay}",
                    attempt, maxAttempts, interval);
                await Task.Delay(interval);
            }
        }
    }
}
