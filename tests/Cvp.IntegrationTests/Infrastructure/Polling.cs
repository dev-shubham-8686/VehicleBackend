namespace Cvp.IntegrationTests.Infrastructure;

public static class Polling
{
    /// <summary>
    /// Repeatedly calls <paramref name="fetch"/> until <paramref name="isReady"/> is true or
    /// <paramref name="timeout"/> elapses. Needed wherever a test observes the effect of an
    /// async background consumer (Kafka -> DB/cache) rather than a synchronous HTTP response.
    /// </summary>
    public static async Task<T?> UntilAsync<T>(
        Func<Task<T?>> fetch,
        Func<T?, bool> isReady,
        TimeSpan timeout,
        TimeSpan? interval = null)
    {
        var pollInterval = interval ?? TimeSpan.FromMilliseconds(250);
        using var cts = new CancellationTokenSource(timeout);

        while (!cts.IsCancellationRequested)
        {
            try
            {
                var value = await fetch();
                if (isReady(value))
                {
                    return value;
                }
            }
            catch (Exception) when (!cts.IsCancellationRequested)
            {
                // Transient (e.g. a 404 before the async consumer has caught up) — keep polling.
            }

            try
            {
                await Task.Delay(pollInterval, cts.Token);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        return await fetch();
    }
}
