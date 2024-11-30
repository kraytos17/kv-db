using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace KVDb;

public sealed class ConcurrencyTest {
    public static async Task RunConcurrencyTestAsync(Db db, ILogger logger) {
        logger.LogInformation("Running Concurrency Test...");

        var tasks = new List<Task>();
        for (var i = 0; i < 10; ++i) {
            var i1 = i;
            var task = Task.Run(async () => {
                for (var j = 1000 * i1 + 1; j <= 1000 * (i1 + 1); ++j) {
                    await db.InsertAsync($"key{j}", $"value{j}");
                }
            });
            tasks.Add(task);
        }

        var stopWatch = Stopwatch.StartNew();
        await Task.WhenAll(tasks);
        
        stopWatch.Stop();
        logger.LogInformation("Completed concurrent insertions of 10000 keys in {elapsedTime} ms.", stopWatch.ElapsedMilliseconds);
    }
}