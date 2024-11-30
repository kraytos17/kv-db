using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace KVDb;

public class DbBenchmarkTests {
    public static async Task RunBenchmarkTestsAsync(Db db, ILogger logger) {
        await BenchmarkInsertAsync(db, logger);
        await BenchmarkGetAsync(db, logger);
        await BenchmarkDeleteAsync(db, logger);
        await ConcurrencyTestAsync(db, logger);
        await InsertMultipleKeysAndTriggerCompaction(db, logger);
    }

    private static async Task BenchmarkInsertAsync(Db db, ILogger logger) {
        logger.LogInformation("Benchmarking Insert operations...");

        var stopWatch = Stopwatch.StartNew();
        for (var i = 1; i <= 1000; ++i) {
            await db.InsertAsync($"key{i}", $"value{i}");
        }

        stopWatch.Stop();
        logger.LogInformation("Inserted 1000 keys in {elapsedTime} ms.", stopWatch.ElapsedMilliseconds);
    }

    private static async Task BenchmarkGetAsync(Db db, ILogger logger) {
        logger.LogInformation("Benchmarking Get operations...");

        var stopWatch = Stopwatch.StartNew();
        for (var i = 1; i <= 1000; ++i) {
            var value = await db.GetAsync($"key{i}");
            if (value == null) {
                logger.LogWarning("Key 'key{i}' was not found!", i);
            }
        }

        stopWatch.Stop();
        logger.LogInformation("Retrieved 1000 keys in {elapsedTime} ms.", stopWatch.ElapsedMilliseconds);
    }

    private static async Task BenchmarkDeleteAsync(Db db, ILogger logger) {
        logger.LogInformation("Benchmarking Delete operations...");

        var stopWatch = Stopwatch.StartNew();
        for (var i = 1; i <= 1000; ++i) {
            await db.DeleteAsync($"key{i}");
        }

        stopWatch.Stop();
        logger.LogInformation("Deleted 1000 keys in {elapsedTime} ms.", stopWatch.ElapsedMilliseconds);
    }

    private static async Task ConcurrencyTestAsync(Db db, ILogger logger) {
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

    private static async Task InsertMultipleKeysAndTriggerCompaction(Db db, ILogger logger) {
        logger.LogInformation("Inserting multiple keys to trigger compaction...");

        for (var i = 1001; i <= 5000; ++i) {
            await db.InsertAsync($"key{i}", $"value{i}");
        }

        var stopWatch = Stopwatch.StartNew();
        await db.CompactAsync();
        stopWatch.Stop();
        logger.LogInformation("Compacted the database in {elapsedTime} ms.", stopWatch.ElapsedMilliseconds);
    }
}
