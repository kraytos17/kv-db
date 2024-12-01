using Microsoft.Extensions.Logging;

namespace KVDb;

public sealed class Program {
    public static async Task Main() {
        using var loggerFactory = LoggerFactory.Create(builder => {
            builder.AddSimpleConsole(options => {
                options.IncludeScopes = false;
                options.SingleLine = true;
                options.TimestampFormat = "HH:mm:ss ";
            });
        });
        
        var logger = loggerFactory.CreateLogger<Program>();
        var basePath = Path.Combine(Directory.GetCurrentDirectory(), "data");
        using var db = new Db(basePath);

        try {
            logger.LogInformation("Starting database operations...");

            var bloomFilter = new BloomFilter(expectedItems: 10000, falsePositiveRate: 0.01);
            bloomFilter.Add("key1");
            bloomFilter.Add("key2");
            bloomFilter.Add("key3");

            Directory.CreateDirectory(basePath);

            var bloomFilterPath = Path.Combine(basePath, "bloom_filter.txt");
            bloomFilter.SaveToDisk(bloomFilterPath);
            logger.LogInformation("Bloom Filter saved to {filePath}", bloomFilterPath);

            await DbBenchmarkTests.RunBenchmarkTestsAsync(db, logger);
            logger.LogInformation("Database operations completed successfully.");
        }
        catch (Exception ex) {
            logger.LogError(ex, "An error occurred during database operations.");
        }
    }
}