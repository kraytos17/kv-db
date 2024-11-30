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

            await DbBenchmarkTests.RunBenchmarkTestsAsync(db, logger);
            logger.LogInformation("Database operations completed successfully.");
        }
        catch (Exception ex) {
            logger.LogError(ex, "An error occurred during database operations.");
        }
    }
}