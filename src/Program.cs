using Microsoft.Extensions.Logging;

namespace KVDb;

public static class Program
{
    public static async Task Main()
    {
        using ILoggerFactory loggerFactory = LoggerFactory.Create(builder => 
            builder.AddSimpleConsole(options => {
                options.IncludeScopes = false;
                options.SingleLine = true;
                options.TimestampFormat = "HH:mm:ss ";
            }));
        ILogger<Db> logger = loggerFactory.CreateLogger<Db>();
        var db = new Db(logger);

        // Insert operation
        await db.Insert("key1", "value1");
        await db.Insert("key2", "value2");
        await db.Insert("key3", "value3");

        // foreach (var entry in db._memTable)
        // {
        //     Console.WriteLine($"key = {entry.Key}, value = {entry.Value}");
        // }

        // Get operation
        var value1 = await db.GetAsync("key1");
        Console.WriteLine($"Value for key1: {value1}");
        
        // Delete operation
        await db.DeleteAsync("key2");
        
        // Attempt to get deleted key
        var value2 = await db.GetAsync("key2");
        Console.WriteLine($"Value for key2 (after deletion): {value2}");
        
        // Insert more keys to trigger MemTable flush and merge
        for (int i = 4; i <= 100; i++)
        {
            await db.Insert($"key{i}", $"value{i}");
        }
        
        // Retrieve a key after segment merge
        var value50 = await db.GetAsync("key50");
        Console.WriteLine($"Value for key50: {value50}");
    }
}
