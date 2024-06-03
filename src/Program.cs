namespace KVDb;

public static class Program
{
    public static async Task Main()
    {
        var db = new Db();
        await db.Insert("key1", "value1");
        var value = await db.GetAsync("key1");
        Console.WriteLine(value);
    }
}