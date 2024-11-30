namespace KVDb;

public sealed record LogEntry(string Key, string Value, string Operation, DateTime Timestamp);
