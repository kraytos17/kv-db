namespace KVDb;

public sealed class SegmentEntry(string key, string value)
{
    public string Key { get; } = key;
    public string Value { get; } = value;

    public Dictionary<string, string> ToDictionary() => new() { { Key, Value } };
}