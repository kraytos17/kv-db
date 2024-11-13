namespace KVDb;

public sealed class MemTable(int maxSize)
{
    private readonly SortedDictionary<string, string> _entries = new();

    public void Clear() => _entries.Clear();

    public bool CapacityReached() => _entries.Count >= maxSize;

    public bool ContainsKey(string key) => _entries.ContainsKey(key);

    public string this[string key]
    {
        get => _entries[key];
        set => _entries[key] = value;
    }

    public IEnumerator<SegmentEntry> GetEnumerator()
    {
        return _entries.Select(kvp => new SegmentEntry(kvp.Key, kvp.Value)).GetEnumerator();
    }
}