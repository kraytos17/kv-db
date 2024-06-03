namespace KVDb;

public class MemTable
{
    private readonly SortedDictionary<string, string> _entries;
    private readonly int _maxSize;

    public MemTable(int maxSize)
    {
        _entries = new SortedDictionary<string, string>();
        _maxSize = maxSize;
    }

    public void Clear() => _entries.Clear();

    public bool CapacityReached() => _entries.Count >= _maxSize;

    public bool ContainsKey(string key) => _entries.ContainsKey(key);

    public string this[string key]
    {
        get => _entries[key];
        set => _entries[key] = value;
    }

    public IEnumerator<SegmentEntry> GetEnumerator()
    {
        foreach (var kvp in _entries)
        {
            yield return new SegmentEntry(kvp.Key, kvp.Value);
        }
    }
}