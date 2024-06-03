using System.Text.RegularExpressions;
using BloomFilter;
using BloomFilter.Configurations;

namespace KVDb;

public sealed partial class Db
{
    private readonly MemTable _memTable;
    private readonly SortedDictionary<string, List<KeyDirEntry>> _sparseMemoryIndex;
    private readonly List<Segment> _immutableSegments;
    private readonly IBloomFilter _bloomFilter;
    private readonly int _maxInMemorySize;
    private readonly int _sparseOffset;
    private readonly int _segmentSize;
    private readonly int _mergeThreshold;
    private readonly bool _persistSegments;
    private readonly string _basePath;
    private const string SegmentDir = "sst_data";
    private const string Tombstone = "TOMBSTONE";

    public Db(
        int maxInMemorySize = 1000,
        int sparseOffset = 300,
        int segmentSize = 50,
        bool persistSegments = true,
        string? basePath = null,
        int mergeThreshold = 3)
    {
        _maxInMemorySize = maxInMemorySize;
        _sparseOffset = sparseOffset;
        _segmentSize = segmentSize;
        _persistSegments = persistSegments;
        _mergeThreshold = mergeThreshold;
        _basePath = basePath ?? SegmentDir;
        _memTable = new MemTable(maxInMemorySize);
        _sparseMemoryIndex = new SortedDictionary<string, List<KeyDirEntry>>();
        _immutableSegments = new List<Segment>();
        _bloomFilter = FilterBuilder.Build(new FilterMemoryOptions());

        if (!Directory.Exists(_basePath))
        {
            Directory.CreateDirectory(_basePath);
        }

        ScanPathForSegments();
    }

    private void ScanPathForSegments()
    {
        var segmentFiles = Directory.GetFiles(_basePath)
            .Where(file => MyRegex().IsMatch(Path.GetFileName(file)))
            .OrderBy(file => file)
            .ToList();

        foreach (var file in segmentFiles)
        {
            var segment = new Segment(file);
            _immutableSegments.Add(segment);
        }

        UpdateSparseMemoryIndex();
        UpdateBloomFilter();
    }

    private void UpdateSparseMemoryIndex()
    {
        int count = 0;

        foreach (var segment in _immutableSegments)
        {
            using (segment)
            {
                while (!segment.ReachedEof())
                {
                    var offset = segment.GetPosition();
                    var entry = segment.ReadEntry();
                    if (count % _sparseOffset == 0 && entry != null)
                    {
                        if (!_sparseMemoryIndex.ContainsKey(entry.Key))
                        {
                            _sparseMemoryIndex[entry.Key] = new List<KeyDirEntry>();
                        }
                        _sparseMemoryIndex[entry.Key].Add(new KeyDirEntry(offset, segment));
                    }
                    count++;
                }
            }
        }
    }

    private void UpdateBloomFilter()
    {
        foreach (var segment in _immutableSegments)
        {
            using (segment)
            {
                while (!segment.ReachedEof())
                {
                    var entry = segment.ReadEntry();
                    if (entry != null)
                    {
                        _bloomFilter.Add(entry.Key);
                    }
                }
            }
        }
    }

    public async Task Insert(string key, string value)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            throw new ArgumentException("Key cannot be null or whitespace.", nameof(key));
        }

        if (_memTable.CapacityReached())
        {
            var segment = await WriteToSegmentAsync();
            _immutableSegments.Add(segment);

            if (_immutableSegments.Count >= _mergeThreshold)
            {
                var mergedSegments = await MergeSegmentsAsync();
                ClearSegmentList();
                _immutableSegments.AddRange(mergedSegments);
                _sparseMemoryIndex.Clear();
                UpdateSparseMemoryIndex();
            }
            _memTable.Clear();
        }
        _memTable[key] = value;
        await _bloomFilter.AddAsync(key);
    }

    public async Task<string?> GetAsync(string key)
    {
        if (!await _bloomFilter.ContainsAsync(key))
        {
            return null;
        }

        if (_memTable.ContainsKey(key))
        {
            var value = _memTable[key];
            return value == Tombstone ? null : value;
        }

        foreach (var closestKey in _sparseMemoryIndex.Keys.Reverse())
        {
            if (string.Compare(closestKey, key, StringComparison.Ordinal) <= 0)
            {
                foreach (var keyDirEntry in _sparseMemoryIndex[closestKey].OrderByDescending(kde => kde.Offset))
                {
                    var entry = await SearchEntryInSegmentAsync(keyDirEntry.Segment, key, keyDirEntry.Offset);
                    return entry?.Value;
                }
            }
        }

        foreach (var segment in _immutableSegments.OrderByDescending(s => s.Timestamp))
        {
            var entry = await SearchEntryInSegmentAsync(segment, key, 0);
            if (entry != null)
            {
                return entry.Value;
            }
        }

        return null;
    }

    private async Task<Segment> WriteToSegmentAsync()
    {
        var segment = new Segment(Path.Combine(_basePath, $"{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}.txt"));
        using (segment)
        {
            int count = 0;
            foreach (var entry in _memTable)
            {
                var offset = segment.GetPosition();
                await segment.AddEntryAsync(entry);

                if (count % _sparseOffset == 0)
                {
                    if (!_sparseMemoryIndex.TryGetValue(entry.Key, out var value))
                    {
                        value = new List<KeyDirEntry>();
                        _sparseMemoryIndex[entry.Key] = value;
                    }
                    value.Add(new KeyDirEntry(offset, segment));
                }
                count++;
            }
        }
        return segment;
    }

    private async Task<List<Segment>> MergeSegmentsAsync()
    {
        var mergedSegments = new List<Segment>();
        var entries = ChainSegmentsAsync(_immutableSegments.ToArray());

        Segment? newSegment = null;
        try
        {
            newSegment = new Segment(Path.Combine(_basePath, $"{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}.txt"));
            int count = 0;
            await foreach (var entry in entries)
            {
                await newSegment.AddEntryAsync(entry);
                count++;
                if (count >= _segmentSize)
                {
                    newSegment.Dispose();
                    mergedSegments.Add(newSegment);
                    newSegment = new Segment(Path.Combine(_basePath, $"{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}.txt"));
                    count = 0;
                }
            }

            if (count > 0)
            {
                newSegment.Dispose();
                mergedSegments.Add(newSegment);
            }
        }
        finally
        {
            if (newSegment != null && !mergedSegments.Contains(newSegment))
            {
                newSegment.Dispose();
            }
        }
        return mergedSegments;
    }

    private static async IAsyncEnumerable<SegmentEntry> ChainSegmentsAsync(params Segment[] segments)
    {
        var heap = new SortedSet<(string Key, double Timestamp, SegmentEntry Entry, Segment Segment)>(
            Comparer<(string, double, SegmentEntry, Segment)>.Create((x, y) =>
            {
                var keyComparison = string.Compare(x.Item1, y.Item1, StringComparison.Ordinal);
                return keyComparison != 0 ? keyComparison : x.Item4.Timestamp.CompareTo(y.Item4.Timestamp);
            }));

        var previousEntry = default(SegmentEntry);
        foreach (var segment in segments)
        {
            using (segment)
            {
                var entry = await segment.ReadEntryAsync();
                if (entry != null)
                {
                    heap.Add((entry.Key, segment.Timestamp, entry, segment));
                }
            }
        }

        while (heap.Count > 0)
        {
            var (_, _, entry, segment) = heap.Min;
            heap.Remove(heap.Min);

            if (previousEntry != null && entry.Key == previousEntry.Key)
            {
                var nextEntry = await segment.ReadEntryAsync();
                if (nextEntry != null)
                    heap.Add((nextEntry.Key, segment.Timestamp, nextEntry, segment));
                continue;
            }
            yield return entry;
            previousEntry = entry;

            var newEntry = await segment.ReadEntryAsync();
            if (newEntry != null)
            {
                heap.Add((newEntry.Key, segment.Timestamp, newEntry, segment));
            }
        }
    }

    private static async Task<SegmentEntry?> SearchEntryInSegmentAsync(Segment segment, string key, long offset)
    {
        using (segment)
        {
            segment.Seek(offset);
            while (!segment.ReachedEof())
            {
                var entry = await segment.ReadEntryAsync();
                if (entry == null)
                {
                    break;
                }

                if (entry.Key == key)
                {
                    return entry;
                }

                if (string.Compare(entry.Key, key, StringComparison.Ordinal) > 0)
                {
                    break;
                }
            }
        }
        return null;
    }

    private void ClearSegmentList()
    {
        foreach (var segment in _immutableSegments)
        {
            File.Delete(segment.Path);
        }
        _immutableSegments.Clear();
    }

    [GeneratedRegex(@"^\d+\.\d+\.txt$")]
    private static partial Regex MyRegex();
}