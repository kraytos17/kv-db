using System.Text.RegularExpressions;
using BloomFilter;
using BloomFilter.Configurations;
using Elephant.Uuidv5Utilities;
using Microsoft.Extensions.Logging;

namespace KVDb;

public sealed partial class Db
{
    private readonly MemTable _memTable;
    private readonly SortedDictionary<string, List<KeyDirEntry>> _sparseMemoryIndex;
    private readonly List<Segment> _immutableSegments;
    private readonly IBloomFilter _bloomFilter;
    private readonly ILogger<Db> _logger;
    private readonly int _maxInMemorySize;
    private readonly int _sparseOffset;
    private readonly int _segmentSize;
    private readonly int _mergeThreshold;
    private readonly bool _persistSegments;
    private readonly string _basePath;
    private const string SegmentDir = "sst_data";
    private static readonly string Tombstone = GenerateTombstone();
    private const string TombStone = "Tombstone";

    public Db(
        ILogger<Db> logger,
        int maxInMemorySize = 1000,
        int sparseOffset = 300,
        int segmentSize = 50,
        bool persistSegments = true,
        string? basePath = null,
        int mergeThreshold = 3)
    {
        _logger = logger;
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

        _logger.InitializingDatabase(_basePath);
        ScanPathForSegments();
    }
    
    private static string GenerateTombstone()
    {
        // Define the namespace UUID (equivalent to uuid.NAMESPACE_OID in Python)
        var namespaceOid = new Guid("6ba7b812-9dad-11d1-80b4-00c04fd430c8");
        return Uuidv5Utils.GenerateGuid(namespaceOid, TombStone).ToString();
    }

    private void ScanPathForSegments()
    {
        _logger.ScanningPathForSegments();
        var segmentFiles = Directory.GetFiles(_basePath)
            .Where(file => MyRegex().IsMatch(Path.GetFileName(file)))
            .OrderBy(file => file)
            .ToList();

        _logger.FoundSegmentFiles(segmentFiles.Count);

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
        _logger.UpdatingSparseMemoryIndex();
        int count = 0;
        foreach (var segment in _immutableSegments)
        {
            using (segment)
            {
                while (!segment.ReachedEof())
                {
                    var offset = segment.GetPosition();
                    var entry = segment.ReadEntry();
                    if (count % _sparseOffset == 0 && entry is not null)
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
        }
        _logger.SparseMemoryIndexUpdated(count);
    }

    private void UpdateBloomFilter()
    {
        _logger.LogInformation("Updating Bloom filter...");
        foreach (var segment in _immutableSegments)
        {
            using (segment)
            {
                while (!segment.ReachedEof())
                {
                    var entry = segment.ReadEntry();
                    if (entry is not null)
                    {
                        _bloomFilter.Add(entry.Key);
                    }
                }
            }
        }
        _logger.BloomFilterUpdated();
    }

    public async Task Insert(string key, string value)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            throw new ArgumentException("Key cannot be null or whitespace.", nameof(key));
        }

        _logger.InsertingKey(key);

        if (_memTable.CapacityReached())
        {
            _logger.MemTableCapacityReached();
            var segment = await WriteToSegmentAsync();
            _immutableSegments.Add(segment);

            if (_immutableSegments.Count >= _mergeThreshold)
            {
                _logger.MergeThresholdReached();
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
        _logger.KeyInserted(key);
    }

    public async Task<string?> GetAsync(string key)
    {
        _logger.RetrievingValue(key);

        if (!await _bloomFilter.ContainsAsync(key))
        {
            _logger.KeyNotFoundInBloomFilter(key);
            return null;
        }

        if (_memTable.ContainsKey(key))
        {
            var value = _memTable[key];
            _logger.KeyFoundInMemTable(key);
            return value == Tombstone ? null : value;
        }

        foreach (var closestKey in _sparseMemoryIndex.Keys.Reverse())
        {
            if (string.Compare(closestKey, key, StringComparison.Ordinal) <= 0)
            {
                foreach (var keyDirEntry in _sparseMemoryIndex[closestKey].OrderByDescending(kde => kde.Offset))
                {
                    var entry = await SearchEntryInSegmentAsync(keyDirEntry.Segment, key, keyDirEntry.Offset);
                    if (entry is not null)
                    {
                        _logger.KeyFoundInSegment(key);
                        return entry.Value;
                    }
                }
            }
        }

        foreach (var segment in _immutableSegments.OrderByDescending(s => s.Timestamp))
        {
            var entry = await SearchEntryInSegmentAsync(segment, key, 0);
            if (entry is not null)
            {
                _logger.KeyFoundInSegment(key);
                return entry.Value;
            }
        }
        _logger.KeyNotFoundInSegment(key);
        return null;
    }
    
    public async Task DeleteAsync(string key)
    {
        _logger.DeletingKey(key);
        await Insert(key, Tombstone);
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
        _logger.MemTableWrittenToSegment(segment.Path);
        return segment;
    }

    private async Task<List<Segment>> MergeSegmentsAsync()
    {
        _logger.MergingSegments();
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
            if (newSegment is not null && !mergedSegments.Contains(newSegment))
            {
                newSegment.Dispose();
            }
        }
        _logger.SegmentsMerged();
        return mergedSegments;
    }

    private async IAsyncEnumerable<SegmentEntry> ChainSegmentsAsync(params Segment[] segments)
    {
        _logger.LogInformation("Chaining segments...");
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
                if (entry is not null)
                {
                    heap.Add((entry.Key, segment.Timestamp, entry, segment));
                }
            }
        }

        while (heap.Count > 0)
        {
            var (_, _, entry, segment) = heap.Min;
            heap.Remove(heap.Min);

            if (previousEntry is not null && entry.Key == previousEntry.Key)
            {
                var nextEntry = await segment.ReadEntryAsync();
                if (nextEntry is not null)
                    heap.Add((nextEntry.Key, segment.Timestamp, nextEntry, segment));
                continue;
            }
            yield return entry;
            previousEntry = entry;

            var newEntry = await segment.ReadEntryAsync();
            if (newEntry is not null)
            {
                heap.Add((newEntry.Key, segment.Timestamp, newEntry, segment));
            }
        }
        _logger.ChainedSegments();
    }

    private async Task<SegmentEntry?> SearchEntryInSegmentAsync(Segment segment, string key, long offset)
    {
        _logger.SearchForEntryInSegment(key, segment.Path, offset);
        using (segment)
        {
            segment.Seek(offset);
            while (!segment.ReachedEof())
            {
                var entry = await segment.ReadEntryAsync();
                if (entry is null)
                {
                    break;
                }

                if (entry.Key == key)
                {
                    _logger.KeyFoundInSegment(key);
                    return entry;
                }

                if (string.Compare(entry.Key, key, StringComparison.Ordinal) > 0)
                {
                    break;
                }
            }
        }
        _logger.KeyNotFoundInSegment(key);
        return null;
    }

    private void ClearSegmentList()
    {
        _logger.ClearingSegmentList();
        foreach (var segment in _immutableSegments)
        {
            File.Delete(segment.Path);
        }
        _immutableSegments.Clear();
        _logger.SegmentListCleared();
    }

    [GeneratedRegex(@"^\d+\.\d+\.txt$")]
    private static partial Regex MyRegex();
}
