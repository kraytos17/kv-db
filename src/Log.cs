using Microsoft.Extensions.Logging;

namespace KVDb;

public static partial class LogMessages
{
    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Information,
        Message = "Initializing database at path: {BasePath}")]
    public static partial void InitializingDatabase(this ILogger<Db> logger, string basePath);

    [LoggerMessage(
        EventId = 2,
        Level = LogLevel.Information,
        Message = "Scanning path for segments...")]
    public static partial void ScanningPathForSegments(this ILogger<Db> logger);

    [LoggerMessage(
        EventId = 3,
        Level = LogLevel.Information,
        Message = "Found {Count} segment files.")]
    public static partial void FoundSegmentFiles(this ILogger<Db> logger, int count);

    [LoggerMessage(
        EventId = 4,
        Level = LogLevel.Information,
        Message = "Updating sparse memory index...")]
    public static partial void UpdatingSparseMemoryIndex(this ILogger<Db> logger);

    [LoggerMessage(
        EventId = 5,
        Level = LogLevel.Information,
        Message = "Sparse memory index updated with {Count} entries.")]
    public static partial void SparseMemoryIndexUpdated(this ILogger<Db> logger, int count);

    [LoggerMessage(
        EventId = 7,
        Level = LogLevel.Information,
        Message = "Bloom filter updated.")]
    public static partial void BloomFilterUpdated(this ILogger<Db> logger);

    [LoggerMessage(
        EventId = 8,
        Level = LogLevel.Information,
        Message = "Inserting key: {Key}")]
    public static partial void InsertingKey(this ILogger<Db> logger, string key);

    [LoggerMessage(
        EventId = 9,
        Level = LogLevel.Information,
        Message = "MemTable capacity reached. Writing to segment...")]
    public static partial void MemTableCapacityReached(this ILogger<Db> logger);

    [LoggerMessage(
        EventId = 10,
        Level = LogLevel.Information,
        Message = "Merge threshold reached. Merging segments...")]
    public static partial void MergeThresholdReached(this ILogger<Db> logger);

    [LoggerMessage(
        EventId = 11,
        Level = LogLevel.Information,
        Message = "Key: {Key} inserted successfully.")]
    public static partial void KeyInserted(this ILogger<Db> logger, string key);

    [LoggerMessage(
        EventId = 12,
        Level = LogLevel.Information,
        Message = "Retrieving value for key: {Key}")]
    public static partial void RetrievingValue(this ILogger<Db> logger, string key);

    [LoggerMessage(
        EventId = 13,
        Level = LogLevel.Information,
        Message = "Key: {Key} not found in Bloom filter.")]
    public static partial void KeyNotFoundInBloomFilter(this ILogger<Db> logger, string key);

    [LoggerMessage(
        EventId = 14,
        Level = LogLevel.Information,
        Message = "Key: {Key} found in MemTable.")]
    public static partial void KeyFoundInMemTable(this ILogger<Db> logger, string key);

    [LoggerMessage(
        EventId = 15,
        Level = LogLevel.Information,
        Message = "Key: {Key} found in segment.")]
    public static partial void KeyFoundInSegment(this ILogger<Db> logger, string key);

    [LoggerMessage(
        EventId = 16,
        Level = LogLevel.Information,
        Message = "Key: {Key} not found in segment.")]
    public static partial void KeyNotFoundInSegment(this ILogger<Db> logger, string key);

    [LoggerMessage(
        EventId = 17,
        Level = LogLevel.Information,
        Message = "MemTable written to segment: {SegmentPath}")]
    public static partial void MemTableWrittenToSegment(this ILogger<Db> logger, string segmentPath);

    [LoggerMessage(
        EventId = 18,
        Level = LogLevel.Information,
        Message = "Merging segments...")]
    public static partial void MergingSegments(this ILogger<Db> logger);

    [LoggerMessage(
        EventId = 19,
        Level = LogLevel.Information,
        Message = "Segments merged successfully.")]
    public static partial void SegmentsMerged(this ILogger<Db> logger);

    [LoggerMessage(
        EventId = 20,
        Level = LogLevel.Information,
        Message = "Chained segments")]
    public static partial void ChainedSegments(this ILogger<Db> logger);
    
    [LoggerMessage(
        EventId = 21,
        Level = LogLevel.Information,
        Message = "Searching for key: {Key} in segment: {Path} from offset: {Offset}")]
    public static partial void SearchForEntryInSegment(this ILogger<Db> logger, string key, string path, long offset);
    
    [LoggerMessage(
        EventId = 22,
        Level = LogLevel.Information,
        Message = "Clearing segment list...")]
    public static partial void ClearingSegmentList(this ILogger<Db> logger);
    
    [LoggerMessage(
        EventId = 23,
        Level = LogLevel.Information,
        Message = "Segment list cleared.")]
    public static partial void SegmentListCleared(this ILogger<Db> logger);
    
    [LoggerMessage(
        EventId = 24,
        Level = LogLevel.Information,
        Message = "Deleting {key}.")]
    public static partial void DeletingKey(this ILogger<Db> logger, string key);
}