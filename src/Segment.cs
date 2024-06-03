using System.Buffers;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace KVDb;

public sealed partial class Segment : IDisposable
{
    public string Path { get; }
    private readonly FileStream _fileStream;
    private readonly BufferedStream _bufferedStream;
    private readonly StreamWriter _writer;
    private readonly StreamReader _reader;
    private string? _previousEntryKey;
    private int Size { get; set; }
    public double Timestamp { get; }

    private static readonly ArrayPool<byte> ByteArrayPool = ArrayPool<byte>.Shared;

    public Segment(string path)
    {
        Path = path;
        _fileStream = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 4096, FileOptions.SequentialScan);
        _bufferedStream = new BufferedStream(_fileStream, 4096);
        _writer = new StreamWriter(_bufferedStream, Encoding.UTF8, 4096);
        _reader = new StreamReader(_bufferedStream, Encoding.UTF8, true, 4096);
        Size = 0;
        _previousEntryKey = null;
        Timestamp = ExtractTimestamp();
    }

    private double ExtractTimestamp()
    {
        var match = MyRegex().Match(Path);
        return match.Success ? double.Parse(match.Value) : 0;
    }

    public void Dispose()
    {
        _writer.Flush();
        _fileStream.Dispose();
        _bufferedStream.Dispose();
        _reader.Dispose();
        _writer.Dispose();
    }

    public SegmentEntry? ReadEntry()
    {
        var line = _reader.ReadLine();
        if (line == null)
        {
            return null;
        }

        var utf8Bytes = ByteArrayPool.Rent(Encoding.UTF8.GetMaxByteCount(line.Length));
        try
        {
            var byteCount = Encoding.UTF8.GetBytes(line, utf8Bytes);
            var dict = JsonSerializer.Deserialize<Dictionary<string, string>>(utf8Bytes.AsSpan(0, byteCount));
            Debug.Assert(dict != null, nameof(dict) + " != null");
            var (key, value) = dict.FirstOrDefault();
            return new SegmentEntry(key, value);
        }
        finally
        {
            ByteArrayPool.Return(utf8Bytes);
        }
    }

    public async Task<SegmentEntry?> ReadEntryAsync()
    {
        var line = await _reader.ReadLineAsync();
        if (line == null)
        {
            return null;
        }

        var utf8Bytes = ByteArrayPool.Rent(Encoding.UTF8.GetMaxByteCount(line.Length));
        try
        {
            var byteCount = Encoding.UTF8.GetBytes(line, utf8Bytes);
            var dict = JsonSerializer.Deserialize<Dictionary<string, string>>(utf8Bytes.AsSpan(0, byteCount));
            var kvp = dict?.FirstOrDefault();
            return new SegmentEntry(kvp?.Key!, kvp?.Value!);
        }
        finally
        {
            ByteArrayPool.Return(utf8Bytes);
        }
    }

    public void AddEntry(SegmentEntry entry)
    {
        if (_previousEntryKey != null && string.CompareOrdinal(_previousEntryKey, entry.Key) > 0)
        {
            throw new UnsortedEntriesException();
        }

        var jsonUtf8 = JsonSerializer.Serialize(entry.ToDictionary());
        _writer.WriteLine(jsonUtf8);
        _previousEntryKey = entry.Key;
        Size++;
    }

    public async Task AddEntryAsync(SegmentEntry entry)
    {
        if (_previousEntryKey != null && string.CompareOrdinal(_previousEntryKey, entry.Key) > 0)
        {
            throw new UnsortedEntriesException();
        }

        var jsonUtf8 = JsonSerializer.Serialize(entry.ToDictionary());
        await _writer.WriteLineAsync(jsonUtf8);
        _previousEntryKey = entry.Key;
        Size++;
    }

    public void Seek(long position) => _fileStream.Seek(position, SeekOrigin.Begin);

    public long GetPosition() => _fileStream.Position;

    public bool ReachedEof() => _reader.EndOfStream;

    [GeneratedRegex(@"[+-]?\d+\.\d+")]
    private static partial Regex MyRegex();
}