using System.Text;

namespace KVDb;

public sealed class SsTable(string filePath) {
    private SstFooter? _footer;
    private readonly List<SstIndexEntry> _index = [];

    public async Task WriteAsync(IDictionary<string, string> record, CancellationToken ct = default) {
        ArgumentNullException.ThrowIfNull(record);

        var sortedKeys = record.Keys.OrderBy(k => k).ToList();
        var metadata = new SstMetadata(
            MinKey: sortedKeys.First(),
            MaxKey: sortedKeys.Last(),
            MaxCount: sortedKeys.Count
        );

        var indexEntries = new List<SstIndexEntry>();
        long position = metadata.ToString().Length;
        
        foreach (var key in sortedKeys) {
            var keyValueLine = $"{key}:{record[key]}\n";
            indexEntries.Add(new SstIndexEntry(key, position));
            position += keyValueLine.Length;
        }

        long indexStart = position;
        var footer = new SstFooter(
            DataStartOffset: metadata.ToString().Length, 
            IndexStartOffset: indexStart
        );

        await using var stream = new StreamWriter(filePath, false, Encoding.UTF8);
        await stream.WriteLineAsync($"MinKey={metadata.MinKey},MaxKey={metadata.MaxKey},MaxCount={metadata.MaxCount}");

        foreach (var key in sortedKeys) {
            await stream.WriteLineAsync($"{key}:{record[key]}");
        }

        foreach (var indexEntry in indexEntries) {
            await stream.WriteLineAsync($"{indexEntry.Key}:{indexEntry.Position}");
        }

        await stream.WriteLineAsync($"IndexStart={footer.IndexStartOffset}");
    }

    public async Task<string?> ReadAsync(ReadOnlyMemory<char> key, CancellationToken cancellationToken = default) {
        if (key.IsEmpty) {
            throw new ArgumentException("Key cannot be empty", nameof(key));
        }

        await EnsureFooterAndIndexAsync(cancellationToken);
        var keyStr = key.ToString();
        var indexEntry = _index.FirstOrDefault(entry => entry.Key == keyStr);
        if (indexEntry == default) return null;

        await using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read);
        stream.Seek(indexEntry.Position, SeekOrigin.Begin);

        using var reader = new StreamReader(stream, Encoding.UTF8, leaveOpen: true);
        return await reader.ReadLineAsync(cancellationToken);
    }

    private async Task<Dictionary<string, string>> ReadAllAsync(CancellationToken cancellationToken = default) {
        await EnsureFooterAndIndexAsync(cancellationToken);

        var records = new Dictionary<string, string>();
        if (_footer == null) return records;

        await using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read);
        stream.Seek(_footer.DataStartOffset, SeekOrigin.Begin);

        using var reader = new StreamReader(stream, Encoding.UTF8, leaveOpen: true);
        while (await reader.ReadLineAsync(cancellationToken) is { } line) {
            if (line.StartsWith("IndexStart=")) break;

            var parts = line.Split(':');
            if (parts.Length == 2) {
                records[parts[0]] = parts[1];
            }
        }

        return records;
    }

    public static async Task<SsTable> CompactAsync(IEnumerable<SsTable> tables, string outputPath, CancellationToken ct = default) {
        var mergedRecords = new SortedDictionary<string, string>();
        foreach (var table in tables) {
            var tableRecords = await table.ReadAllAsync(ct);
            foreach (var (key, value) in tableRecords) {
                mergedRecords[key] = value;
            }
        }

        var compactedTable = new SsTable(outputPath);
        await compactedTable.WriteAsync(mergedRecords, ct);
        return compactedTable;
    }

    public static async Task<SsTable> RecoverFromWalAsync(string walPath, string sstablePath, CancellationToken ct = default) {
        var recoveredRecords = new SortedDictionary<string, string>();

        await using var stream = new FileStream(walPath, FileMode.Open, FileAccess.Read);
        using var reader = new StreamReader(stream, Encoding.UTF8);

        while (await reader.ReadLineAsync(ct) is { } line) {
            var parts = line.Split('|');
            if (parts.Length != 3) continue;

            var key = parts[0];
            var value = parts[1];
            var operation = parts[2];

            switch (operation) {
                case "INSERT":
                    recoveredRecords[key] = value;
                    break;
                case "DELETE":
                    recoveredRecords.Remove(key);
                    break;
            }
        }

        var recoveredTable = new SsTable(sstablePath);
        await recoveredTable.WriteAsync(recoveredRecords, ct);
        return recoveredTable;
    }

    private async Task EnsureFooterAndIndexAsync(CancellationToken cancellationToken = default) {
        if (_footer is not null && _index.Any()) return;

        await using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read);
        using var reader = new StreamReader(stream, Encoding.UTF8);

        long position = 0;
        while (await reader.ReadLineAsync(cancellationToken) is { } line) {
            if (line.StartsWith("IndexStart=")) {
                _footer = new SstFooter(
                    DataStartOffset: position, 
                    IndexStartOffset: long.Parse(line.Split('=')[1])
                );
                break;
            }

            if (line.Contains(':')) {
                var parts = line.Split(':');
                if (parts.Length == 2) {
                    _index.Add(new SstIndexEntry(parts[0], position));
                }
            }

            position += line.Length + 1;
        }
    }
}
