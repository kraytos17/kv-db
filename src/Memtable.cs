using System.Text;

namespace KVDb;

public sealed class MemTable : IDisposable {
    private readonly SortedDictionary<string, string> _entries = new();
    private readonly string _walPath;
    private readonly string _sstableDirectory;
    private long _currSize;
    private const long FlushThreshold = 64 * 1024 * 1024;

    public MemTable(string walPath) {
        _walPath = walPath;
        _sstableDirectory = Path.GetDirectoryName(walPath) ?? ".";
        RecoverFromWal();
    }

    public void Insert(string key, string value) {
        WriteLogEntry(key, value, "INSERT");
        _entries[key] = value;
        _currSize += key.Length + value.Length;
        
        if (_currSize >= FlushThreshold) {
            _ =  FlushToDisk();
        }
    }

    public string? Get(string key) {
        return _entries.GetValueOrDefault(key);
    }

    public void Delete(string key) {
        if (_entries.Remove(key)) {
            WriteLogEntry(key, string.Empty, "DELETE");
        }
    }

    public async Task FlushToDisk(CancellationToken cancellationToken = default) {
        if (_entries.Count == 0) return;

        Compact();
        var sstablePath = GenerateSsTablePath();
        var sstable = new SsTable(sstablePath);
        
        await sstable.WriteAsync(_entries, cancellationToken);

        _entries.Clear();
        _currSize = 0;

        WriteLogEntry("FLUSH", sstablePath, "FLUSH");
    }
    
    private string GenerateSsTablePath() {
        var timestamp = DateTime.UtcNow.Ticks;
        return Path.Combine(_sstableDirectory, $"sst_{timestamp}.sst");
    }

    private void Compact() {
        var keysToRemove = _entries.Where(entry => string.IsNullOrEmpty(entry.Value))
                                   .Select(entry => entry.Key)
                                   .ToList();

        foreach (var key in keysToRemove) {
            _entries.Remove(key);
        }
        
        _currSize = _entries.Sum(entry => entry.Key.Length + entry.Value.Length);
    }

    private void RecoverFromWal() {
        if (!File.Exists(_walPath)) return;

        using var stream = new FileStream(_walPath, FileMode.Open, FileAccess.Read);
        using var reader = new StreamReader(stream, Encoding.UTF8);

        var recoveredEntries = new SortedDictionary<string, LogEntry>();
        while (!reader.EndOfStream) {
            var line = reader.ReadLine();
            if (string.IsNullOrEmpty(line)) continue;

            var values = line.Split('|');
            if (values.Length < 4) continue;

            var logEntry = new LogEntry(
                values[0],
                values[1],
                values[2],
                DateTime.Parse(values[3])
            );

            if (!recoveredEntries.TryGetValue(logEntry.Key, out var existingEntry) || 
                existingEntry.Timestamp < logEntry.Timestamp) {
                recoveredEntries[logEntry.Key] = logEntry;
            }
        }

        foreach (var (_, logEntry) in recoveredEntries) {
            switch (logEntry.Operation) {
                case "INSERT":
                    _entries[logEntry.Key] = logEntry.Value;
                    _currSize += logEntry.Key.Length + logEntry.Value.Length;
                    break;
                case "DELETE":
                    _entries.Remove(logEntry.Key);
                    break;
            }
        }

        WriteLogEntry("RECOVERY", "Completed", "RECOVERY");
    }
    
    private void WriteLogEntry(string key, string value, string operation) {
        var entry = new LogEntry(key, value, operation, DateTime.UtcNow);

        using var stream = new FileStream(_walPath, FileMode.Append, FileAccess.Write);
        using var writer = new StreamWriter(stream, Encoding.UTF8);
        writer.WriteLine($"{entry.Key}|{entry.Value}|{entry.Operation}|{entry.Timestamp:O}");
    }

    public void Dispose() {
        FlushToDisk().GetAwaiter().GetResult();
    }
}
