using System.Collections.Concurrent;

namespace KVDb;

public sealed class Db : IDisposable {
    private readonly string _basePath;
    private readonly MemTable _memTable;
    private readonly ConcurrentBag<SsTable> _sstables = [];
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly CancellationTokenSource _cts = new();

    public Db(string basePath) {
        _basePath = basePath;
        Directory.CreateDirectory(basePath);

        var walPath = Path.Combine(basePath, "write-ahead.log");
        _memTable = new MemTable(walPath);

        LoadExistingSsTables();
    }

    private void LoadExistingSsTables() {
        var sstFiles = Directory.GetFiles(_basePath, "*.sst")
            .OrderBy(f => f)
            .Select(f => new SsTable(f));

        foreach (var ssTable in sstFiles) {
            _sstables.Add(ssTable);
        }
    }

    public async Task InsertAsync(string key, string value, CancellationToken cancellationToken = default) {
        await _writeLock.WaitAsync(cancellationToken);
        try { 
            _memTable.Insert(key, value);
        }
        finally {
            _writeLock.Release();
        }
    }

    public async Task<string?> GetAsync(string key, CancellationToken cancellationToken = default) {
        var memTableValue = _memTable.Get(key);
        if (memTableValue is not null) {
            return memTableValue == "DELETED" ? null : memTableValue;
        }

        foreach (var ssTable in _sstables.Reverse()) {
            var value = await ssTable.ReadAsync(key.AsMemory(), cancellationToken);
            if (value is not null) {
                return value;
            }
        }

        return null;
    }

    public async Task DeleteAsync(string key, CancellationToken cancellationToken = default) {
        await _writeLock.WaitAsync(cancellationToken);
        try {
            _memTable.Delete(key);
        }
        finally {
            _writeLock.Release();
        }
    }

    public async Task CompactAsync(CancellationToken cancellationToken = default) {
        await _writeLock.WaitAsync(cancellationToken);
        try {
            await _memTable.FlushToDisk(cancellationToken);

            if (_sstables.Count > 1) {
                var compactedSstablePath = Path.Combine(_basePath, $"compacted_{Guid.NewGuid()}.sst");
                var compactedSsTable = await SsTable.CompactAsync(_sstables, compactedSstablePath, cancellationToken);

                _sstables.Clear();
                _sstables.Add(compactedSsTable);
            }
        }
        finally {
            _writeLock.Release();
        }
    }

    public void Dispose() {
        _memTable.Dispose();
        _writeLock.Dispose();
        _cts.Dispose();
    }
}