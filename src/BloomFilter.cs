using System.Collections;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace KVDb;

public sealed class BloomFilter {
    private readonly BitArray _bitArray;
    private readonly int[] _seeds;
    private readonly int _hashFunctionCount;
    private readonly int _expectedItems;
    private readonly double _falsePositiveRate;
    private readonly JsonSerializerOptions _options = new() {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public BloomFilter(int expectedItems, double falsePositiveRate) {
        if (expectedItems <= 0)
            throw new ArgumentException("Expected items must be a positive number.", nameof(expectedItems));

        if (falsePositiveRate is <= 0 or >= 1)
            throw new ArgumentException("False positive rate must be between 0 and 1.", nameof(falsePositiveRate));

        _expectedItems = expectedItems;
        _falsePositiveRate = falsePositiveRate;

        int bitArraySize = CalculateBitArraySize(expectedItems, falsePositiveRate);
        _hashFunctionCount = CalculateHashFunctionCount(bitArraySize, expectedItems);
        _bitArray = new BitArray(bitArraySize);
        _seeds = GenerateHashSeeds(_hashFunctionCount);
    }

    private BloomFilter(BitArray bitArray, int[] seeds, int hashFunctionCount, int expectedItems, double falsePositiveRate) {
        _bitArray = bitArray;
        _seeds = seeds;
        _hashFunctionCount = hashFunctionCount;
        _expectedItems = expectedItems;
        _falsePositiveRate = falsePositiveRate;
    }

    public void Add(string? key) {
        ArgumentNullException.ThrowIfNull(key);

        foreach (var hash in HashKey(key)) {
            _bitArray[hash] = true;
        }
    }

    public bool MightContain(string? key) {
        ArgumentNullException.ThrowIfNull(key);

        return HashKey(key).All(hash => _bitArray[hash]);
    }

    public void SaveToDisk(string filePath) {
        try {
            var bloomFilterData = CreateBloomFilterData();
            var jsonString = JsonSerializer.Serialize(bloomFilterData, _options);
            File.WriteAllText(filePath, jsonString);
        }
        catch (Exception ex) {
            throw new IOException($"Error saving Bloom Filter to {filePath}: {ex.Message}", ex);
        }
    }

    public BloomFilter LoadFromDisk(string filePath) {
        try {
            string jsonString = File.ReadAllText(filePath);
            return DeserializeBloomFilter(jsonString);
        }
        catch (Exception ex) {
            throw new IOException($"Error loading Bloom Filter from {filePath}: {ex.Message}", ex);
        }
    }

    public string Serialize() {
        var bloomFilterData = CreateBloomFilterData();
        return JsonSerializer.Serialize(bloomFilterData, _options);
    }

    public BloomFilter Deserialize(string serializedData) => DeserializeBloomFilter(serializedData);
    
    private BloomFilterData CreateBloomFilterData() {
        return new BloomFilterData {
            ExpectedItems = _expectedItems,
            FalsePositiveRate = _falsePositiveRate,
            BitArrayLength = _bitArray.Length,
            HashFunctionCount = _hashFunctionCount,
            Seeds = _seeds,
            BitArray = SerializeBitArray()
        };
    }

    private BloomFilter DeserializeBloomFilter(string serializedData) {
        var bloomFilterData = JsonSerializer.Deserialize<BloomFilterData>(serializedData, _options);
        
        if (bloomFilterData == null)
            throw new InvalidOperationException("Failed to deserialize Bloom Filter data.");

        var bitArray = new BitArray(bloomFilterData.BitArray) {
            Length = bloomFilterData.BitArrayLength
        };

        return new BloomFilter(
            bitArray,
            bloomFilterData.Seeds,
            bloomFilterData.HashFunctionCount,
            bloomFilterData.ExpectedItems,
            bloomFilterData.FalsePositiveRate
        );
    }

    private IEnumerable<int> HashKey(string key) => 
        _seeds.Select(seed => Math.Abs(MurmurHash3.Hash(key, (uint)seed) % _bitArray.Length));

    private static int[] GenerateHashSeeds(int count) {
        var seeds = new int[count];
        var random = new Random();
        for (var i = 0; i < count; ++i) {
            seeds[i] = random.Next();
        }
        return seeds;
    }

    private static int CalculateBitArraySize(int n, double p) => (int)Math.Ceiling(-n * Math.Log(p) / Math.Pow(Math.Log(2), 2));
    private static int CalculateHashFunctionCount(int m, int n) => (int)Math.Ceiling((double)m / n * Math.Log(2));
    
    private bool[] SerializeBitArray() {
        var boolArray = new bool[_bitArray.Length];
        for (var i = 0; i < _bitArray.Length; i++) {
            boolArray[i] = _bitArray[i];
        }
        return boolArray;
    }
    
    public void Clear() => _bitArray.SetAll(false);
    private class BloomFilterData {
        public int ExpectedItems { get; init; }
        public double FalsePositiveRate { get; init; }
        public int BitArrayLength { get; init; }
        public int HashFunctionCount { get; init; }
        public required int[] Seeds { get; init; }
        public required bool[] BitArray { get; init; }
    }
}
