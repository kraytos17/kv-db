using System.Collections;

namespace KVDb;

public sealed class BloomFilter {
    private readonly BitArray _bitArray;
    private readonly int[] _seeds;
    private readonly int _hashFunctionCount;

    public BloomFilter(int expectedItems, double falsePositiveRate) {
        int bitArraySize = CalculateBitArraySize(expectedItems, falsePositiveRate);
        _hashFunctionCount = CalculateHashFunctionCount(bitArraySize, expectedItems);
        _bitArray = new BitArray(bitArraySize);
        _seeds = GenerateHashSeeds(_hashFunctionCount);
    }

    public void Add(string key) {
        foreach (var hash in HashKey(key)) {
            _bitArray[hash] = true;
        }
    }

    public bool MightContain(string key) => HashKey(key).All(hash => _bitArray[hash]);

    public void SaveToDisk(string filePath) {
        using var stream = new FileStream(filePath, FileMode.Create, FileAccess.Write);
        using var writer = new BinaryWriter(stream);

        writer.Write(_bitArray.Length);
        writer.Write(_hashFunctionCount);
        writer.Write(_seeds.Length);
        foreach (var seed in _seeds) writer.Write(seed);
        foreach (bool bit in _bitArray) writer.Write(bit);
    }

    public static BloomFilter LoadFromDisk(string filePath) {
        using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read);
        using var reader = new BinaryReader(stream);

        int bitArraySize = reader.ReadInt32();
        int hashFunctionCount = reader.ReadInt32();
        int seedCount = reader.ReadInt32();
        var seeds = new int[seedCount];
        for (var i = 0; i < seedCount; ++i) seeds[i] = reader.ReadInt32();

        var bitArray = new BitArray(bitArraySize);
        for (var i = 0; i < bitArraySize; ++i) bitArray[i] = reader.ReadBoolean();

        return new BloomFilter(bitArray, seeds, hashFunctionCount);
    }

    private IEnumerable<int> HashKey(string key) => 
        _seeds.Select(seed => Math.Abs(MurmurHash3.Hash(key, (uint)seed) % _bitArray.Length));

    private static int[] GenerateHashSeeds(int count) {
        var seeds = new int[count];
        var random = new Random();
        for (var i = 0; i < count; ++i) seeds[i] = random.Next();
        return seeds;
    }

    private static int CalculateBitArraySize(int n, double p) => (int)Math.Ceiling(-n * Math.Log(p) / Math.Pow(Math.Log(2), 2));

    private static int CalculateHashFunctionCount(int m, int n) => (int)Math.Ceiling((double)m / n * Math.Log(2));

    private BloomFilter(BitArray bitArray, int[] seeds, int hashFunctionCount) {
        _bitArray = bitArray;
        _seeds = seeds;
        _hashFunctionCount = hashFunctionCount;
    }
}
