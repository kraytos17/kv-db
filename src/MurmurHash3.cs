using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text;

namespace KVDb;

public static class MurmurHash3 {
    private const uint Seed = 0x9747b28c; // Default seed value

    public static int Hash(string key, uint seed = Seed) {
        ReadOnlySpan<byte> data = Encoding.UTF8.GetBytes(key);
        return ComputeHash(data, seed);
    }

    private static int ComputeHash(ReadOnlySpan<byte> data, uint seed) {
        const uint c1 = 0xcc9e2d51;
        const uint c2 = 0x1b873593;
        const int r1 = 15;
        const int r2 = 13;
        const uint m = 5;
        const uint n = 0xe6546b64;

        uint hash = seed;
        int length = data.Length;
        int numBlocks = length / 4;

        for (int i = 0; i < numBlocks; i++) {
            uint k = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(i * 4, 4));

            k *= c1;
            k = RotateLeft(k, r1);
            k *= c2;

            hash ^= k;
            hash = RotateLeft(hash, r2) * m + n;
        }

        uint tail = 0;
        int remainingBytesStart = numBlocks * 4;
        int remainingBytesCount = length % 4;

        if (remainingBytesCount > 0) {
            for (var i = 0; i < remainingBytesCount; ++i) {
                tail |= (uint)data[remainingBytesStart + i] << (i * 8);
            }

            tail *= c1;
            tail = RotateLeft(tail, r1);
            tail *= c2;
            hash ^= tail;
        }

        hash ^= (uint)length;
        hash = FMix(hash);

        return (int)hash;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint RotateLeft(uint value, int count) => (value << count) | (value >> (32 - count));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint FMix(uint h) {
        h ^= h >> 16;
        h *= 0x85ebca6b;
        h ^= h >> 13;
        h *= 0xc2b2ae35;
        h ^= h >> 16;

        return h;
    }
}
