namespace KVDb;

public sealed class KeyDirEntry(long offset, Segment segment)
{
    public long Offset { get; } = offset;
    public Segment Segment { get; } = segment;
}