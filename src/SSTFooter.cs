namespace KVDb;

public sealed record SstFooter(long DataStartOffset, long IndexStartOffset);