namespace KVDb;

public sealed record SstMetadata(string MinKey, string MaxKey, int MaxCount);