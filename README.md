# KVDb: Key-Value Database with Bloom Filter and SSTable Support
KVDb is a custom .NET-based key-value database designed to handle large volumes of data with high efficiency and low memory usage. It supports data persistence with SSTables, in-memory storage through MemTables, and probabilistic filtering using a Bloom filter.

## Features
* MemTable for In-Memory Data: Stores data before persisting to disk.
* SSTable Storage: Efficient on-disk storage with support for merges.
* Bloom Filter: Fast existence checks for keys.
* Logging and Error Handling: Configurable with Microsoft.Extensions.Logging.
