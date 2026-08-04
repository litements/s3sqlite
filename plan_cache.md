# Cache implementation plan

## Status

This plan defines the first cache implementation for `s3sqlite`.

The first implementation will provide one cache only:

- a disk-backed least-frequently-used cache;
- a regular SQLite database as the cache file;
- no application-level memory tier;
- an fsspec-independent public cache interface.

Tiered caching can be added later without changing the VFS read path.

## Goals

The implementation must:

1. Let external users implement a cache without importing fsspec or s3fs.
2. Make the cache the default byte-range cache used by `S3VFS`.
3. Reuse fsspec and s3fs for S3 access, authentication, retries, and endpoint support.
4. Persist cached blocks across processes and program runs.
5. Bound the cache file by a configurable byte limit.
6. Evict blocks with an LFU policy.
7. Preserve exact random-access reads required by the APSW VFS.
8. Keep the first implementation small enough to test and reason about.

## Non-goals

This iteration will not implement:

- an in-memory block cache;
- a tiered cache;
- background prefetching;
- cache invalidation APIs;
- distributed cache coordination;
- a custom S3 client;
- fsspec `BaseCache` subclasses or `register_cache()` integration.

SQLite and the operating system may still cache the cache database internally.
The package will not add a separate Python memory cache.

## Public interface

The public interface will use three concepts.

### Object information

The cache needs a stable identity for the remote object.

```python
@dataclass(frozen=True, slots=True)
class ObjectInfo:
    path: str
    size: int
    identity: str
```

`identity` will normally contain an object version ID or ETag. The object path
and size will also be part of the cache key. A source with no validator will
fall back to metadata available from the filesystem adapter.

### Cache provider

The configured cache object will create a reader for each opened remote object.

```python
class Cache(Protocol):
    def open(
        self,
        info: ObjectInfo,
        fetch_range: FetchRange,
    ) -> CacheReader: ...
```

`Cache.open()` is called once for each VFS file handle. The provider may keep
shared state, such as a disk database, across those readers.

### Cache reader

The VFS will use a stateless random-access method.

```python
class CacheReader(Protocol):
    def read_at(self, offset: int, length: int) -> bytes: ...

    def close(self) -> None: ...
```

`read_at()` uses zero-based offsets and returns data from the half-open range
`[offset, offset + length)`. A read past the object end returns the available
bytes. A negative offset or length is invalid.

The cache must return the requested bytes in order. It may fetch larger ranges
internally. It must not expose a partially written cache block.

### Range source callback

The cache will receive a callback with this contract:

```python
FetchRange = Callable[[int, int], bytes]
```

The callback receives a half-open range and must return exactly that range.
The callback is the only remote-I/O dependency of the cache.

## fsspec and s3fs integration

`S3VFS` will continue to accept an fsspec filesystem. The VFS will use it to:

1. Open the remote object.
2. Read object metadata.
3. Build `ObjectInfo`.
4. Provide a range callback to the configured cache.

When the package cache is active, the fsspec byte cache will be disabled with
`cache_type="none"`. This avoids two independent byte-cache layers.

The fsspec file object remains an internal transport detail. External cache
implementations will receive `ObjectInfo` and `FetchRange`, not an fsspec file
object.

The transport callback will use the opened fsspec file with a serialized
`seek()` and `read()` operation. This preserves backend behavior such as
s3fs's object metadata and conditional reads. A later optimized adapter may
use a stateless `cat_file()` call or a direct boto3/botocore range request.

If a custom cache is supplied, its source file will still be opened with the
fsspec byte cache disabled. s3fs connection pooling, credential handling,
retry logic, and metadata behavior remain available.

## Default cache configuration

`S3VFS` will create an `LFUCache` when no custom cache is supplied.

Initial defaults:

- cache path: the platform user cache directory under `s3sqlite/cache.sqlite3`;
- maximum cache size: 1 GiB;
- block size: 64 KiB;
- SQLite journal mode: WAL;
- SQLite synchronous mode: `NORMAL`;
- SQLite busy timeout: 30 seconds.

The cache path, maximum size, block size, and custom cache provider will be
configurable through `S3VFS`.

The block size is a remote-cache setting. The SQLite sector size reported by
the VFS remains 4096 bytes and is not tied to the cache block size.

## LFU behavior

The cache is block based.

For a read:

1. Clamp the requested range to the object size.
2. Map the range to block numbers.
3. Read cached blocks from SQLite.
4. Increment each hit's frequency.
5. Group adjacent missing blocks into contiguous source requests.
6. Split each fetched range into blocks.
7. Store the new blocks with frequency one.
8. Evict the least frequently used blocks if the byte limit is exceeded.
9. Assemble and return the requested slice.

The eviction order is:

1. Lowest frequency first.
2. Oldest `last_used` value first when frequencies match.

The cache will use a logical access timestamp for tie-breaking. Frequency and
access updates will be committed in SQLite transactions.

The implementation will not decay frequencies in this iteration. This keeps
the behavior deterministic and makes the first benchmark results easier to
interpret. Frequency decay can be added if long-lived workloads show that old
hot blocks remain pinned too long.

## SQLite cache schema

The cache database will contain one block table and one usage table.

```sql
CREATE TABLE cache_blocks (
    object_key TEXT NOT NULL,
    block_size INTEGER NOT NULL,
    block_number INTEGER NOT NULL,
    data BLOB NOT NULL,
    frequency INTEGER NOT NULL,
    last_used INTEGER NOT NULL,
    PRIMARY KEY (object_key, block_size, block_number)
);

CREATE INDEX cache_blocks_eviction
ON cache_blocks (frequency, last_used);

CREATE TABLE cache_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    used_bytes INTEGER NOT NULL
);
```

`object_key` will be a deterministic hash of the object path, size, identity,
and cache format version. The block size is stored separately so a cache file
can safely contain entries created with different settings.

`used_bytes` avoids scanning the whole BLOB table on every insertion. Inserts,
frequency updates, usage accounting, and eviction will use SQLite transactions.

## Concurrency and failure behavior

Each cache reader will use its own SQLite connection. SQLite WAL mode will let
readers continue while another process updates the cache. SQLite busy timeout
will handle short writer contention.

The implementation will not coordinate duplicate source fetches across
processes. Two processes may fetch the same missing block. This is safe because
the object key is immutable and each completed block is written as one SQLite
transaction. Avoiding duplicate fetches can be added later if measurements
show that contention matters.

Remote fetches will occur outside SQLite write transactions. A failed fetch
will not create or update a cache row.

A cache write will complete the BLOB row and usage accounting in one commit.
Readers will only use committed rows. SQLite recovery will remove incomplete
transactions after a process failure.

The cache is a performance optimization. A cache read failure or corrupt cache
database must not silently return incorrect bytes. The first implementation
will raise the SQLite or source error and will document clearing the cache file
as the recovery action.

## VFS lifecycle

`S3VFS.xOpen()` will:

1. Open the source file with fsspec byte caching disabled.
2. Read its metadata.
3. Build `ObjectInfo`.
4. Call `cache.open(info, fetch_range)`.
5. Return an `S3VFSFile` that delegates `xRead()` to `CacheReader.read_at()`.

`S3VFSFile.xClose()` will close the cache reader and then the source file.
`xFileSize()` will use the object metadata instead of seeking the source file.

## Implementation steps

1. Add the public cache protocols and `ObjectInfo` type.
2. Add default cache-path resolution and `LFUCache` configuration.
3. Implement SQLite schema creation and versioning.
4. Implement block reads, grouped source fetches, frequency updates, and LFU eviction.
5. Update `S3VFS` to create the default cache and wire the source callback.
6. Update `S3VFSFile` to use `read_at()` and close both resources.
7. Add unit tests for cache behavior and protocol integration.
8. Update integration fixtures to use isolated temporary cache databases.
9. Run formatting, linting, type checks, and the test suite.

## Verification plan

Unit tests will cover:

- partial reads within one block;
- reads spanning multiple blocks;
- reads past end of file;
- empty files;
- repeated reads served from disk without another source fetch;
- persistence after closing and reopening the cache;
- object identity isolation;
- frequency updates;
- LFU eviction with recency tie-breaking;
- maximum cache size enforcement;
- failed fetches leaving no committed block;
- two cache connections using the same SQLite file;
- custom cache provider integration with `S3VFS`.

The existing S3 integration tests will verify that SQLite queries still return
the same rows through the default disk cache. The fixture will use a temporary
cache path so tests do not share the user's cache database.

Performance measurements will be added after correctness is established. They
will record source request count, fetched bytes, query latency, cache size, and
behavior for cold and warm reads.

## Future tiered cache

The public `Cache` and `CacheReader` protocols will not change for tiering.

A future `TieredCache` can use the same `read_at()` contract and add an
in-memory block store above the current SQLite disk store:

```text
read_at()
  -> memory tier
  -> SQLite disk tier
  -> remote range source
```

Disk blocks can be promoted into memory. Remote misses can populate both tiers.
That work is intentionally excluded from the first implementation.
