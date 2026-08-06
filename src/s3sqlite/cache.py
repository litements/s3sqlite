"""Persistent byte-range cache implementations for :mod:`s3sqlite`."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from collections.abc import Callable
from collections.abc import Generator
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

DEFAULT_BLOCK_SIZE = 64 * 1024
DEFAULT_MAX_SIZE = 100 * 1024**2
DEFAULT_BUSY_TIMEOUT = 30.0
CACHE_SCHEMA_VERSION = 1
CACHE_FORMAT_VERSION = 1


@dataclass(frozen=True, slots=True)
class ObjectInfo:
    """Identify one immutable remote object opened through the VFS."""

    path: str
    size: int
    identity: str


FetchRange = Callable[[int, int], bytes]


class CacheReader(Protocol):
    """Read exact byte ranges from one cached remote object."""

    def read_at(self, offset: int, length: int) -> bytes:
        """Return bytes in the half-open range ``[offset, offset + length)``."""

    def close(self) -> None:
        """Release resources held by this reader."""


class Cache(Protocol):
    """Create cache readers for remote objects."""

    def open(self, info: ObjectInfo, fetch_range: FetchRange) -> CacheReader:
        """Create a reader for ``info`` backed by ``fetch_range``."""


def default_cache_path() -> Path:
    """Return the platform-specific default SQLite cache path."""
    cache_home = os.environ.get("S3SQLITE_CACHE_HOME") or os.environ.get(
        "XDG_CACHE_HOME"
    )
    if cache_home:
        cache_root = Path(cache_home)
    elif os.name == "nt":
        local_app_data = os.environ.get("LOCALAPPDATA")
        cache_root = (
            Path(local_app_data)
            if local_app_data
            else Path.home() / "AppData" / "Local"
        )
    else:
        cache_root = Path.home() / ".cache"

    return cache_root / "s3sqlite" / "cache.sqlite3"


@contextmanager
def _write_transaction(
    connection: sqlite3.Connection,
) -> Generator[None, None, None]:
    """Run a SQLite write transaction with rollback on failure."""
    connection.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def _object_key(info: ObjectInfo) -> str:
    """Return a deterministic cache key for one remote object."""
    key_data = {
        "format_version": CACHE_FORMAT_VERSION,
        "identity": info.identity,
        "path": info.path,
        "size": info.size,
    }
    serialized_key = json.dumps(
        key_data,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(serialized_key.encode("utf-8")).hexdigest()


class LFUCache:
    """A disk-backed least-frequently-used cache of remote object blocks."""

    def __init__(
        self,
        path: str | Path | None = None,
        max_size: int = DEFAULT_MAX_SIZE,
        block_size: int = DEFAULT_BLOCK_SIZE,
        busy_timeout: float = DEFAULT_BUSY_TIMEOUT,
    ) -> None:
        """Create or open a SQLite cache database.

        Args:
            path: SQLite database path. The platform cache path is used when
                omitted.
            max_size: Maximum number of cached data bytes.
            block_size: Size of remote blocks fetched and stored together.
            busy_timeout: SQLite lock timeout in seconds.
        """
        if max_size < 0:
            raise ValueError("max_size must be non-negative")
        if block_size <= 0:
            raise ValueError("block_size must be positive")
        if busy_timeout < 0:
            raise ValueError("busy_timeout must be non-negative")

        self.path = Path(path) if path is not None else default_cache_path()
        self.max_size = max_size
        self.block_size = block_size
        self.busy_timeout = busy_timeout

        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = self._connect()
        connection.close()

    def open(self, info: ObjectInfo, fetch_range: FetchRange) -> CacheReader:
        """Create a reader for ``info`` using the supplied range source."""
        if info.size < 0:
            raise ValueError("ObjectInfo.size must be non-negative")

        connection = self._connect()
        reader = _LFUCacheReader(
            cache=self,
            connection=connection,
            info=info,
            fetch_range=fetch_range,
        )
        try:
            reader._enforce_cache_limit()
        except BaseException:
            reader.close()
            raise
        return reader

    def _connect(self) -> sqlite3.Connection:
        """Open a configured SQLite connection and initialize its schema."""
        if sqlite3.threadsafety != 3:
            raise sqlite3.DatabaseError(
                "s3sqlite requires serialized SQLite (sqlite3.threadsafety "
                f"== 3, got {sqlite3.threadsafety})"
            )
        connection = sqlite3.connect(
            database=str(self.path),
            isolation_level=None,
            timeout=self.busy_timeout,
            # Reader connections are shared across whatever threads APSW uses
            # to run SQL in serialized mode; SQLite's own mutexes serialize
            # statement execution, so no application-level lock is needed.
            check_same_thread=False,
            cached_statements=0,
        )
        try:
            busy_timeout_milliseconds = int(self.busy_timeout * 1000)
            connection.execute(f"PRAGMA busy_timeout = {busy_timeout_milliseconds}")
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = NORMAL")
            self._initialize_schema(connection=connection)
        except BaseException:
            connection.close()
            raise
        return connection

    def _initialize_schema(self, connection: sqlite3.Connection) -> None:
        """Create the cache schema and set its SQLite schema version."""
        version_row = connection.execute("PRAGMA user_version").fetchone()
        if version_row is None:
            raise sqlite3.DatabaseError("SQLite did not return user_version")

        current_version = int(version_row[0])
        if current_version > CACHE_SCHEMA_VERSION:
            raise sqlite3.DatabaseError(
                f"Unsupported cache schema version: {current_version}"
            )
        if current_version == CACHE_SCHEMA_VERSION:
            return

        with _write_transaction(connection):
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS cache_blocks (
                    object_key TEXT NOT NULL,
                    block_size INTEGER NOT NULL,
                    block_number INTEGER NOT NULL,
                    data BLOB NOT NULL,
                    PRIMARY KEY (object_key, block_size, block_number)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS cache_block_usage (
                    object_key TEXT NOT NULL,
                    block_size INTEGER NOT NULL,
                    block_number INTEGER NOT NULL,
                    frequency INTEGER NOT NULL,
                    last_used INTEGER NOT NULL,
                    PRIMARY KEY (object_key, block_size, block_number)
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS cache_block_usage_eviction
                ON cache_block_usage (frequency, last_used)
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS cache_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    used_bytes INTEGER NOT NULL,
                    last_used INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO cache_state (id, used_bytes, last_used)
                VALUES (
                    1,
                    COALESCE((SELECT SUM(length(data)) FROM cache_blocks), 0),
                    COALESCE((SELECT MAX(last_used) FROM cache_block_usage), 0)
                )
                """
            )

            connection.execute(f"PRAGMA user_version = {CACHE_SCHEMA_VERSION}")


class _LFUCacheReader:
    """SQLite-backed reader for one object identity."""

    def __init__(
        self,
        cache: LFUCache,
        connection: sqlite3.Connection,
        info: ObjectInfo,
        fetch_range: FetchRange,
    ) -> None:
        """Create a cache reader with owned read and write connections.

        Reads run on ``connection``; all writes, including usage tracking and
        store transactions, run on ``write_connection`` so that read queries
        never share a connection with an open write transaction.
        """
        self.cache = cache
        self.connection = connection
        try:
            self.write_connection = self._create_write_connection()
        except BaseException:
            connection.close()
            raise
        self.info = info
        self.fetch_range = fetch_range
        self.object_key = _object_key(info)
        self.closed = False

    def read_at(self, offset: int, length: int) -> bytes:
        """Return the requested range, fetching and caching missing blocks.

        SQLite serializes the statements of each local database connection, so
        a reader is used by one thread at a time (APSW may move it between
        threads). Concurrent use of one reader is not supported: write
        transactions on a single connection cannot overlap. Different readers
        sharing the same cache database are safe: SQLite's own file locks and
        the busy timeout serialize their write transactions.
        """
        try:
            return self._read_at(offset=offset, length=length)
        except sqlite3.ProgrammingError as error:
            # A concurrent close() can tear a connection down between the
            # closed check and a statement; SQLite surfaces that as a clean
            # error instead of corruption, and we translate it to our own.
            if self.closed:
                raise ValueError("Cache reader is closed") from error
            raise

    def _read_at(self, offset: int, length: int) -> bytes:
        """Return the requested range from SQLite and the range source."""
        if self.closed:
            raise ValueError("Cache reader is closed")
        if offset < 0:
            raise ValueError("offset must be non-negative")
        if length < 0:
            raise ValueError("length must be non-negative")
        if length == 0 or offset >= self.info.size:
            return b""

        end = min(offset + length, self.info.size)
        first_block = offset // self.cache.block_size
        last_block = (end - 1) // self.cache.block_size
        cached_blocks = self._read_cached_blocks(
            first_block=first_block,
            last_block=last_block,
        )

        missing_blocks = [
            block_number
            for block_number in range(first_block, last_block + 1)
            if block_number not in cached_blocks
        ]
        for missing_start, missing_end in _group_adjacent(missing_blocks):
            fetched_blocks = self._fetch_blocks(
                first_block=missing_start,
                last_block=missing_end,
            )
            cached_blocks.update(fetched_blocks)

        return self._assemble(
            blocks=cached_blocks,
            offset=offset,
            end=end,
            first_block=first_block,
            last_block=last_block,
        )

    def close(self) -> None:
        """Close the reader's SQLite connections.

        Idempotent and safe to race with ``read_at``: the closed flag is set
        before the connections are closed, and pysqlite's deferred close
        leaves any in-flight statement to finish cleanly.
        """
        if self.closed:
            return

        self.closed = True
        self.write_connection.close()
        self.connection.close()

    def _create_write_connection(self) -> sqlite3.Connection:
        """Open the dedicated connection used for all cache writes.

        Kept separate from the read connection so that read queries never run
        on a connection with an open write transaction. Transactions on this
        connection serialize with other writers through SQLite's file locks
        and the busy timeout, with no application-level lock.
        """
        connection = sqlite3.connect(
            database=str(self.cache.path),
            isolation_level=None,
            timeout=self.cache.busy_timeout,
            check_same_thread=False,
            cached_statements=0,
        )
        try:
            busy_timeout_milliseconds = int(self.cache.busy_timeout * 1000)
            connection.execute(f"PRAGMA busy_timeout = {busy_timeout_milliseconds}")
            connection.execute("PRAGMA synchronous = NORMAL")
        except BaseException:
            connection.close()
            raise
        return connection

    def _read_cached_blocks(
        self, first_block: int, last_block: int
    ) -> dict[int, bytes]:
        """Return cached blocks, marking each returned block as used."""
        rows = self.connection.execute(
            """
            SELECT block_number, data
            FROM cache_blocks
            WHERE object_key = ?
              AND block_size = ?
              AND block_number BETWEEN ? AND ?
            """,
            (
                self.object_key,
                self.cache.block_size,
                first_block,
                last_block,
            ),
        ).fetchall()
        if not rows:
            return {}

        cached_blocks = {
            int(block_number): self._validate_block(
                block_number=int(block_number),
                data=data,
            )
            for block_number, data in rows
        }
        next_last_used = self._next_last_used()
        for block_number in sorted(cached_blocks):
            next_last_used += 1
            self.write_connection.execute(
                """
                UPDATE cache_block_usage
                SET frequency = frequency + 1, last_used = ?
                WHERE object_key = ?
                  AND block_size = ?
                  AND block_number = ?
                """,
                (
                    next_last_used,
                    self.object_key,
                    self.cache.block_size,
                    block_number,
                ),
            )

        return cached_blocks

    def _fetch_blocks(self, first_block: int, last_block: int) -> dict[int, bytes]:
        """Fetch a contiguous missing block range and cache complete blocks."""
        source_start = first_block * self.cache.block_size
        source_end = min(
            (last_block + 1) * self.cache.block_size,
            self.info.size,
        )
        expected_length = source_end - source_start
        fetched_data = self.fetch_range(source_start, source_end)
        if not isinstance(fetched_data, bytes):
            raise TypeError("fetch_range must return bytes")
        if len(fetched_data) != expected_length:
            raise ValueError(
                "fetch_range returned an incorrect number of bytes: "
                f"expected {expected_length}, got {len(fetched_data)}"
            )

        fetched_blocks: dict[int, bytes] = {}
        for block_number in range(first_block, last_block + 1):
            block_start = block_number * self.cache.block_size
            block_end = min(block_start + self.cache.block_size, self.info.size)
            relative_start = block_start - source_start
            relative_end = block_end - source_start
            block_data = fetched_data[relative_start:relative_end]
            fetched_blocks[block_number] = self._validate_block(
                block_number=block_number,
                data=block_data,
            )

        self._store_blocks(blocks=fetched_blocks)
        return fetched_blocks

    def _store_blocks(self, blocks: dict[int, bytes]) -> None:
        """Insert fetched blocks and evict least-used blocks if necessary."""
        if not blocks:
            return

        with _write_transaction(self.write_connection):
            used_bytes = self._used_bytes(self.write_connection)
            next_last_used = self._next_last_used()
            for block_number in sorted(blocks):
                block_data = blocks[block_number]
                if len(block_data) > self.cache.max_size:
                    continue

                candidate_last_used = next_last_used + 1
                insert_cursor = self.write_connection.execute(
                    """
                    INSERT OR IGNORE INTO cache_blocks (
                        object_key,
                        block_size,
                        block_number,
                        data
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (
                        self.object_key,
                        self.cache.block_size,
                        block_number,
                        sqlite3.Binary(block_data),
                    ),
                )
                if insert_cursor.rowcount == 1:
                    next_last_used = candidate_last_used
                    used_bytes += len(block_data)
                    self.write_connection.execute(
                        """
                        INSERT OR IGNORE INTO cache_block_usage (
                            object_key,
                            block_size,
                            block_number,
                            frequency,
                            last_used
                        ) VALUES (?, ?, ?, 1, ?)
                        """,
                        (
                            self.object_key,
                            self.cache.block_size,
                            block_number,
                            candidate_last_used,
                        ),
                    )

            used_bytes = self._evict(used_bytes=used_bytes)
            self.write_connection.execute(
                "UPDATE cache_state SET used_bytes = ? WHERE id = 1",
                (used_bytes,),
            )

    def _enforce_cache_limit(self) -> None:
        """Evict existing blocks when this reader has a smaller size limit."""
        if self._used_bytes(self.connection) <= self.cache.max_size:
            return

        with _write_transaction(self.write_connection):
            used_bytes = self._used_bytes(self.write_connection)
            if used_bytes > self.cache.max_size:
                used_bytes = self._evict(used_bytes=used_bytes)
                self.write_connection.execute(
                    "UPDATE cache_state SET used_bytes = ? WHERE id = 1",
                    (used_bytes,),
                )

    def _evict(self, used_bytes: int) -> int:
        """Evict blocks in LFU order until the configured byte limit is met."""
        over = used_bytes - self.cache.max_size
        if over <= 0:
            return used_bytes

        deleted_rows = self.write_connection.execute(
            """
            WITH eviction_order AS (
                SELECT
                    usage.object_key,
                    usage.block_size,
                    usage.block_number,
                    length(blocks.data) AS data_length,
                    SUM(length(blocks.data)) OVER (
                        ORDER BY usage.frequency, usage.last_used,
                                 usage.object_key, usage.block_size,
                                 usage.block_number
                    ) AS cumulative
                FROM cache_block_usage AS usage
                JOIN cache_blocks AS blocks
                  ON blocks.object_key = usage.object_key
                 AND blocks.block_size = usage.block_size
                 AND blocks.block_number = usage.block_number
            )
            DELETE FROM cache_blocks
            WHERE (object_key, block_size, block_number) IN (
                SELECT object_key, block_size, block_number
                FROM eviction_order
                WHERE cumulative - data_length < ?
            )
            RETURNING object_key, block_size, block_number, length(data)
            """,
            (over,),
        ).fetchall()
        if not deleted_rows:
            return used_bytes

        freed_bytes = 0
        deleted_keys: list[tuple[object, object, object]] = []
        for object_key, block_size, block_number, data_length in deleted_rows:
            freed_bytes += int(data_length)
            deleted_keys.append((object_key, block_size, block_number))
        self.write_connection.executemany(
            """
            DELETE FROM cache_block_usage
            WHERE object_key = ? AND block_size = ? AND block_number = ?
            """,
            deleted_keys,
        )
        return max(used_bytes - freed_bytes, 0)

    def _used_bytes(self, connection: sqlite3.Connection) -> int:
        """Return the tracked number of cached data bytes."""
        row = connection.execute(
            "SELECT used_bytes FROM cache_state WHERE id = 1"
        ).fetchone()
        if row is None:
            raise sqlite3.DatabaseError("Cache state row is missing")
        return int(row[0])

    def _next_last_used(self) -> int:
        """Return and consume the next logical access timestamp."""
        row = self.write_connection.execute(
            """
            UPDATE cache_state
            SET last_used = last_used + 1
            WHERE id = 1
            RETURNING last_used
            """
        ).fetchone()
        if row is None:
            raise sqlite3.DatabaseError("Cache state row is missing")
        return int(row[0])

    def _validate_block(self, block_number: int, data: object) -> bytes:
        """Validate one cached block before it can be returned or stored."""
        if not isinstance(data, bytes):
            raise sqlite3.DatabaseError(
                f"Cached block is not a BLOB: block_number={block_number}"
            )

        expected_length = min(
            self.cache.block_size,
            self.info.size - block_number * self.cache.block_size,
        )
        if expected_length < 0 or len(data) != expected_length:
            raise sqlite3.DatabaseError(
                "Cached block has an incorrect length: "
                f"block_number={block_number}, expected={expected_length}, "
                f"actual={len(data)}"
            )
        return data

    def _assemble(
        self,
        blocks: dict[int, bytes],
        offset: int,
        end: int,
        first_block: int,
        last_block: int,
    ) -> bytes:
        """Assemble the requested slice from complete cached blocks."""
        result = bytearray()
        for block_number in range(first_block, last_block + 1):
            block_data = blocks.get(block_number)
            if block_data is None:
                raise sqlite3.DatabaseError(
                    f"Missing block after fetch: {block_number}"
                )

            block_start = block_number * self.cache.block_size
            slice_start = max(offset, block_start) - block_start
            slice_end = min(end, block_start + len(block_data)) - block_start
            result.extend(block_data[slice_start:slice_end])

        expected_length = end - offset
        if len(result) != expected_length:
            raise sqlite3.DatabaseError(
                "Assembled range has an incorrect length: "
                f"expected={expected_length}, actual={len(result)}"
            )
        return bytes(result)


def _group_adjacent(block_numbers: list[int]) -> Iterator[tuple[int, int]]:
    """Yield inclusive ranges of adjacent block numbers."""
    if not block_numbers:
        return

    range_start = block_numbers[0]
    range_end = range_start
    for block_number in block_numbers[1:]:
        if block_number == range_end + 1:
            range_end = block_number
            continue

        yield range_start, range_end
        range_start = block_number
        range_end = block_number

    yield range_start, range_end
