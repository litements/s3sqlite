from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import apsw

from s3sqlite.cache import Cache
from s3sqlite.cache import CacheReader
from s3sqlite.cache import FetchRange
from s3sqlite.cache import LFUCache
from s3sqlite.cache import ObjectInfo
from s3sqlite.vfs import S3VFS


@dataclass(frozen=True, slots=True)
class BenchmarkMeasurement:
    """Record one remote page-workload measurement."""

    elapsed_seconds: float
    source_calls: int
    source_bytes: int


@dataclass(frozen=True, slots=True)
class QueryMeasurement:
    """Record one regular VFS SQL-query measurement."""

    rows: list[tuple[Any, ...]]
    elapsed_seconds: float
    source_calls: int
    source_bytes: int


class NoCacheReader:
    """Cache-reader-shaped adapter that fetches every requested range."""

    def __init__(self, info: ObjectInfo, fetch_range: FetchRange) -> None:
        """Create a reader without storing any fetched bytes."""
        self.info = info
        self.fetch_range = fetch_range
        self.closed = False

    def read_at(self, offset: int, length: int) -> bytes:
        """Fetch the requested range directly from the source."""
        if self.closed:
            raise ValueError("Cache reader is closed")
        if offset < 0:
            raise ValueError("offset must be non-negative")
        if length < 0:
            raise ValueError("length must be non-negative")
        if length == 0 or offset >= self.info.size:
            return b""

        end = min(offset + length, self.info.size)
        data = self.fetch_range(offset, end)
        expected_length = end - offset
        if len(data) != expected_length:
            raise ValueError(
                "fetch_range returned an incorrect number of bytes: "
                f"expected {expected_length}, got {len(data)}"
            )
        return data

    def close(self) -> None:
        """Close the no-cache reader."""
        self.closed = True


class NoCache:
    """Cache provider used as the uncached benchmark baseline."""

    def open(self, info: ObjectInfo, fetch_range: FetchRange) -> CacheReader:
        """Create a reader that forwards every read to the source."""
        return NoCacheReader(info=info, fetch_range=fetch_range)


class CountingCache:
    """Wrap a cache provider and record its source range requests."""

    def __init__(self, provider: Cache) -> None:
        """Create a counting wrapper around ``provider``."""
        self.provider = provider
        self.source_ranges: list[tuple[int, int]] = []

    def open(self, info: ObjectInfo, fetch_range: FetchRange) -> CacheReader:
        """Record source ranges before delegating to the wrapped provider."""

        def counting_fetch_range(start: int, end: int) -> bytes:
            """Record and fetch one exact source range."""
            self.source_ranges.append((start, end))
            return fetch_range(start, end)

        return self.provider.open(
            info=info,
            fetch_range=counting_fetch_range,
        )


def create_large_sqlite_database(database_path: Path) -> bytes:
    """Create and return a multi-megabyte SQLite database for benchmarking."""
    connection = sqlite3.connect(database=database_path)
    try:
        connection.execute("PRAGMA page_size = 4096")
        connection.execute("PRAGMA journal_mode = DELETE")
        connection.execute(
            """
            CREATE TABLE hot_records (
                id INTEGER PRIMARY KEY,
                category INTEGER NOT NULL,
                payload BLOB NOT NULL
            )
            """
        )
        hot_payload = bytes(range(256)) * 2
        hot_records = (
            (record_id, record_id % 32, hot_payload) for record_id in range(1, 2049)
        )
        connection.executemany(
            "INSERT INTO hot_records (id, category, payload) VALUES (?, ?, ?)",
            hot_records,
        )
        connection.execute(
            """
            CREATE TABLE cold_records (
                id INTEGER PRIMARY KEY,
                category INTEGER NOT NULL,
                payload BLOB NOT NULL
            )
            """
        )
        cold_payload = bytes(range(256)) * 8
        cold_records = (
            (record_id, record_id % 32, cold_payload) for record_id in range(1, 4097)
        )
        connection.executemany(
            "INSERT INTO cold_records (id, category, payload) VALUES (?, ?, ?)",
            cold_records,
        )
        connection.commit()
    finally:
        connection.close()

    return database_path.read_bytes()


def measure_page_reads(
    vfs: S3VFS,
    database_key: str,
    offsets: list[int],
    expected_data: bytes,
    page_size: int,
    source_ranges: list[tuple[int, int]],
) -> BenchmarkMeasurement:
    """Read SQLite pages and measure elapsed time and source traffic."""
    range_start = len(source_ranges)
    started_at = time.perf_counter()
    vfs_file = vfs.xOpen(
        name=database_key,
        flags=apsw.SQLITE_OPEN_READONLY,
    )
    try:
        for offset in offsets:
            data = vfs_file.xRead(amount=page_size, offset=offset)
            assert data == expected_data[offset : offset + page_size]
    finally:
        vfs_file.xClose()
    elapsed_seconds = time.perf_counter() - started_at

    new_ranges = source_ranges[range_start:]
    source_bytes = sum(end - start for start, end in new_ranges)
    return BenchmarkMeasurement(
        elapsed_seconds=elapsed_seconds,
        source_calls=len(new_ranges),
        source_bytes=source_bytes,
    )


def measure_remote_query(
    vfs: S3VFS,
    database_key: str,
    query: str,
    source_ranges: list[tuple[int, int]],
) -> QueryMeasurement:
    """Run one SQL query through APSW and measure its source traffic."""
    range_start = len(source_ranges)
    started_at = time.perf_counter()
    with apsw.Connection(
        filename=database_key,
        vfs=vfs.name,
        flags=apsw.SQLITE_OPEN_READONLY,
    ) as connection:
        rows = connection.execute(query).fetchall()
    elapsed_seconds = time.perf_counter() - started_at

    new_ranges = source_ranges[range_start:]
    return QueryMeasurement(
        rows=rows,
        elapsed_seconds=elapsed_seconds,
        source_calls=len(new_ranges),
        source_bytes=sum(end - start for start, end in new_ranges),
    )


def test_benchmark_lfu_cache_against_no_cache(
    bucket: str,
    s3_client: Any,
    s3_filesystem: Any,
    tmp_path: Path,
) -> None:
    """Compare partial LFU and uncached reads and SQL queries remotely."""
    database_path = tmp_path / "large.sqlite3"
    database_bytes = create_large_sqlite_database(database_path=database_path)
    assert len(database_bytes) >= 4 * 1024 * 1024

    object_key = "large.sqlite3"
    database_key = f"{bucket}/{object_key}"
    s3_client.upload_file(
        Filename=str(database_path),
        Bucket=bucket,
        Key=object_key,
    )

    page_size = 4096
    cache_block_size = 64 * 1024
    cache_capacity = 4 * 1024 * 1024
    hot_block_count = 32
    cold_block_count = 128
    hot_offsets = [
        block_number * cache_block_size for block_number in range(hot_block_count)
    ]
    hot_warm_offsets = hot_offsets * 8
    cold_offsets = [
        block_number * cache_block_size
        for block_number in range(hot_block_count, hot_block_count + cold_block_count)
    ]
    assert cold_offsets[-1] + page_size <= len(database_bytes)

    no_cache = CountingCache(provider=NoCache())
    no_cache_vfs = S3VFS(
        name="benchmark-no-cache",
        fs=s3_filesystem,
        cache=no_cache,
    )
    no_cache_measurement = measure_page_reads(
        vfs=no_cache_vfs,
        database_key=database_key,
        offsets=hot_warm_offsets + cold_offsets + hot_offsets,
        expected_data=database_bytes,
        page_size=page_size,
        source_ranges=no_cache.source_ranges,
    )

    lfu_cache = LFUCache(
        path=tmp_path / "benchmark-cache.sqlite3",
        max_size=cache_capacity,
        block_size=cache_block_size,
    )
    lfu = CountingCache(provider=lfu_cache)
    lfu_vfs = S3VFS(
        name="benchmark-lfu",
        fs=s3_filesystem,
        cache=lfu,
    )
    lfu_hot_measurement = measure_page_reads(
        vfs=lfu_vfs,
        database_key=database_key,
        offsets=hot_warm_offsets,
        expected_data=database_bytes,
        page_size=page_size,
        source_ranges=lfu.source_ranges,
    )
    lfu_cold_measurement = measure_page_reads(
        vfs=lfu_vfs,
        database_key=database_key,
        offsets=cold_offsets,
        expected_data=database_bytes,
        page_size=page_size,
        source_ranges=lfu.source_ranges,
    )
    lfu_hot_final_measurement = measure_page_reads(
        vfs=lfu_vfs,
        database_key=database_key,
        offsets=hot_offsets,
        expected_data=database_bytes,
        page_size=page_size,
        source_ranges=lfu.source_ranges,
    )

    remote_queries = [
        "SELECT COUNT(*), SUM(length(payload)) FROM hot_records;",
        "SELECT SUM(id), SUM(length(payload)) FROM hot_records WHERE category = 7;",
        "SELECT MIN(id), MAX(id), AVG(category) FROM hot_records WHERE id BETWEEN 257 AND 1792;",
        "SELECT COUNT(*), SUM(length(payload)) FROM hot_records WHERE id % 5 = 0;",
        "SELECT SUM(id * (category + 1)) FROM hot_records WHERE category IN (3, 11, 23);",
        "SELECT COUNT(*) FROM hot_records WHERE id BETWEEN 100 AND 1500;",
        "SELECT COUNT(*), SUM(length(payload)) FROM hot_records WHERE category BETWEEN 4 AND 12;",
        "SELECT SUM(id), SUM(length(payload)) FROM hot_records WHERE id BETWEEN 1024 AND 2048;",
    ]
    assert len(remote_queries) == len(set(remote_queries))
    no_cache_query_measurements = [
        measure_remote_query(
            vfs=no_cache_vfs,
            database_key=database_key,
            query=remote_query,
            source_ranges=no_cache.source_ranges,
        )
        for remote_query in remote_queries
    ]
    lfu_query_measurements = [
        measure_remote_query(
            vfs=lfu_vfs,
            database_key=database_key,
            query=remote_query,
            source_ranges=lfu.source_ranges,
        )
        for remote_query in remote_queries
    ]

    no_cache_query_seconds = sum(
        measurement.elapsed_seconds for measurement in no_cache_query_measurements
    )
    lfu_query_seconds = sum(
        measurement.elapsed_seconds for measurement in lfu_query_measurements
    )
    no_cache_query_calls = sum(
        measurement.source_calls for measurement in no_cache_query_measurements
    )
    lfu_query_calls = sum(
        measurement.source_calls for measurement in lfu_query_measurements
    )
    no_cache_query_bytes = sum(
        measurement.source_bytes for measurement in no_cache_query_measurements
    )
    lfu_query_bytes = sum(
        measurement.source_bytes for measurement in lfu_query_measurements
    )
    lfu_page_bytes = sum(
        measurement.source_bytes
        for measurement in (
            lfu_hot_measurement,
            lfu_cold_measurement,
            lfu_hot_final_measurement,
        )
    )

    lfu_source_calls = sum(
        measurement.source_calls
        for measurement in (
            lfu_hot_measurement,
            lfu_cold_measurement,
            lfu_hot_final_measurement,
        )
    )
    with sqlite3.connect(lfu_cache.path) as connection:
        used_bytes = connection.execute(
            "SELECT used_bytes FROM cache_state WHERE id = 1"
        ).fetchone()[0]

    assert lfu_hot_measurement.source_calls == hot_block_count
    assert lfu_cold_measurement.source_calls == cold_block_count
    assert lfu_hot_final_measurement.source_calls == 0
    assert lfu_source_calls < no_cache_measurement.source_calls
    for no_cache_query, lfu_query in zip(
        no_cache_query_measurements,
        lfu_query_measurements,
        strict=True,
    ):
        assert lfu_query.rows == no_cache_query.rows
    assert lfu_query_calls < no_cache_query_calls
    assert lfu_query_seconds < no_cache_query_seconds
    assert used_bytes <= cache_capacity
    assert used_bytes < len(database_bytes)

    print(
        "partial S3 SQLite benchmark: "
        f"file_bytes={len(database_bytes)} "
        f"cache_capacity={cache_capacity} "
        f"no_cache_calls={no_cache_measurement.source_calls} "
        f"no_cache_bytes={no_cache_measurement.source_bytes} "
        f"lfu_calls={lfu_source_calls} "
        f"lfu_bytes={lfu_page_bytes} "
        f"lfu_final_hot_calls={lfu_hot_final_measurement.source_calls} "
        f"no_cache_seconds={no_cache_measurement.elapsed_seconds:.6f} "
        f"lfu_seconds={sum(measurement.elapsed_seconds for measurement in (lfu_hot_measurement, lfu_cold_measurement, lfu_hot_final_measurement)):.6f} "
        f"remote_queries={len(remote_queries)} "
        f"sql_no_cache_calls={no_cache_query_calls} "
        f"sql_lfu_calls={lfu_query_calls} "
        f"sql_no_cache_bytes={no_cache_query_bytes} "
        f"sql_lfu_bytes={lfu_query_bytes} "
        f"sql_no_cache_seconds={no_cache_query_seconds:.6f} "
        f"sql_lfu_seconds={lfu_query_seconds:.6f}"
    )
