from __future__ import annotations

import os
import sqlite3
import threading
import time
from pathlib import Path
from pathlib import PosixPath
from typing import Any
from typing import cast

import apsw
import pytest

import s3sqlite.cache
from s3sqlite.cache import CacheReader
from s3sqlite.cache import FetchRange
from s3sqlite.cache import LFUCache
from s3sqlite.cache import ObjectInfo
from s3sqlite.cache import default_cache_path
from s3sqlite.vfs import S3VFS


def source_for(data: bytes) -> tuple[list[tuple[int, int]], FetchRange]:
    """Create a range source and record every requested source range."""
    calls: list[tuple[int, int]] = []

    def fetch_range(start: int, end: int) -> bytes:
        """Return one exact range from the test object."""
        calls.append((start, end))
        return data[start:end]

    return calls, fetch_range


def object_info(identity: str = "etag-a", size: int = 10) -> ObjectInfo:
    """Return a small test object description."""
    return ObjectInfo(path="bucket/object", size=size, identity=identity)


def test_default_cache_path_prefers_custom_and_xdg_homes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prefer the package-specific cache home, then XDG's cache home."""
    monkeypatch.setenv("XDG_CACHE_HOME", "/tmp/xdg-cache")
    monkeypatch.setenv("S3SQLITE_CACHE_HOME", "/tmp/s3sqlite-cache")
    assert default_cache_path() == Path("/tmp/s3sqlite-cache/s3sqlite/cache.sqlite3")

    monkeypatch.delenv("S3SQLITE_CACHE_HOME")
    assert default_cache_path() == Path("/tmp/xdg-cache/s3sqlite/cache.sqlite3")


def test_reads_partial_spanning_and_past_end_ranges(tmp_path: Path) -> None:
    """Read exact slices while grouping adjacent missing blocks."""
    data = bytes(range(10))
    calls, fetch_range = source_for(data=data)
    cache = LFUCache(path=tmp_path / "cache.sqlite3", max_size=100, block_size=4)
    reader = cache.open(info=object_info(), fetch_range=fetch_range)

    assert reader.read_at(offset=1, length=2) == data[1:3]
    assert calls == [(0, 4)]
    assert reader.read_at(offset=2, length=7) == data[2:9]
    assert calls == [(0, 4), (4, 10)]
    assert reader.read_at(offset=9, length=10) == data[9:]
    assert reader.read_at(offset=10, length=10) == b""
    reader.close()


def test_empty_and_invalid_ranges(tmp_path: Path) -> None:
    """Handle empty reads without source I/O and reject negative ranges."""
    calls, fetch_range = source_for(data=b"abc")
    cache = LFUCache(path=tmp_path / "cache.sqlite3", block_size=4)
    reader = cache.open(info=object_info(size=3), fetch_range=fetch_range)

    assert reader.read_at(offset=0, length=0) == b""
    assert reader.read_at(offset=4, length=1) == b""
    assert calls == []
    with pytest.raises(ValueError, match="offset"):
        reader.read_at(offset=-1, length=1)
    with pytest.raises(ValueError, match="length"):
        reader.read_at(offset=0, length=-1)
    reader.close()


def test_cache_persists_blocks_across_readers(tmp_path: Path) -> None:
    """Serve a block from SQLite after the first reader is closed."""
    cache_path = tmp_path / "cache.sqlite3"
    data = b"abcdefgh"
    first_calls, first_fetch = source_for(data=data)
    cache = LFUCache(path=cache_path, max_size=100, block_size=4)
    first_reader = cache.open(info=object_info(size=len(data)), fetch_range=first_fetch)
    assert first_reader.read_at(offset=0, length=4) == data[:4]
    first_reader.close()

    second_calls, second_fetch = source_for(data=data)
    second_reader = cache.open(
        info=object_info(size=len(data)),
        fetch_range=second_fetch,
    )
    assert second_reader.read_at(offset=0, length=4) == data[:4]
    second_reader.close()

    assert first_calls == [(0, 4)]
    assert second_calls == []


def test_object_identity_isolates_cached_blocks(tmp_path: Path) -> None:
    """Do not reuse bytes for a changed object validator."""
    cache = LFUCache(path=tmp_path / "cache.sqlite3", block_size=4)
    first_calls, first_fetch = source_for(data=b"old!")
    first_reader = cache.open(
        info=object_info(identity="etag-old", size=4),
        fetch_range=first_fetch,
    )
    assert first_reader.read_at(offset=0, length=4) == b"old!"
    first_reader.close()

    second_calls, second_fetch = source_for(data=b"new!")
    second_reader = cache.open(
        info=object_info(identity="etag-new", size=4),
        fetch_range=second_fetch,
    )
    assert second_reader.read_at(offset=0, length=4) == b"new!"
    second_reader.close()

    assert first_calls == [(0, 4)]
    assert second_calls == [(0, 4)]


def test_failed_fetch_leaves_no_committed_block(tmp_path: Path) -> None:
    """Keep a failed source request out of the persistent cache."""
    cache_path = tmp_path / "cache.sqlite3"
    cache = LFUCache(path=cache_path, block_size=4)

    def fetch_range(start: int, end: int) -> bytes:
        """Raise while fetching the requested test range."""
        del start, end
        raise OSError("source unavailable")

    reader = cache.open(info=object_info(size=4), fetch_range=fetch_range)
    with pytest.raises(OSError, match="source unavailable"):
        reader.read_at(offset=0, length=4)
    reader.close()

    with sqlite3.connect(cache_path) as connection:
        block_count = connection.execute("SELECT COUNT(*) FROM cache_blocks").fetchone()
        used_bytes = connection.execute(
            "SELECT used_bytes FROM cache_state WHERE id = 1"
        ).fetchone()
    assert block_count == (0,)
    assert used_bytes == (0,)


def test_wrong_source_length_leaves_no_committed_block(tmp_path: Path) -> None:
    """Reject a source callback that violates its exact-range contract."""
    cache_path = tmp_path / "cache.sqlite3"
    cache = LFUCache(path=cache_path, block_size=4)
    reader = cache.open(
        info=object_info(size=4),
        fetch_range=lambda start, end: b"bad",
    )

    with pytest.raises(ValueError, match="incorrect number"):
        reader.read_at(offset=0, length=4)
    reader.close()

    with sqlite3.connect(cache_path) as connection:
        block_count = connection.execute("SELECT COUNT(*) FROM cache_blocks").fetchone()
    assert block_count == (0,)


def test_lfu_eviction_prefers_low_frequency_blocks(tmp_path: Path) -> None:
    """Evict the least frequently used block before a hot block."""
    data = b"abcdefghijkl"
    calls, fetch_range = source_for(data=data)
    cache = LFUCache(path=tmp_path / "cache.sqlite3", max_size=8, block_size=4)
    reader = cache.open(info=object_info(size=len(data)), fetch_range=fetch_range)

    assert reader.read_at(offset=0, length=4) == data[:4]
    assert reader.read_at(offset=4, length=4) == data[4:8]
    assert reader.read_at(offset=0, length=1) == data[:1]
    assert reader.read_at(offset=8, length=4) == data[8:]
    reader.close()

    with sqlite3.connect(cache.path) as connection:
        rows = connection.execute(
            """
            SELECT block_number, frequency
            FROM cache_block_usage
            ORDER BY block_number
            """
        ).fetchall()
    assert rows == [(0, 2), (2, 1)]


def test_lfu_eviction_uses_recency_for_frequency_ties(tmp_path: Path) -> None:
    """Evict the older block when candidate frequencies are equal."""
    data = b"abcdefghijkl"
    calls, fetch_range = source_for(data=data)
    cache = LFUCache(path=tmp_path / "cache.sqlite3", max_size=8, block_size=4)
    reader = cache.open(info=object_info(size=len(data)), fetch_range=fetch_range)

    assert reader.read_at(offset=0, length=4) == data[:4]
    assert reader.read_at(offset=4, length=4) == data[4:8]
    assert reader.read_at(offset=8, length=4) == data[8:]
    reader.close()

    with sqlite3.connect(cache.path) as connection:
        rows = connection.execute(
            "SELECT block_number FROM cache_blocks ORDER BY block_number"
        ).fetchall()
    assert rows == [(1,), (2,)]
    assert calls == [(0, 4), (4, 8), (8, 12)]


def test_constructor_validates_arguments(tmp_path: Path) -> None:
    """Reject invalid cache configuration arguments."""
    with pytest.raises(ValueError, match="max_size"):
        LFUCache(path=tmp_path / "cache.sqlite3", max_size=-1)
    with pytest.raises(ValueError, match="block_size"):
        LFUCache(path=tmp_path / "cache.sqlite3", block_size=0)
    with pytest.raises(ValueError, match="block_size"):
        LFUCache(path=tmp_path / "cache.sqlite3", block_size=-4)
    with pytest.raises(ValueError, match="busy_timeout"):
        LFUCache(path=tmp_path / "cache.sqlite3", busy_timeout=-1)


def test_open_rejects_negative_object_size(tmp_path: Path) -> None:
    """Reject objects with a negative recorded size."""
    cache = LFUCache(path=tmp_path / "cache.sqlite3", block_size=4)

    with pytest.raises(ValueError, match="size"):
        cache.open(info=object_info(size=-1), fetch_range=lambda start, end: b"")


def test_read_after_close_raises_and_close_is_idempotent(tmp_path: Path) -> None:
    """Reject reads after close while allowing repeated close calls."""
    cache = LFUCache(path=tmp_path / "cache.sqlite3", block_size=4)
    reader = cache.open(
        info=object_info(size=4),
        fetch_range=lambda start, end: b"abcd",
    )

    assert reader.read_at(offset=0, length=4) == b"abcd"
    reader.close()
    reader.close()

    with pytest.raises(ValueError, match="closed"):
        reader.read_at(offset=0, length=4)


def test_multiple_readers_share_the_cache_concurrently(tmp_path: Path) -> None:
    """Serve correct ranges when separate readers use the cache from many threads.

    This mirrors real usage: each local SQLite connection owns one reader, and
    SQLite serializes its own connection's statements, so different readers
    are the unit of concurrency. SQLite's file locks serialize their cache
    writes instead of an application-level lock.
    """
    data = bytes(range(256)) * 64
    cache = LFUCache(path=tmp_path / "cache.sqlite3", block_size=64)
    readers = [
        cache.open(
            info=object_info(size=len(data)),
            fetch_range=source_for(data=data)[1],
        )
        for _ in range(8)
    ]

    errors: list[BaseException] = []

    def worker(seed: int) -> None:
        reader = readers[seed % len(readers)]
        try:
            for i in range(100):
                offset = (i * 97 + seed * 13) % (len(data) - 1)
                length = 1 + (i * 29 + seed * 7) % 64
                expected = data[offset : offset + length]
                actual = reader.read_at(offset=offset, length=length)
                if actual != expected:
                    raise AssertionError(
                        f"seed={seed} i={i} offset={offset}: "
                        f"expected {len(expected)} bytes, got {len(actual)}"
                    )
        except BaseException as error:
            errors.append(error)

    threads = [threading.Thread(target=worker, args=(seed,)) for seed in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert all(not thread.is_alive() for thread in threads)
    assert errors == []
    for reader in readers:
        reader.close()


def test_reader_created_in_one_thread_can_be_used_from_another(
    tmp_path: Path,
) -> None:
    """Serve reads when SQLite moves a connection between threads."""
    data = b"abcdefgh"
    cache = LFUCache(path=tmp_path / "cache.sqlite3", block_size=4)
    reader = cache.open(
        info=object_info(size=len(data)),
        fetch_range=source_for(data=data)[1],
    )

    results: list[bytes] = []
    thread = threading.Thread(
        target=lambda: results.append(reader.read_at(offset=2, length=4))
    )
    thread.start()
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert results == [data[2:6]]
    reader.close()


def test_close_racing_with_reads_never_corrupts(tmp_path: Path) -> None:
    """Close a reader while another thread keeps reading from it."""
    data = os.urandom(4096)
    _, fetch_range = source_for(data=data)
    cache = LFUCache(path=tmp_path / "cache.sqlite3", block_size=128)
    reader = cache.open(
        info=object_info(size=len(data)),
        fetch_range=fetch_range,
    )

    outcomes: list[str] = []

    def worker() -> None:
        while True:
            try:
                reader.read_at(offset=0, length=128)
            except ValueError as error:
                if "closed" in str(error):
                    outcomes.append("closed")
                    return
                raise

    thread = threading.Thread(target=worker)
    thread.start()
    time.sleep(0.05)
    reader.close()
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert "closed" in outcomes


def test_fetch_range_must_return_bytes(tmp_path: Path) -> None:
    """Reject source callbacks that return non-bytes data."""
    cache = LFUCache(path=tmp_path / "cache.sqlite3", block_size=4)
    fetch_text = cast(FetchRange, lambda start, end: "abcd")
    reader = cache.open(info=object_info(size=4), fetch_range=fetch_text)

    with pytest.raises(TypeError, match="bytes"):
        reader.read_at(offset=0, length=4)


@pytest.mark.parametrize("max_size", [0, 2])
def test_max_size_below_block_size_never_stores_but_serves_reads(
    tmp_path: Path,
    max_size: int,
) -> None:
    """Serve correct reads when the capacity cannot hold one block."""
    data = b"abcdefgh"
    calls, fetch_range = source_for(data=data)
    cache = LFUCache(path=tmp_path / "cache.sqlite3", max_size=max_size, block_size=4)
    reader = cache.open(info=object_info(size=len(data)), fetch_range=fetch_range)

    assert reader.read_at(offset=0, length=len(data)) == data
    assert reader.read_at(offset=1, length=3) == data[1:4]
    reader.close()

    assert calls == [(0, 8), (0, 4)]
    with sqlite3.connect(cache.path) as connection:
        block_count = connection.execute("SELECT COUNT(*) FROM cache_blocks").fetchone()
        used_bytes = connection.execute(
            "SELECT used_bytes FROM cache_state WHERE id = 1"
        ).fetchone()
    assert block_count == (0,)
    assert used_bytes == (0,)


def test_tiny_object_fits_in_one_partial_block(tmp_path: Path) -> None:
    """Cache a partial final block for objects smaller than one block."""
    data = b"abcde"
    calls, fetch_range = source_for(data=data)
    cache = LFUCache(path=tmp_path / "cache.sqlite3", block_size=64 * 1024)
    reader = cache.open(info=object_info(size=len(data)), fetch_range=fetch_range)

    assert reader.read_at(offset=0, length=len(data)) == data
    assert reader.read_at(offset=2, length=3) == data[2:]
    reader.close()

    assert calls == [(0, 5)]


def test_zero_size_object_never_fetches(tmp_path: Path) -> None:
    """Return empty reads for empty objects without source I/O."""
    calls, fetch_range = source_for(data=b"")
    cache = LFUCache(path=tmp_path / "cache.sqlite3", block_size=4)
    reader = cache.open(info=object_info(size=0), fetch_range=fetch_range)

    assert reader.read_at(offset=0, length=10) == b""
    assert reader.read_at(offset=0, length=0) == b""
    reader.close()

    assert calls == []


def test_cache_key_includes_object_size(tmp_path: Path) -> None:
    """Do not reuse cached bytes when the object size changes."""
    cache = LFUCache(path=tmp_path / "cache.sqlite3", block_size=4)
    first_calls, first_fetch = source_for(data=b"abcd")
    first_reader = cache.open(
        info=object_info(identity="same", size=4),
        fetch_range=first_fetch,
    )
    assert first_reader.read_at(offset=0, length=4) == b"abcd"
    first_reader.close()

    second_calls, second_fetch = source_for(data=b"abcdefgh")
    second_reader = cache.open(
        info=object_info(identity="same", size=8),
        fetch_range=second_fetch,
    )
    assert second_reader.read_at(offset=0, length=8) == b"abcdefgh"
    second_reader.close()

    assert first_calls == [(0, 4)]
    assert second_calls == [(0, 8)]


def test_opening_smaller_cache_evicts_shared_blocks(tmp_path: Path) -> None:
    """Evict blocks down to the limit when opening a smaller cache."""
    cache_path = tmp_path / "cache.sqlite3"
    data = b"abcdefghijkl"
    calls, fetch_range = source_for(data=data)
    large_cache = LFUCache(path=cache_path, max_size=100, block_size=4)
    reader = large_cache.open(info=object_info(size=len(data)), fetch_range=fetch_range)
    assert reader.read_at(offset=0, length=len(data)) == data
    reader.close()

    small_cache = LFUCache(path=cache_path, max_size=4, block_size=4)
    small_reader = small_cache.open(
        info=object_info(size=len(data)),
        fetch_range=lambda start, end: b"",
    )
    small_reader.close()

    with sqlite3.connect(cache_path) as connection:
        rows = connection.execute("SELECT block_number FROM cache_blocks").fetchall()
        used_bytes = connection.execute(
            "SELECT used_bytes FROM cache_state WHERE id = 1"
        ).fetchone()
    assert rows == [(2,)]
    assert used_bytes == (4,)


def test_unsupported_schema_version_raises(tmp_path: Path) -> None:
    """Reject cache databases created by a newer schema version."""
    cache_path = tmp_path / "cache.sqlite3"
    LFUCache(path=cache_path, block_size=4)
    with sqlite3.connect(cache_path) as connection:
        connection.execute("PRAGMA user_version = 99")

    with pytest.raises(sqlite3.DatabaseError, match="Unsupported cache schema version"):
        LFUCache(path=cache_path, block_size=4)


def test_corrupt_cache_file_raises(tmp_path: Path) -> None:
    """Reject cache paths that do not contain a SQLite database."""
    cache_path = tmp_path / "cache.sqlite3"
    cache_path.write_bytes(b"this is not a sqlite database")

    with pytest.raises(sqlite3.DatabaseError, match="not a database"):
        LFUCache(path=cache_path, block_size=4)


def test_tampered_block_length_raises(tmp_path: Path) -> None:
    """Reject cached blocks whose stored length is corrupted."""
    cache_path = tmp_path / "cache.sqlite3"
    calls, fetch_range = source_for(data=b"abcd")
    cache = LFUCache(path=cache_path, block_size=4)
    reader = cache.open(info=object_info(size=4), fetch_range=fetch_range)
    assert reader.read_at(offset=0, length=4) == b"abcd"

    with sqlite3.connect(cache_path) as connection:
        connection.execute("UPDATE cache_blocks SET data = zeroblob(5)")

    with pytest.raises(sqlite3.DatabaseError, match="incorrect length"):
        reader.read_at(offset=0, length=4)
    reader.close()


def test_tampered_block_type_raises(tmp_path: Path) -> None:
    """Reject cached blocks stored as non-BLOB SQLite values."""
    cache_path = tmp_path / "cache.sqlite3"
    calls, fetch_range = source_for(data=b"abcd")
    cache = LFUCache(path=cache_path, block_size=4)
    reader = cache.open(info=object_info(size=4), fetch_range=fetch_range)
    assert reader.read_at(offset=0, length=4) == b"abcd"

    with sqlite3.connect(cache_path) as connection:
        connection.execute("UPDATE cache_blocks SET data = 'not-a-blob'")

    with pytest.raises(sqlite3.DatabaseError, match="not a BLOB"):
        reader.read_at(offset=0, length=4)
    reader.close()


def test_missing_cache_state_row_raises(tmp_path: Path) -> None:
    """Reject fetches when the cache accounting row is missing."""
    cache_path = tmp_path / "cache.sqlite3"
    cache = LFUCache(path=cache_path, block_size=4)
    reader = cache.open(
        info=object_info(size=4),
        fetch_range=lambda start, end: b"abcd",
    )

    with sqlite3.connect(cache_path) as connection:
        connection.execute("DELETE FROM cache_state")

    with pytest.raises(sqlite3.DatabaseError, match="Cache state row is missing"):
        reader.read_at(offset=0, length=4)
    reader.close()


def test_default_cache_path_windows_and_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use platform-specific default cache paths when no env home is set."""
    monkeypatch.delenv("S3SQLITE_CACHE_HOME", raising=False)
    monkeypatch.delenv("XDG_CACHE_HOME", raising=False)
    monkeypatch.setenv("HOME", "/tmp/home")
    # ``os.name`` drives both the cache-home branch and pathlib's concrete
    # Path class, so pin pathlib to a constructible class for this platform.
    monkeypatch.setattr(s3sqlite.cache, "Path", PosixPath)

    monkeypatch.setattr(os, "name", "nt")
    monkeypatch.setenv("LOCALAPPDATA", "/tmp/local-app-data")
    assert default_cache_path() == PosixPath(
        "/tmp/local-app-data/s3sqlite/cache.sqlite3"
    )

    monkeypatch.delenv("LOCALAPPDATA")
    assert default_cache_path() == PosixPath(
        "/tmp/home/AppData/Local/s3sqlite/cache.sqlite3"
    )

    monkeypatch.setattr(os, "name", "posix")
    assert default_cache_path() == PosixPath("/tmp/home/.cache/s3sqlite/cache.sqlite3")


def test_two_cache_connections_share_one_database(tmp_path: Path) -> None:
    """Allow separate cache providers to read the same SQLite cache file."""
    cache_path = tmp_path / "cache.sqlite3"
    first_cache = LFUCache(path=cache_path, block_size=4)
    second_cache = LFUCache(path=cache_path, block_size=4)
    first_calls, first_fetch = source_for(data=b"abcd")
    second_calls, second_fetch = source_for(data=b"abcd")
    first_reader = first_cache.open(info=object_info(size=4), fetch_range=first_fetch)
    second_reader = second_cache.open(
        info=object_info(size=4), fetch_range=second_fetch
    )

    assert first_reader.read_at(offset=0, length=4) == b"abcd"
    assert second_reader.read_at(offset=0, length=4) == b"abcd"
    first_reader.close()
    second_reader.close()

    assert first_calls == [(0, 4)]
    assert second_calls == []


class FakeFile:
    """Small seekable file used to test VFS transport wiring."""

    def __init__(self, data: bytes) -> None:
        """Create a seekable file with stable metadata."""
        self.data = data
        self.position = 0
        self.details = {"size": len(data), "ETag": "etag-test"}
        self.closed = False

    def seek(self, offset: int, whence: int = 0) -> int:
        """Move the current file position."""
        if whence == 0:
            self.position = offset
        elif whence == 1:
            self.position += offset
        elif whence == 2:
            self.position = len(self.data) + offset
        else:
            raise ValueError(whence)
        return self.position

    def read(self, length: int = -1) -> bytes:
        """Read bytes from the current file position."""
        if length < 0:
            length = len(self.data) - self.position
        result = self.data[self.position : self.position + length]
        self.position += len(result)
        return result

    def close(self) -> None:
        """Mark the file as closed."""
        self.closed = True

    def __enter__(self) -> FakeFile:
        """Enter the file context manager."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Exit the file context manager."""
        del args
        self.close()


class FakeFilesystem:
    """Filesystem that returns one in-memory file and records open options."""

    def __init__(self, data: bytes) -> None:
        """Create a fake filesystem."""
        self.data = data
        self.open_kwargs: dict[str, Any] | None = None
        self.file: FakeFile | None = None

    def open(self, path: str, mode: str = "rb", **kwargs: Any) -> FakeFile:
        """Open the fake object and record the transport options."""
        del path, mode
        self.open_kwargs = kwargs
        self.file = FakeFile(data=self.data)
        return self.file


class RecordingReader:
    """Cache reader that forwards reads to the source callback."""

    def __init__(self, fetch_range: FetchRange) -> None:
        """Create a source-forwarding reader."""
        self.fetch_range = fetch_range
        self.closed = False

    def read_at(self, offset: int, length: int) -> bytes:
        """Read one range from the configured source callback."""
        return self.fetch_range(offset, offset + length)

    def close(self) -> None:
        """Mark the cache reader as closed."""
        self.closed = True


class RecordingCache:
    """Custom cache provider that records object metadata and callbacks."""

    def __init__(self) -> None:
        """Create an empty recording cache."""
        self.info: ObjectInfo | None = None
        self.reader: RecordingReader | None = None

    def open(self, info: ObjectInfo, fetch_range: FetchRange) -> CacheReader:
        """Record the cache inputs and create a forwarding reader."""
        self.info = info
        self.reader = RecordingReader(fetch_range=fetch_range)
        return self.reader


def test_vfs_wires_custom_cache_and_closes_both_resources() -> None:
    """Use exact source reads while disabling fsspec's independent cache."""
    filesystem = FakeFilesystem(data=b"abcdefgh")
    filesystem_for_vfs: Any = filesystem
    cache = RecordingCache()
    vfs = S3VFS(
        name="test-vfs",
        fs=filesystem_for_vfs,
        cache=cache,
        file_kwargs={"cache_type": "bytes"},
    )

    vfs_file = vfs.xOpen(name="bucket/object", flags=apsw.SQLITE_OPEN_READONLY)
    assert filesystem.open_kwargs == {"cache_type": "none"}
    assert cache.info == ObjectInfo(
        path="bucket/object",
        size=8,
        identity="ETag=etag-test",
    )
    assert vfs_file.xFileSize() == 8
    assert vfs_file.xSectorSize() == 4096
    assert vfs_file.xRead(amount=3, offset=2) == b"cde"
    vfs_file.xClose()
    vfs_file.xClose()

    assert filesystem.file is not None
    assert filesystem.file.closed is True
    assert cache.reader is not None
    assert cache.reader.closed is True
