from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import apsw
import pytest

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
            FROM cache_blocks
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
