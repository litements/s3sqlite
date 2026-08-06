"""Adversarial and edge-case tests for the S3 VFS transport layer."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import apsw
import pytest

from s3sqlite.cache import CacheReader
from s3sqlite.cache import FetchRange
from s3sqlite.cache import ObjectInfo
from s3sqlite.vfs import S3VFS
from s3sqlite.vfs import _object_identity
from s3sqlite.vfs import convert_flags

SAMPLE_QUERIES = [
    "SELECT category, SUM(value) FROM records GROUP BY category ORDER BY category;",
    "SELECT * FROM records ORDER BY id DESC LIMIT 3;",
    "SELECT COUNT(*) FROM records;",
    "SELECT id, category FROM records WHERE category = 'b';",
]


def create_sample_database(database_path: Path) -> None:
    """Create a small SQLite database used by the VFS transport tests."""
    connection = sqlite3.connect(database=database_path)
    try:
        connection.executescript(
            """
            CREATE TABLE records (
                id INTEGER PRIMARY KEY,
                category TEXT NOT NULL,
                value REAL NOT NULL
            );
            INSERT INTO records (id, category, value) VALUES
                (1, 'a', 10.0),
                (2, 'b', 20.0),
                (3, 'a', 30.0);
            """
        )
    finally:
        connection.close()


class FakeFile:
    """Seekable in-memory file with optional object metadata."""

    def __init__(
        self,
        data: bytes,
        details: dict[str, Any] | None = None,
        size: int | None = None,
    ) -> None:
        """Create a file over ``data`` with the given metadata attributes."""
        self.data = data
        self.position = 0
        self.closed = False
        self.details = details
        if size is not None:
            self.size = size

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
    """Serve in-memory object bytes and record every transport call."""

    def __init__(
        self,
        data: bytes,
        details: dict[str, Any] | None = None,
        size: int | None = None,
        info: dict[str, Any] | None = None,
        available_paths: set[str] | None = None,
    ) -> None:
        """Create a filesystem serving ``data`` for the configured paths."""
        self.data = data
        self.details = details
        self.size = size
        self.info_result = info
        self.available_paths = available_paths
        self.open_calls: list[tuple[str, dict[str, Any]]] = []
        self.files: list[FakeFile] = []
        self.delete_calls: list[str] = []

    def open(self, path: str, mode: str = "rb", **kwargs: Any) -> FakeFile:
        """Open the object and record the transport options."""
        del mode
        self.open_calls.append((path, dict(kwargs)))
        if self.available_paths is not None and path not in self.available_paths:
            raise FileNotFoundError(f"no such key: {path}")
        file = FakeFile(data=self.data, details=self.details, size=self.size)
        self.files.append(file)
        return file

    def info(self, path: str) -> dict[str, Any]:
        """Return the configured filesystem-level object metadata."""
        del path
        if self.info_result is None:
            raise FileNotFoundError("no such key")
        return dict(self.info_result)

    def delete(self, path: str) -> None:
        """Record a delete request."""
        self.delete_calls.append(path)


def create_vfs(
    tmp_path: Path,
    filesystem: FakeFilesystem,
    name: str = "test-vfs",
) -> S3VFS:
    """Create an S3 VFS wired to a fake filesystem and a temp cache."""
    filesystem_for_vfs: Any = filesystem
    return S3VFS(
        name=name,
        fs=filesystem_for_vfs,
        cache_path=tmp_path / "cache.sqlite3",
    )


def test_xaccess_reports_existing_and_missing_objects(tmp_path: Path) -> None:
    """Report object existence through the source filesystem."""
    filesystem = FakeFilesystem(
        data=b"x" * 8,
        details={"size": 8, "ETag": "etag-test"},
        available_paths={"bucket/existing.sqlite3"},
    )
    vfs = create_vfs(tmp_path=tmp_path, filesystem=filesystem)

    assert vfs.xAccess(pathname="bucket/existing.sqlite3", flags=0) is True
    assert vfs.xAccess(pathname="bucket/absent.sqlite3", flags=0) is False


def test_xopen_rejects_missing_filename(tmp_path: Path) -> None:
    """Reject open requests without a filename."""
    vfs = create_vfs(tmp_path=tmp_path, filesystem=FakeFilesystem(data=b""))

    with pytest.raises(ValueError, match="filename"):
        vfs.xOpen(name=None, flags=apsw.SQLITE_OPEN_READONLY)


def test_xopen_accepts_uri_filenames(tmp_path: Path) -> None:
    """Strip the URI scheme before opening the object."""
    database_path = tmp_path / "database.sqlite3"
    create_sample_database(database_path=database_path)
    database_bytes = database_path.read_bytes()
    database_key = "bucket/database.sqlite3"
    filesystem = FakeFilesystem(
        data=database_bytes,
        details={"size": len(database_bytes), "ETag": "etag-test"},
        available_paths={database_key},
    )
    vfs = create_vfs(tmp_path=tmp_path, filesystem=filesystem)

    with apsw.Connection(
        filename=f"file:{database_key}",
        vfs=vfs.name,
        flags=apsw.SQLITE_OPEN_READONLY | apsw.SQLITE_OPEN_URI,
    ) as remote_connection:
        rows = remote_connection.execute("SELECT COUNT(*) FROM records").fetchall()

    assert rows == [(3,)]
    assert any(path == database_key for path, _ in filesystem.open_calls)


def test_xopen_forces_transport_cache_off_and_keeps_other_kwargs(
    tmp_path: Path,
) -> None:
    """Override the transport cache while preserving the other open options."""
    filesystem = FakeFilesystem(
        data=b"abcdefgh",
        details={"size": 8, "ETag": "etag-test"},
    )
    file_kwargs = {"cache_type": "bytes", "block_size": 8, "mode": "wb"}
    filesystem_for_vfs: Any = filesystem
    vfs = S3VFS(
        name="test-vfs",
        fs=filesystem_for_vfs,
        cache_path=tmp_path / "cache.sqlite3",
        file_kwargs=file_kwargs,
    )

    vfs.xOpen(name="bucket/object", flags=apsw.SQLITE_OPEN_READONLY)

    assert filesystem.open_calls == [
        ("bucket/object", {"cache_type": "none", "block_size": 8})
    ]
    assert file_kwargs == {"cache_type": "bytes", "block_size": 8, "mode": "wb"}


def test_xopen_propagates_source_open_failure(tmp_path: Path) -> None:
    """Propagate source open failures for missing objects."""
    filesystem = FakeFilesystem(
        data=b"x",
        details={"size": 1, "ETag": "etag-test"},
        available_paths=set(),
    )
    vfs = create_vfs(tmp_path=tmp_path, filesystem=filesystem)

    with pytest.raises(FileNotFoundError, match="no such key"):
        vfs.xOpen(name="bucket/absent", flags=apsw.SQLITE_OPEN_READONLY)


def test_xopen_closes_source_file_when_metadata_is_missing(tmp_path: Path) -> None:
    """Close the source file when object metadata cannot be determined."""
    filesystem = FakeFilesystem(data=b"abcdefgh", info={"ETag": "etag-test"})
    vfs = create_vfs(tmp_path=tmp_path, filesystem=filesystem)

    with pytest.raises(ValueError, match="No object size"):
        vfs.xOpen(name="bucket/object", flags=apsw.SQLITE_OPEN_READONLY)

    assert filesystem.files[0].closed is True


@pytest.mark.parametrize(
    ("details", "size", "info", "match"),
    [
        (None, None, {"ETag": "etag-test"}, "No object size"),
        ({"size": "abc"}, None, None, "Invalid object size"),
        ({"size": -1}, None, None, "Invalid object size"),
        (None, None, {"size": "abc"}, "Invalid object size"),
        (None, None, {"size": -5}, "Invalid object size"),
    ],
)
def test_xopen_rejects_invalid_object_sizes(
    tmp_path: Path,
    details: dict[str, Any] | None,
    size: int | None,
    info: dict[str, Any] | None,
    match: str,
) -> None:
    """Reject objects with missing, non-numeric, or negative sizes."""
    filesystem = FakeFilesystem(
        data=b"abcdefgh",
        details=details,
        size=size,
        info=info,
    )
    vfs = create_vfs(tmp_path=tmp_path, filesystem=filesystem)

    with pytest.raises(ValueError, match=match):
        vfs.xOpen(name="bucket/object", flags=apsw.SQLITE_OPEN_READONLY)


def test_xopen_falls_back_to_filesystem_info_and_prefers_file_details(
    tmp_path: Path,
) -> None:
    """Use file metadata first and filesystem info as a fallback."""
    filesystem = FakeFilesystem(
        data=b"abcdefgh",
        details={"size": 10, "ETag": "file-etag"},
        info={"size": 6, "ETag": "info-etag"},
    )
    vfs = create_vfs(tmp_path=tmp_path, filesystem=filesystem)

    vfs_file = vfs.xOpen(name="bucket/object", flags=apsw.SQLITE_OPEN_READONLY)
    assert vfs_file.xFileSize() == 10

    fallback = FakeFilesystem(
        data=b"abcdefgh",
        info={"size": 6, "ETag": "info-etag"},
    )
    fallback_vfs = create_vfs(
        tmp_path=tmp_path,
        filesystem=fallback,
        name="fallback-vfs",
    )
    fallback_file = fallback_vfs.xOpen(
        name="bucket/object",
        flags=apsw.SQLITE_OPEN_READONLY,
    )
    assert fallback_file.xFileSize() == 6


@pytest.mark.parametrize(
    ("metadata", "expected"),
    [
        ({"size": 1, "VersionId": "v", "ETag": "e"}, "VersionId=v"),
        ({"size": 1, "version_id": "v", "etag": "e"}, "version_id=v"),
        ({"size": 1, "ETag": "e", "e_tag": "x"}, "ETag=e"),
        ({"size": 1, "etag": "e"}, "etag=e"),
        ({"size": 1, "e_tag": "x"}, "e_tag=x"),
        ({"size": 1, "LastModified": "when"}, '{"LastModified":"when","size":1}'),
        ({"size": 1, "blob": b"\x00"}, '{"blob":"b\'\\\\x00\'","size":1}'),
        ({}, "size=1"),
    ],
)
def test_object_identity_precedence_and_fallbacks(
    metadata: dict[str, Any],
    expected: str,
) -> None:
    """Choose the strongest validator and serialize the fallbacks."""
    assert _object_identity(metadata=metadata, size=1) == expected


def test_xdelete_is_ignored(tmp_path: Path) -> None:
    """Ignore delete requests because the VFS is read-only."""
    filesystem = FakeFilesystem(data=b"x", details={"size": 1, "ETag": "e"})
    vfs = create_vfs(tmp_path=tmp_path, filesystem=filesystem)

    assert vfs.xDelete(filename="bucket/object", syncdir=False) is None  # type: ignore[func-returns-value]
    assert vfs.xDelete(filename="bucket/object", syncdir=True) is None  # type: ignore[func-returns-value]
    assert filesystem.delete_calls == []


def test_xfullpathname_passthrough(tmp_path: Path) -> None:
    """Return the object path unchanged."""
    vfs = create_vfs(tmp_path=tmp_path, filesystem=FakeFilesystem(data=b"x"))

    assert vfs.xFullPathname(filename="bucket/object") == "bucket/object"


def test_readonly_file_methods_return_fixed_values_and_ignore_requests(
    tmp_path: Path,
) -> None:
    """Expose read-only characteristics and ignore modification requests."""
    filesystem = FakeFilesystem(
        data=b"abcdefgh",
        details={"size": 8, "ETag": "etag-test"},
    )
    vfs = create_vfs(tmp_path=tmp_path, filesystem=filesystem)
    vfs_file = vfs.xOpen(name="bucket/object", flags=apsw.SQLITE_OPEN_READONLY)

    assert vfs_file.xFileControl() is True
    assert vfs_file.xDeviceCharacteristics() == 4096
    assert vfs_file.xCheckReservedLock() is False
    assert vfs_file.xSectorSize() == 4096
    assert vfs_file.xFileSize() == 8
    assert vfs_file.xRead(amount=0, offset=0) == b""
    vfs_file.xLock(level=4)
    vfs_file.xUnlock(level=0)
    vfs_file.xSync(flags=0)
    vfs_file.xTruncate(newsize=0)
    vfs_file.xWrite(data=b"junk", offset=0)
    vfs_file.xClose()

    assert filesystem.files[0].closed is True


def test_read_after_close_raises_and_close_is_idempotent(tmp_path: Path) -> None:
    """Reject reads after close while allowing repeated close calls."""
    filesystem = FakeFilesystem(
        data=b"abcdefgh",
        details={"size": 8, "ETag": "etag-test"},
    )
    vfs = create_vfs(tmp_path=tmp_path, filesystem=filesystem)
    vfs_file = vfs.xOpen(name="bucket/object", flags=apsw.SQLITE_OPEN_READONLY)

    vfs_file.xClose()
    vfs_file.xClose()

    with pytest.raises(ValueError, match="closed"):
        vfs_file.xRead(amount=4, offset=0)


def test_xopen_closes_source_file_when_cache_fails(tmp_path: Path) -> None:
    """Close the source file when the cache provider fails to open."""

    class FailingCache:
        """Cache provider that always fails when opening a reader."""

        def open(self, info: ObjectInfo, fetch_range: FetchRange) -> CacheReader:
            """Fail immediately."""
            del info, fetch_range
            raise RuntimeError("cache failure")

    filesystem = FakeFilesystem(data=b"x", details={"size": 1, "ETag": "e"})
    filesystem_for_vfs: Any = filesystem
    vfs = S3VFS(
        name="test-vfs",
        fs=filesystem_for_vfs,
        cache=FailingCache(),
    )

    with pytest.raises(RuntimeError, match="cache failure"):
        vfs.xOpen(name="bucket/object", flags=apsw.SQLITE_OPEN_READONLY)

    assert filesystem.files[0].closed is True


def test_vfs_names_are_unique(tmp_path: Path) -> None:
    """Create distinct APSW VFS names for equal constructor names."""
    first = create_vfs(
        tmp_path=tmp_path,
        filesystem=FakeFilesystem(data=b"x"),
        name="shared",
    )
    second = create_vfs(
        tmp_path=tmp_path,
        filesystem=FakeFilesystem(data=b"x"),
        name="shared",
    )

    assert first.name != second.name
    assert first.name.startswith("shared-")


@pytest.mark.parametrize("value", ["x", 1.5, None])
def test_convert_flags_rejects_unsupported_types(value: Any) -> None:
    """Reject flag values that are neither integers nor lists."""
    with pytest.raises(ValueError):
        convert_flags(value)


def test_query_through_fake_filesystem_matches_local_sqlite(tmp_path: Path) -> None:
    """Run real SQL queries through the VFS over a fake object source."""
    database_path = tmp_path / "database.sqlite3"
    create_sample_database(database_path=database_path)
    database_bytes = database_path.read_bytes()
    database_key = "bucket/database.sqlite3"
    filesystem = FakeFilesystem(
        data=database_bytes,
        details={"size": len(database_bytes), "ETag": "etag-test"},
        available_paths={database_key},
    )
    vfs = create_vfs(tmp_path=tmp_path, filesystem=filesystem)

    with sqlite3.connect(database=database_path) as local_connection:
        for query in SAMPLE_QUERIES:
            expected_rows = local_connection.execute(query).fetchall()
            with apsw.Connection(
                filename=database_key,
                vfs=vfs.name,
                flags=apsw.SQLITE_OPEN_READONLY,
            ) as remote_connection:
                actual_rows = remote_connection.execute(query).fetchall()
            assert actual_rows == expected_rows


def test_write_attempt_raises_and_leaves_source_unchanged(tmp_path: Path) -> None:
    """Reject write attempts and leave the source object untouched."""
    database_path = tmp_path / "database.sqlite3"
    create_sample_database(database_path=database_path)
    database_bytes = database_path.read_bytes()
    database_key = "bucket/database.sqlite3"
    filesystem = FakeFilesystem(
        data=database_bytes,
        details={"size": len(database_bytes), "ETag": "etag-test"},
        available_paths={database_key},
    )
    vfs = create_vfs(tmp_path=tmp_path, filesystem=filesystem)

    with apsw.Connection(
        filename=database_key,
        vfs=vfs.name,
        flags=apsw.SQLITE_OPEN_READWRITE,
    ) as remote_connection:
        with pytest.raises(FileNotFoundError, match="journal"):
            remote_connection.execute(
                "INSERT INTO records (category, value) VALUES ('c', 40.0)"
            )

    assert filesystem.data == database_bytes

    with apsw.Connection(
        filename=database_key,
        vfs=vfs.name,
        flags=apsw.SQLITE_OPEN_READONLY,
    ) as remote_connection:
        rows = remote_connection.execute(
            "SELECT category FROM records ORDER BY id"
        ).fetchall()
    assert rows == [("a",), ("b",), ("a",)]
