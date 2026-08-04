"""APSW VFS integration for reading SQLite databases from object storage."""

from __future__ import annotations

import json
import logging
import threading
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import apsw
import fsspec

from s3sqlite.cache import DEFAULT_BLOCK_SIZE
from s3sqlite.cache import DEFAULT_MAX_SIZE
from s3sqlite.cache import Cache
from s3sqlite.cache import CacheReader
from s3sqlite.cache import LFUCache
from s3sqlite.cache import ObjectInfo

logger = logging.getLogger(__name__)
SQLITE_SECTOR_SIZE = 4096


# SQLite open flags.
SQLITE_OPEN_READONLY = 0x00000001
SQLITE_OPEN_READWRITE = 0x00000002
SQLITE_OPEN_CREATE = 0x00000004
SQLITE_OPEN_DELETEONCLOSE = 0x00000008
SQLITE_OPEN_EXCLUSIVE = 0x00000010
SQLITE_OPEN_AUTOPROXY = 0x00000020
SQLITE_OPEN_URI = 0x00000040
SQLITE_OPEN_MEMORY = 0x00000080
SQLITE_OPEN_MAIN_DB = 0x00000100
SQLITE_OPEN_TEMP_DB = 0x00000200
SQLITE_OPEN_TRANSIENT_DB = 0x00000400
SQLITE_OPEN_MAIN_JOURNAL = 0x00000800
SQLITE_OPEN_TEMP_JOURNAL = 0x00001000
SQLITE_OPEN_SUBJOURNAL = 0x00002000
SQLITE_OPEN_SUPER_JOURNAL = 0x00004000
SQLITE_OPEN_NOMUTEX = 0x00008000
SQLITE_OPEN_FULLMUTEX = 0x00010000
SQLITE_OPEN_SHAREDCACHE = 0x00020000
SQLITE_OPEN_PRIVATECACHE = 0x00040000
SQLITE_OPEN_WAL = 0x00080000
SQLITE_OPEN_NOFOLLOW = 0x01000000


def hexify(number: int) -> str:
    """Format a SQLite flag as a zero-padded hexadecimal value."""
    padding = 8
    return f"{number:#0{padding}x}"


def convert_flags(flags: int | list[int]) -> str | list[str]:
    """Format one or more SQLite flags for diagnostic logging."""
    if isinstance(flags, list):
        return [hexify(flag) for flag in flags]

    if isinstance(flags, int):
        return hexify(flags)

    raise ValueError(flags)


class S3VFS(apsw.VFS):
    """APSW VFS that reads a SQLite database by byte ranges from object storage."""

    def __init__(
        self,
        name: str,
        fs: fsspec.AbstractFileSystem,
        cache: Cache | None = None,
        cache_path: str | Path | None = None,
        cache_size: int = DEFAULT_MAX_SIZE,
        cache_block_size: int = DEFAULT_BLOCK_SIZE,
        file_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Create a VFS backed by an fsspec filesystem.

        Args:
            name: Base name used to identify this VFS instance.
            fs: Filesystem containing the SQLite object.
            cache: Custom cache provider, or a disk-backed LFU cache by default.
            cache_path: SQLite path used by the default cache.
            cache_size: Maximum size of the default cache in bytes.
            cache_block_size: Remote block size used by the default cache.
            file_kwargs: Extra keyword arguments passed to ``fs.open``.
        """
        self.name = f"{name}-{uuid.uuid4()}"
        self.fs = fs
        self.cache = (
            cache
            if cache is not None
            else LFUCache(
                path=cache_path,
                max_size=cache_size,
                block_size=cache_block_size,
            )
        )
        self.file_kwargs = dict(file_kwargs) if file_kwargs is not None else {}
        super().__init__(name=self.name, base="")

    def xAccess(self, pathname: str, flags: int) -> bool:
        """Return whether an object can be opened for reading."""
        del flags
        try:
            with self.fs.open(pathname, mode="rb"):
                return True
        except Exception:
            return False

    def xFullPathname(self, filename: str) -> str:
        """Return the object path unchanged."""
        logger.debug("Calling VFS xFullPathname for %s", filename)
        return filename

    def xDelete(self, filename: str, syncdir: bool) -> None:
        """Ignore delete requests because this VFS is read-only."""
        logger.debug("Ignoring VFS xDelete for %s (syncdir=%s)", filename, syncdir)

    def xOpen(
        self,
        name: str | apsw.URIFilename | None,
        flags: int | list[int],
    ) -> S3VFSFile:
        """Open an object as an APSW VFS file."""
        if name is None:
            raise ValueError("APSW did not provide a filename")

        filename = name.filename() if isinstance(name, apsw.URIFilename) else name
        logger.debug(
            "Calling VFS xOpen for %s with flags %s",
            filename,
            convert_flags(flags),
        )

        open_kwargs = dict(self.file_kwargs)
        open_kwargs.pop("mode", None)
        open_kwargs["cache_type"] = "none"
        file_object = self.fs.open(filename, mode="rb", **open_kwargs)
        source_lock = threading.Lock()

        def fetch_range(start: int, end: int) -> bytes:
            """Read one exact half-open range from the serialized source file."""
            if start < 0 or end < start:
                raise ValueError("Invalid source range")

            amount = end - start
            with source_lock:
                file_object.seek(start)
                data = file_object.read(amount)
            if not isinstance(data, bytes):
                raise TypeError("The fsspec source must return bytes")
            if len(data) != amount:
                raise OSError(
                    "The fsspec source returned an incomplete range: "
                    f"expected {amount}, got {len(data)}"
                )
            return data

        try:
            object_info = _object_info(
                filesystem=self.fs,
                file_object=file_object,
                filename=filename,
            )
            cache_reader = self.cache.open(
                info=object_info,
                fetch_range=fetch_range,
            )
        except BaseException:
            file_object.close()
            raise

        return S3VFSFile(
            file_object=file_object,
            cache_reader=cache_reader,
            name=filename,
            flags=flags,
            object_info=object_info,
        )

    def upload_file(self, dbfile: str | Path, dest: str) -> None:
        """Upload a local SQLite file to the destination object path."""
        self.fs.upload(str(dbfile), dest)


class S3VFSFile(apsw.VFSFile):
    """Read-only APSW file wrapper around a cached fsspec file object."""

    def __init__(
        self,
        file_object: Any,
        cache_reader: CacheReader,
        name: str,
        flags: int | list[int],
        object_info: ObjectInfo,
    ) -> None:
        """Create a VFS file wrapper with ownership of both resources."""
        self.f = file_object
        self.cache_reader = cache_reader
        self.flags = flags
        self.name = name
        self.object_info = object_info
        self.mode = "rb"
        self.closed = False
        logger.debug(
            "Opened VFS file %s with flags %s",
            self.name,
            convert_flags(self.flags),
        )

    def xRead(self, amount: int, offset: int) -> bytes:
        """Read ``amount`` bytes starting at ``offset`` from the cache."""
        logger.debug(
            "Calling file xRead for %s: amount=%s offset=%s",
            self.name,
            amount,
            offset,
        )
        data = self.cache_reader.read_at(offset=offset, length=amount)
        logger.debug("Read %s bytes from %s", len(data), self.name)
        return data

    def xFileControl(self, *args: Any) -> bool:
        """Report that unsupported file-control requests are ignored."""
        del args
        return True

    def xDeviceCharacteristics(self) -> int:
        """Return the sector size used by the underlying object file."""
        return SQLITE_SECTOR_SIZE

    def xCheckReservedLock(self) -> bool:
        """Report that no write lock is held."""
        return False

    def xLock(self, level: int) -> None:
        """Ignore lock requests for this read-only VFS."""
        del level

    def xUnlock(self, level: int) -> None:
        """Ignore unlock requests for this read-only VFS."""
        del level

    def xSectorSize(self) -> int:
        """Return SQLite's fixed sector size."""
        return SQLITE_SECTOR_SIZE

    def xClose(self) -> None:
        """Close the cache reader and the underlying object file."""
        logger.debug("Calling file xClose for %s", self.name)
        if self.closed:
            return

        self.closed = True
        try:
            self.cache_reader.close()
        finally:
            self.f.close()

    def xFileSize(self) -> int:
        """Return the size recorded in the remote object metadata."""
        logger.debug("Calling file xFileSize for %s", self.name)
        return self.object_info.size

    def xSync(self, flags: int | list[int]) -> None:
        """Ignore sync requests because the VFS cannot write objects."""
        logger.debug("Ignoring file xSync for %s with flags %s", self.name, flags)

    def xTruncate(self, newsize: int) -> None:
        """Ignore truncate requests because the VFS is read-only."""
        logger.debug("Ignoring file xTruncate for %s at size %s", self.name, newsize)

    def xWrite(self, data: Any, offset: int) -> None:
        """Ignore write requests because the VFS is read-only."""
        logger.debug(
            "Ignoring file xWrite for %s: data_size=%s offset=%s",
            self.name,
            len(data),
            offset,
        )


def _object_info(
    filesystem: fsspec.AbstractFileSystem,
    file_object: Any,
    filename: str,
) -> ObjectInfo:
    """Build object metadata from the opened fsspec file and filesystem."""
    details = getattr(file_object, "details", None)
    if isinstance(details, Mapping):
        metadata = dict(details)
    else:
        metadata = {}

    file_size = getattr(file_object, "size", None)
    if "size" not in metadata and file_size is not None:
        metadata["size"] = file_size

    if "size" not in metadata:
        filesystem_details = filesystem.info(filename)
        if isinstance(filesystem_details, Mapping):
            metadata = {**dict(filesystem_details), **metadata}

    size_value = metadata.get("size")
    if size_value is None:
        raise ValueError(f"No object size was provided for {filename}")
    try:
        size = int(size_value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"Invalid object size for {filename}: {size_value}") from error
    if size < 0:
        raise ValueError(f"Invalid object size for {filename}: {size}")

    identity = _object_identity(metadata=metadata, size=size)
    return ObjectInfo(path=filename, size=size, identity=identity)


def _object_identity(metadata: Mapping[str, Any], size: int) -> str:
    """Select a stable validator or serialize available fallback metadata."""
    identity_keys = (
        "VersionId",
        "version_id",
        "ETag",
        "etag",
        "e_tag",
    )
    for key in identity_keys:
        value = metadata.get(key)
        if value is not None:
            return f"{key}={value}"

    if metadata:
        serialized_metadata = json.dumps(
            dict(metadata),
            default=str,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        return serialized_metadata

    return f"size={size}"
