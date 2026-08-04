"""Read SQLite databases from S3-compatible object storage."""

from __future__ import annotations

import logging
import uuid
from pathlib import Path
from typing import Any

import apsw
import fsspec

logger = logging.getLogger(__name__)


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
        block_size: int = 4096,
        file_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Create a VFS backed by an fsspec filesystem.

        Args:
            name: Base name used to identify this VFS instance.
            fs: Filesystem containing the SQLite object.
            block_size: Read-ahead block size used by the filesystem.
            file_kwargs: Extra keyword arguments passed to ``fs.open``.
        """
        self.name = f"{name}-{uuid.uuid4()}"
        self.fs = fs
        self.block_size = block_size
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

        file_object = self.fs.open(
            filename,
            mode="rb",
            block_size=self.block_size,
            **self.file_kwargs,
        )
        return S3VFSFile(
            file_object=file_object,
            name=filename,
            flags=flags,
        )

    def upload_file(self, dbfile: str | Path, dest: str) -> None:
        """Upload a local SQLite file to the destination object path."""
        self.fs.upload(str(dbfile), dest)


class S3VFSFile(apsw.VFSFile):
    """Read-only APSW file wrapper around an fsspec file object."""

    def __init__(
        self,
        file_object: Any,
        name: str,
        flags: int | list[int],
    ) -> None:
        """Create a VFS file wrapper."""
        self.f = file_object
        self.flags = flags
        self.name = name
        self.mode = "rb"
        logger.debug(
            "Opened VFS file %s with flags %s",
            self.name,
            convert_flags(self.flags),
        )

    def xRead(self, amount: int, offset: int) -> bytes:
        """Read ``amount`` bytes starting at ``offset``."""
        logger.debug(
            "Calling file xRead for %s: amount=%s offset=%s",
            self.name,
            amount,
            offset,
        )
        self.f.seek(offset)
        data = self.f.read(amount)
        logger.debug("Read %s bytes from %s", len(data), self.name)
        return data

    def xFileControl(self, *args: Any) -> bool:
        """Report that unsupported file-control requests are ignored."""
        del args
        return True

    def xDeviceCharacteristics(self) -> int:
        """Return the sector size used by the underlying object file."""
        return 4096

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
        """Return the block size used by the underlying file object."""
        return self.f.block_size

    def xClose(self) -> None:
        """Close the underlying object file."""
        logger.debug("Calling file xClose for %s", self.name)
        self.f.close()

    def xFileSize(self) -> int:
        """Return the size of the underlying object file in bytes."""
        logger.debug("Calling file xFileSize for %s", self.name)
        position = self.f.tell()
        self.f.seek(0, 2)
        size = self.f.tell()
        self.f.seek(position)
        logger.debug("Size of %s is %s bytes", self.name, size)
        return size

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


__all__ = ["S3VFS", "S3VFSFile", "convert_flags", "hexify"]
