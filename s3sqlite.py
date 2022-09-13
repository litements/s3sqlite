import apsw
import s3fs
import uuid
import logging
import sys


logger = logging.getLogger("s3sqlite")
handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(
    logging.Formatter(
        fmt="%(levelname)s [%(asctime)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
)
logger.addHandler(handler)


# fmt: off
# SQLite open flags
SQLITE_OPEN_READONLY =         0x00000001 # /* Ok for sqlite3_open_v2() */
SQLITE_OPEN_READWRITE =        0x00000002 # /* Ok for sqlite3_open_v2() */
SQLITE_OPEN_CREATE =           0x00000004 # /* Ok for sqlite3_open_v2() */
SQLITE_OPEN_DELETEONCLOSE =    0x00000008 # /* VFS only */
SQLITE_OPEN_EXCLUSIVE =        0x00000010 # /* VFS only */
SQLITE_OPEN_AUTOPROXY =        0x00000020 # /* VFS only */
SQLITE_OPEN_URI =              0x00000040 # /* Ok for sqlite3_open_v2() */
SQLITE_OPEN_MEMORY =           0x00000080 # /* Ok for sqlite3_open_v2() */
SQLITE_OPEN_MAIN_DB =          0x00000100 # /* VFS only */
SQLITE_OPEN_TEMP_DB =          0x00000200 # /* VFS only */
SQLITE_OPEN_TRANSIENT_DB =     0x00000400 # /* VFS only */
SQLITE_OPEN_MAIN_JOURNAL =     0x00000800 # /* VFS only */
SQLITE_OPEN_TEMP_JOURNAL =     0x00001000 # /* VFS only */
SQLITE_OPEN_SUBJOURNAL =       0x00002000 # /* VFS only */
SQLITE_OPEN_SUPER_JOURNAL =    0x00004000 # /* VFS only */
SQLITE_OPEN_NOMUTEX =          0x00008000 # /* Ok for sqlite3_open_v2() */
SQLITE_OPEN_FULLMUTEX =        0x00010000 # /* Ok for sqlite3_open_v2() */
SQLITE_OPEN_SHAREDCACHE =      0x00020000 # /* Ok for sqlite3_open_v2() */
SQLITE_OPEN_PRIVATECACHE =     0x00040000 # /* Ok for sqlite3_open_v2() */
SQLITE_OPEN_WAL =              0x00080000 # /* VFS only */
SQLITE_OPEN_NOFOLLOW =         0x01000000 # /* Ok for sqlite3_open_v2() 
# fmt: on


def hexify(n):
    padding = 8
    return f"{n:#0{padding}x}"


def convert_flags(flags):
    if isinstance(flags, list):
        return [hexify(f) for f in flags]
    elif isinstance(flags, int):
        return hexify(flags)
    else:
        raise ValueError(flags)


class S3VFS(apsw.VFS):
    def __init__(self, name: str, fs: s3fs.S3FileSystem, block_size=4096):
        self.name = f"{name}-{str(uuid.uuid4())}"
        self.fs = fs
        self._block_size = block_size
        super().__init__(name=self.name, base="")

    def xAccess(self, pathname, flags):
        try:
            with self.fs.open(pathname):
                return True
        except Exception:
            return False

    def xFullPathname(self, filename):
        logger.debug("Calling VFS xFullPathname")
        logger.debug(f"Name: {self.name} fs: {self.fs}")
        logger.debug(filename)
        return filename

    def xDelete(self, filename, syncdir):
        logger.debug("Calling VFS xDelete")
        logger.debug(
            f"Name: {self.name} fs: {self.fs}, filename: {filename}, syncdir: {syncdir}"
        )
        pass

    def xOpen(self, name, flags):
        # TODO: check flags to make sure the DB is openned in read-only mode.
        logger.debug("Calling VFS xOpen")
        fname = name.filename() if isinstance(name, apsw.URIFilename) else name
        logger.debug(
            f"Name: {self.name} fs: {self.fs}, open_name: {fname}, flags: {convert_flags(flags)}"
        )

        ofile = self.fs.open(fname, mode="rb")

        return S3VFSFile(f=ofile, name=fname, flags=flags, block_size=self._block_size)

    def upload_file(self, dbfile, dest):
        self.fs.upload(dbfile, dest)


class S3VFSFile(apsw.VFSFile):
    def __init__(self, f: s3fs.S3File, name, flags, block_size):
        self._block_size = block_size
        self.f = f
        self.flags = flags
        logger.debug(f"Openned AVFSFile with flags: {convert_flags(self.flags)}")
        self.name = name
        self.mode = "rb"

    def xRead(self, amount, offset) -> bytes:
        logger.debug("Calling file xRead")
        logger.debug(
            f"Name: {self.name} file: {self.f.path}, amount: {amount} offset: {offset}"
        )
        self.f.seek(offset)
        data = self.f.read(amount)
        logger.debug(f"Read data: {data}")
        return data

    def xFileControl(self, *args):
        return True

    def xDeviceCharacteristics(self):
        logger.debug("Calling xDeviceCharacteristics")
        return 4096

    def xCheckReservedLock(self):
        return False

    def xLock(self, level):
        pass

    def xUnlock(self, level):
        pass

    def xSectorSize(self):
        return self._block_size

    def xClose(self):
        logger.debug("Calling file xClose")
        logger.debug(f"Name: {self.name} file: {self.f.path}")
        self.f.close()
        pass

    def xFileSize(self):
        logger.debug("Calling file xFileSize")
        logger.debug(f"Name: {self.name} file: {self.f.path}")
        pos = self.f.tell()
        self.f.seek(0, 2)
        size = self.f.tell()
        self.f.seek(pos)
        logger.debug(f"Size: {size}")
        return size

    def xSync(self, flags):
        logger.debug("Calling file xSync")
        logger.debug(
            f"Name: {self.name} file: {self.f.path}, flags: {convert_flags(flags)}"
        )
        pass

    def xTruncate(self, newsize):
        logger.debug("Calling file xTruncate")
        logger.debug(f"Name: {self.name} file: {self.f}, newsize: {newsize}")
        pass

    def xWrite(self, data, offset):
        logger.debug("Calling file xWrite")
        logger.debug(
            f"Name: {self.name} file: {self.f.path}, data_size: {len(data)}, offset: {offset}, data: {data}"
        )
        pass
