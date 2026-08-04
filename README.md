# s3sqlite

> Query SQLite databases in S3-compatible object storage using s3fs

[APSW](https://rogerbinns.github.io/apsw/) SQLite VFS. This VFS enables reading
databases from S3-compatible object storage using
[s3fs](https://s3fs.readthedocs.io/en/latest/index.html). It only supports
reading operations; operations that try to modify the database file are
ignored.

Inspired by [sqlite-s3vfs](https://github.com/uktrade/sqlite-s3vfs) and
[sqlite-s3-query](https://github.com/michalc/sqlite-s3-query).

## Notes about journal mode

This VFS works with database files in any journal mode that is **not**
[WAL](https://sqlite.org/wal.html). A database can use WAL while it is being
generated, but its journal mode must be changed before the file is uploaded to
object storage.

The test suite covers that workflow. The page size cannot be changed while a
database is in WAL mode; change it before enabling WAL or after switching back
to a rollback journal mode. Run `VACUUM;` after changing the page size.

## Example usage

```py
import apsw
import s3fs

from s3sqlite.vfs import S3VFS

s3 = s3fs.S3FileSystem(
    key="somekey",
    secret="secret",
    client_kwargs={
        "endpoint_url": "http://...",
        "region_name": "garage",
    },
    config_kwargs={"s3": {"addressing_style": "path"}},
)

s3vfs = S3VFS(name="s3-vfs", fs=s3)
database_key = "mybucket/awesome.sqlite3"

s3vfs.upload_file(dbfile="awesome.sqlite3", dest=database_key)

with apsw.Connection(
    filename=database_key,
    vfs=s3vfs.name,
    flags=apsw.SQLITE_OPEN_READONLY,
) as connection:
    cursor = connection.execute("SELECT * FROM records")
    print(cursor.fetchall())
```

## Installation

```sh
python3 -m pip install s3sqlite
```

Development uses [uv](https://docs.astral.sh/uv/) and keeps its environment in
the project:

```sh
uv sync --group dev
uv run pytest
```

## Local Garage testing

The integration tests start and stop a disposable single-node
[Garage](https://garagehq.deuxfleurs.fr/documentation/) container and expose
its S3 API through an ephemeral local port. This setup is intended only for
local testing and has no data redundancy. The test fixture writes Garage's
configuration and data to a temporary directory, so no local service setup or
cleanup is required.

## Alternatives

- [sqlite-s3vfs](https://github.com/uktrade/sqlite-s3vfs): This VFS stores the
  SQLite file as separate database pages. That enables a single writer without
  overwriting the whole file. `s3sqlite` uploads one complete file to an S3
  object, which also makes object versioning available through the storage
  provider.
- [sqlite-s3-query](https://github.com/michalc/sqlite-s3-query): This VFS is
  similar to `s3sqlite`, but it uses `ctypes` to create the VFS and `httpx` for
  S3 requests.

## License

Distributed under the Apache 2.0 license. See `LICENSE` for more information.
