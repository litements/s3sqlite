# s3sqlite

Query a SQLite database in S3 without downloading the complete object first.

`s3sqlite` is a read-only [APSW](https://rogerbinns.github.io/apsw/) SQLite VFS.
It uses [s3fs](https://s3fs.readthedocs.io/en/latest/) to fetch byte ranges from
S3-compatible object storage and keeps frequently used ranges in a local cache.

## Install

`s3sqlite` requires Python 3.12 or later.

```sh
python3 -m pip install s3sqlite
```

## Query a database from an S3 URI

This is the smallest complete example. Replace the S3 URI and SQL query with
your own values.

```python
import apsw
import s3fs

from s3sqlite.vfs import S3VFS

vfs = S3VFS(name="s3", fs=s3fs.S3FileSystem())

with apsw.Connection(
    "s3://my-bucket/data/analytics.sqlite3",
    vfs=vfs.name,
    flags=apsw.SQLITE_OPEN_READONLY,
) as database:
    rows = database.execute("SELECT * FROM events LIMIT 10").fetchall()

print(rows)
```

`s3fs.S3FileSystem()` uses the normal AWS credential configuration, including
environment variables, shared AWS configuration files, and instance or task
roles. The credentials need permission to read the database object.

Always pass `apsw.SQLITE_OPEN_READONLY`. The VFS does not support modifying a
remote database.

## End-to-end: create, upload, and query

This example creates a local database, uploads it, and queries it through the
VFS.

```python
import sqlite3

import apsw
import s3fs

from s3sqlite.vfs import S3VFS

local_path = "sales.sqlite3"
database_uri = "s3://my-bucket/databases/sales.sqlite3"

# Create and close the local database before uploading it.
with sqlite3.connect(local_path) as database:
    database.execute("PRAGMA journal_mode = DELETE")
    database.execute(
        """
        CREATE TABLE sale (
            id INTEGER PRIMARY KEY,
            country TEXT NOT NULL,
            total REAL NOT NULL
        )
        """
    )
    database.executemany(
        "INSERT INTO sale (country, total) VALUES (?, ?)",
        [
            ("Spain", 12.50),
            ("France", 50.00),
            ("Spain", 25.00),
        ],
    )

filesystem = s3fs.S3FileSystem()
vfs = S3VFS(name="s3", fs=filesystem)
vfs.upload_file(dbfile=local_path, dest=database_uri)

with apsw.Connection(
    database_uri,
    vfs=vfs.name,
    flags=apsw.SQLITE_OPEN_READONLY,
) as database:
    rows = database.execute(
        """
        SELECT country, SUM(total) AS total
        FROM sale
        GROUP BY country
        ORDER BY total DESC
        """
    ).fetchall()

print(rows)
# [('France', 50.0), ('Spain', 37.5)]
```

You can also upload the database with the AWS CLI or any other S3 client.
`upload_file()` is a convenience method, not a required part of querying.

## S3-compatible storage

Pass the endpoint and any provider-specific settings to `S3FileSystem`. This
example uses path-style addressing, which is common with local and self-hosted
S3-compatible services.

```python
import apsw
import s3fs

from s3sqlite.vfs import S3VFS

filesystem = s3fs.S3FileSystem(
    key="access-key",
    secret="secret-key",
    client_kwargs={
        "endpoint_url": "https://objects.example.com",
        "region_name": "us-east-1",
    },
    config_kwargs={"s3": {"addressing_style": "path"}},
)
vfs = S3VFS(name="objects", fs=filesystem)

with apsw.Connection(
    "s3://my-bucket/data.sqlite3",
    vfs=vfs.name,
    flags=apsw.SQLITE_OPEN_READONLY,
) as database:
    for row in database.execute(
        "SELECT timestamp, value FROM measurement ORDER BY timestamp DESC LIMIT 5"
    ):
        print(row)
```

Remove `config_kwargs` when the provider uses virtual-hosted-style addressing.

## Prepare an existing database for upload

The uploaded object must contain the complete database. WAL mode is not
supported because its latest transactions can be stored in a separate `-wal`
file.

If the database currently uses WAL, checkpoint it and change its journal mode
before uploading it:

```python
import sqlite3

with sqlite3.connect("analytics.sqlite3", isolation_level=None) as database:
    database.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    journal_mode = database.execute("PRAGMA journal_mode = DELETE").fetchone()[0]
    assert journal_mode == "delete"

# Upload only after this connection and all other writers are closed.
```

The final journal mode can be `DELETE`, `TRUNCATE`, `PERSIST`, `MEMORY`, or
`OFF`. Do not upload a live database while another process can modify it.

SQLite page sizes from 512 bytes through 65,536 bytes are supported. If you
change the page size, run `VACUUM` after setting `PRAGMA page_size` and before
uploading the database.

## Configure the local cache

By default, each VFS uses a persistent LFU cache with 64 KiB blocks and a
maximum size of 1 GiB. Cached ranges are shared across later connections and
are isolated by the remote object's version or ETag.

```python
from pathlib import Path

import s3fs

from s3sqlite.vfs import S3VFS

vfs = S3VFS(
    name="s3",
    fs=s3fs.S3FileSystem(),
    cache_path=Path("/var/tmp/s3sqlite-cache.sqlite3"),
    cache_size=256 * 1024 * 1024,
    cache_block_size=128 * 1024,
)
```

When `cache_path` is omitted, the cache is stored at:

- `$S3SQLITE_CACHE_HOME/s3sqlite/cache.sqlite3`, when set
- `$XDG_CACHE_HOME/s3sqlite/cache.sqlite3`, when set
- `~/.cache/s3sqlite/cache.sqlite3` on other Unix systems
- `%LOCALAPPDATA%/s3sqlite/cache.sqlite3` on Windows

Choose a block size based on the workload. Larger blocks reduce S3 requests
for scans but download more unused data for sparse queries.

## `S3VFS` options

| Argument           | Default             | Purpose                                       |
| ------------------ | ------------------- | --------------------------------------------- |
| `name`             | Required            | Base name for the registered APSW VFS         |
| `fs`               | Required            | Configured `s3fs.S3FileSystem` instance       |
| `cache`            | `None`              | Custom cache implementation                   |
| `cache_path`       | Platform cache path | SQLite file used by the default cache         |
| `cache_size`       | `1 GiB`             | Maximum cached data size in bytes             |
| `cache_block_size` | `64 KiB`            | Size of each fetched and cached range         |
| `file_kwargs`      | `None`              | Extra keyword arguments passed to `fs.open()` |

The VFS disables the separate `fsspec` file cache. Use `cache_path`,
`cache_size`, and `cache_block_size` to configure caching in `s3sqlite`.

## Limitations

- The VFS is read-only. Upload a new database object to publish changes.
- WAL databases must be checkpointed and changed to another journal mode
  before upload.
- Query performance depends on database layout, query access patterns, S3
  latency, and cache settings.
- Each opened database must remain a valid, unchanged SQLite object for the
  lifetime of its connection.

## Development

Development uses [uv](https://docs.astral.sh/uv/):

```sh
uv sync --group dev
uv run pytest
```

The integration tests start a disposable single-node
[Garage](https://garagehq.deuxfleurs.fr/documentation/) container. They expose
its S3 API on an ephemeral local port and keep its configuration and data in a
temporary directory.

## Alternatives

- [sqlite-s3vfs](https://github.com/uktrade/sqlite-s3vfs) stores each SQLite
  page as a separate object. This supports a single writer without replacing
  the complete database object.
- [sqlite-s3-query](https://github.com/michalc/sqlite-s3-query) also reads a
  SQLite object with range requests. It implements the VFS with `ctypes` and
  uses `httpx` for S3 requests.

## License

Distributed under the Apache 2.0 license. See `LICENSE` for more information.
