# s3sqlite

> Query SQLite databases in S3 using s3fs

[APSW](https://rogerbinns.github.io/apsw/) SQLite VFS. This VFS enables reading
databases from S3. This only supports reading operations, any operation that
tries to modify the DB file is ignored.

Inspired by [sqlite-s3vfs](https://github.com/uktrade/sqlite-s3vfs) and
[sqlite-s3-query](https://github.com/michalc/sqlite-s3-query).

## Example usage

```py
import s3fs
import s3sqlite
import apsw

# Create an S3 filesystem. Check the s3fs docs for more examples:
# https://s3fs.readthedocs.io/en/latest/
s3 = s3fs.S3FileSystem(
    key="somekey",
    secret="secret",
    client_kwargs={"endpoint_url": "http://..."},
)

s3vfs = s3sqlite.AbstractVFS(name="s3-vfs", fs=s3)

# Define the S3 location
key_prefix = "mybucket/awesome.sqlite3"

# Upload the file to S3
s3vfs.upload_file(get_db_wal[0], dest=key_prefix)

# Create a database and query it
with apsw.Connection(
    key_prefix, vfs=s3vfs.name, flags=apsw.SQLITE_OPEN_READONLY
) as conn:

    cursor = conn.execute("...")
    print(cursor.fetchall())

```

## Installation

```
python3 -m pip install s3sqlite
```

## Run tests

The testing script will use the [Chinook
database](https://github.com/lerocha/chinook-database/), it will modify (and
`VACUUM;`) the file to use all the possible combinations of journal modes and
page sizes

1. Download the chinook database:

```sh
curl https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_Sqlite_AutoIncrementPKs.sqlite -o chinook.sqlite3
```

2. Make sure you have Docker installed.

The testing script will start a [MinIO](https://min.io/) container to run the
tests locally. After the tests finish, the container will be stopped
atuomatically.

3. Run the tests:

```sh
python3 -m pytest test.py
```

## Alternatives

- [sqlite-s3vfs](https://github.com/uktrade/sqlite-s3vfs): This VFS stores the
  SQLite file as separate DB pages. This enables having a single writer without
  having to overwrite the whole file.
- [sqlite-s3-query](https://github.com/michalc/sqlite-s3-query): This VFS is very
  similar to s3sqlite, but this uses directly `ctypes` to create the VFS and uses
  `httpx` to make requests to S3.

I decided to create a new VFS that didn't require using `ctypes` so that it's
easier to understand and maintain, but I still want to have a single file in S3
(vs. separate DB pages). At the same time, by using
[s3f3](https://s3fs.readthedocs.io/en/latest/) I know I can use any S3
storage supported by that library.

## Other

The Chinook database used for testing can be obtained from: https://github.com/lerocha/chinook-database/

The testing section in this README contains a command you can run to get the file.

## License

Distributed under the Apache 2.0 license. See `LICENSE` for more information.
