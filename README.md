# s3sqlite

> Query SQLite databases in S3 using s3fs

[APSW](https://rogerbinns.github.io/apsw/) SQLite VFS. This VFS enables reading
databases from S3 using
[s3fs](https://s3fs.readthedocs.io/en/latest/index.html). This only supports
reading operations, any operation that tries to modify the DB file is ignored.

Inspired by [sqlite-s3vfs](https://github.com/uktrade/sqlite-s3vfs) and
[sqlite-s3-query](https://github.com/michalc/sqlite-s3-query).

## Notes about journal mode

This VFS will only work when the DB file is in any journal mode that is **not**
[WAL](https://sqlite.org/wal.html). However, it will work if you set the journal
mode to something else just before uploading the file to S3. You can (and
probably should) use WAL mode to generate the DB. Then you can change the
journal mode (and the page size if you neeed) before uploading it to S3.

The test suite
[includes](https://github.com/litements/s3sqlite/blob/3719f1ce50a7b5cfae754776bc9b2c17292f8d72/test.py#L198)
tests for that use case. Take into account that the page size can't be changed
when the database is in WAL mode. You need to change it before setting the WAL
mode or by setting the database to rollback journal mode. [You need to execute
`VACUUM;` after changing the page
size](https://www.sqlite.org/pragma.html#pragma_page_size) in a SQLite database.

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

s3vfs = s3sqlite.S3VFS(name="s3-vfs", fs=s3)

# Define the S3 location
key_prefix = "mybucket/awesome.sqlite3"

# Upload the file to S3
s3vfs.upload_file("awesome.sqlite3", dest=key_prefix)

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
  having to overwrite the whole file. `s3sqlite`'s main difference is that this
  just needs uploading a single file to S3. `sqlite-s3vfs` will split the
  database in pages and upload the pages separately to a bucket prefix. Having
  just a single file has some advantages, like making use of object [versioning
  in the
  bucket](https://s3fs.readthedocs.io/en/latest/index.html?highlight=version#bucket-version-awareness).
  I also think that relying on
  [s3fs](https://s3fs.readthedocs.io/en/latest/index.html) makes the VFS more
  [flexible](https://s3fs.readthedocs.io/en/latest/index.html#s3-compatible-storage)
  than calling `boto3` as `sqlite3-s3vfs` does. `s3fs` should also handle
  retries automatically.
- [sqlite-s3-query](https://github.com/michalc/sqlite-s3-query): This VFS is very
  similar to `s3sqlit`, but it uses `ctypes` directly to create the VFS and uses
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
