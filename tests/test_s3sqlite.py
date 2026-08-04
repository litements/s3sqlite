from __future__ import annotations

import itertools
import sqlite3
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import apsw
import boto3
import pytest
import s3fs
from botocore.config import Config
from testcontainers.core.container import DockerContainer

import s3sqlite

PAGE_SIZES = [512, 1024, 2048, 4096, 8192, 16384, 32768, 65536]
JOURNAL_MODES = ["DELETE", "TRUNCATE", "PERSIST", "MEMORY", "OFF"]
JOURNAL_CASES = list(itertools.product(PAGE_SIZES, JOURNAL_MODES))

S3_REGION = "garage"
S3_ACCESS_KEY = "GK00000000000000000000000000000000"
S3_SECRET_KEY = "0000000000000000000000000000000000000000000000000000000000000000"
GARAGE_DEFAULT_BUCKET = "s3sqlite-default"
GARAGE_CONFIG = """
metadata_dir = "/var/lib/garage/meta"
data_dir = "/var/lib/garage/data"
db_engine = "sqlite"

replication_factor = 1

rpc_bind_addr = "[::]:3901"
rpc_public_addr = "127.0.0.1:3901"
rpc_secret = "0000000000000000000000000000000000000000000000000000000000000000"

[s3_api]
s3_region = "garage"
api_bind_addr = "[::]:3900"
root_domain = ".s3.garage.localhost"

[s3_web]
bind_addr = "[::]:3902"
root_domain = ".web.garage.localhost"
index = "index.html"
"""

QUERIES = [
    """
    SELECT country, SUM(total) AS total
    FROM invoice
    GROUP BY country
    ORDER BY total DESC;
    """,
    """
    SELECT customer, country, total
    FROM invoice
    ORDER BY customer;
    """,
]


@dataclass(frozen=True, slots=True)
class DatabaseCase:
    """A prepared local database and its open comparison connection."""

    path: Path
    connection: sqlite3.Connection


def s3_config() -> Config:
    """Return botocore settings suitable for a local S3-compatible endpoint."""
    return Config(s3={"addressing_style": "path"})


def create_s3_client(endpoint: str) -> Any:
    """Create an authenticated S3 client for the local Garage instance."""
    return boto3.client(
        "s3",
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        endpoint_url=endpoint,
        region_name=S3_REGION,
        config=s3_config(),
    )


def wait_for_garage(client: Any, timeout_seconds: float = 60.0) -> None:
    """Wait until Garage exposes its automatically created default bucket."""
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            client.head_bucket(Bucket=GARAGE_DEFAULT_BUCKET)
        except Exception as error:
            last_error = error
            time.sleep(0.5)
        else:
            return

    raise RuntimeError("Garage did not become ready") from last_error


@pytest.fixture(scope="session")
def garage(tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    """Provide a ready disposable Garage instance for S3 integration tests."""
    data_dir = tmp_path_factory.mktemp("garage")
    (data_dir / "meta").mkdir()
    (data_dir / "data").mkdir()
    config_path = data_dir / "garage.toml"
    config_path.write_text(GARAGE_CONFIG, encoding="utf-8")

    container = DockerContainer(
        image="dxflrs/garage:v2.3.0",
        command="/garage server --single-node --default-bucket",
        env={
            "GARAGE_DEFAULT_ACCESS_KEY": S3_ACCESS_KEY,
            "GARAGE_DEFAULT_SECRET_KEY": S3_SECRET_KEY,
            "GARAGE_DEFAULT_BUCKET": GARAGE_DEFAULT_BUCKET,
        },
        ports=[3900],
        volumes=[
            (str(config_path), "/etc/garage.toml", "ro"),
            (str(data_dir / "meta"), "/var/lib/garage/meta", "rw"),
            (str(data_dir / "data"), "/var/lib/garage/data", "rw"),
        ],
    )

    with container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(3900)
        endpoint = f"http://{host}:{port}"
        wait_for_garage(client=create_s3_client(endpoint=endpoint))
        yield endpoint


@pytest.fixture
def s3_client(garage: str) -> Iterator[Any]:
    """Provide an authenticated S3 client for one test."""
    yield create_s3_client(endpoint=garage)


@pytest.fixture
def bucket(s3_client: Any) -> Iterator[str]:
    """Create and clean up a unique bucket for one test."""
    bucket_name = f"s3vfs-{uuid.uuid4().hex}"
    s3_client.create_bucket(Bucket=bucket_name)
    try:
        yield bucket_name
    finally:
        objects = s3_client.list_objects_v2(Bucket=bucket_name).get("Contents", [])
        if objects:
            s3_client.delete_objects(
                Bucket=bucket_name,
                Delete={"Objects": [{"Key": item["Key"]} for item in objects]},
            )
        s3_client.delete_bucket(Bucket=bucket_name)


@pytest.fixture
def s3vfs(garage: str) -> Iterator[s3sqlite.S3VFS]:
    """Provide an S3 VFS configured for Garage range reads."""
    filesystem = s3fs.S3FileSystem(
        key=S3_ACCESS_KEY,
        secret=S3_SECRET_KEY,
        client_kwargs={
            "endpoint_url": garage,
            "region_name": S3_REGION,
        },
        config_kwargs={"s3": {"addressing_style": "path"}},
    )
    yield s3sqlite.S3VFS(
        name="s3-vfs",
        fs=filesystem,
        file_kwargs={"cache_type": "bytes", "cache_size": 100_000_000},
    )


def set_page_size(connection: sqlite3.Connection, page_size: int) -> None:
    """Set a database page size before the database receives any tables."""
    connection.execute(f"PRAGMA page_size = {page_size};")
    connection.execute("VACUUM;")


def create_schema(connection: sqlite3.Connection) -> None:
    """Create the small comparison schema used by the integration tests."""
    connection.executescript(
        """
        CREATE TABLE invoice (
            customer TEXT NOT NULL,
            country TEXT NOT NULL,
            total REAL NOT NULL
        );

        INSERT INTO invoice (customer, country, total) VALUES
            ('Ada', 'Spain', 12.50),
            ('Grace', 'France', 50.00),
            ('Linus', 'Spain', 25.00),
            ('Margaret', 'Germany', 7.50);
        """
    )


def create_database(
    database_path: Path,
    page_size: int,
    journal_mode: str,
    use_wal_transition: bool,
) -> DatabaseCase:
    """Create a database with the requested page and journal-mode settings."""
    connection = sqlite3.connect(database=database_path, isolation_level=None)
    set_page_size(connection=connection, page_size=page_size)

    if use_wal_transition:
        connection.execute("PRAGMA journal_mode = WAL;")
        create_schema(connection=connection)
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        connection.execute(f"PRAGMA journal_mode = {journal_mode};")
    else:
        connection.execute(f"PRAGMA journal_mode = {journal_mode};")
        create_schema(connection=connection)

    assert connection.execute("PRAGMA page_size;").fetchone()[0] == page_size
    actual_journal_mode = connection.execute("PRAGMA journal_mode;").fetchone()[0]
    assert actual_journal_mode.lower() == journal_mode.lower()
    return DatabaseCase(path=database_path, connection=connection)


@pytest.fixture(params=JOURNAL_CASES)
def database(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[DatabaseCase]:
    """Provide a database for each page-size and journal-mode combination."""
    page_size, journal_mode = request.param
    database_path = tmp_path / "database.sqlite3"
    database_case = create_database(
        database_path=database_path,
        page_size=page_size,
        journal_mode=journal_mode,
        use_wal_transition=False,
    )
    try:
        yield database_case
    finally:
        database_case.connection.close()


@pytest.fixture(params=JOURNAL_CASES)
def database_after_wal(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> Iterator[DatabaseCase]:
    """Provide databases transitioned from WAL to each supported final mode."""
    page_size, journal_mode = request.param
    database_path = tmp_path / "database.sqlite3"
    database_case = create_database(
        database_path=database_path,
        page_size=page_size,
        journal_mode=journal_mode,
        use_wal_transition=True,
    )
    try:
        yield database_case
    finally:
        database_case.connection.close()


def assert_remote_query_matches_local(
    database_case: DatabaseCase,
    database_key: str,
    s3vfs: s3sqlite.S3VFS,
    query: str,
) -> None:
    """Compare one query result from the remote VFS with local SQLite."""
    expected_rows = database_case.connection.execute(query).fetchall()
    with apsw.Connection(
        filename=database_key,
        vfs=s3vfs.name,
        flags=apsw.SQLITE_OPEN_READONLY,
    ) as remote_connection:
        actual_rows = remote_connection.execute(query).fetchall()

    assert actual_rows == expected_rows


@pytest.mark.parametrize("query", QUERIES)
def test_s3vfs_query(
    bucket: str,
    s3vfs: s3sqlite.S3VFS,
    database: DatabaseCase,
    query: str,
) -> None:
    """Query a database uploaded to Garage through the S3 VFS."""
    database_key = f"{bucket}/{database.path.name}"
    s3vfs.upload_file(dbfile=database.path, dest=database_key)

    assert_remote_query_matches_local(
        database_case=database,
        database_key=database_key,
        s3vfs=s3vfs,
        query=query,
    )


@pytest.mark.parametrize("query", QUERIES)
def test_s3vfs_query_after_wal_transition(
    bucket: str,
    s3vfs: s3sqlite.S3VFS,
    database_after_wal: DatabaseCase,
    query: str,
) -> None:
    """Query a database after it has transitioned from WAL journaling."""
    database_key = f"{bucket}/{database_after_wal.path.name}"
    s3vfs.upload_file(dbfile=database_after_wal.path, dest=database_key)

    assert_remote_query_matches_local(
        database_case=database_after_wal,
        database_key=database_key,
        s3vfs=s3vfs,
        query=query,
    )


def test_convert_flags_formats_integer_and_list() -> None:
    """Format the flag shapes passed by APSW."""
    assert s3sqlite.convert_flags(1) == "0x000001"
    assert s3sqlite.convert_flags([1, 4]) == ["0x000001", "0x000004"]
