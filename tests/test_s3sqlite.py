from __future__ import annotations

import itertools
import sqlite3
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import apsw
import pytest

from s3sqlite.vfs import S3VFS
from s3sqlite.vfs import convert_flags

PAGE_SIZES = [512, 1024, 2048, 4096, 8192, 16384, 32768, 65536]
JOURNAL_MODES = ["DELETE", "TRUNCATE", "PERSIST", "MEMORY", "OFF"]
JOURNAL_CASES = list(itertools.product(PAGE_SIZES, JOURNAL_MODES))

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
    s3vfs: S3VFS,
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
    s3vfs: S3VFS,
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
    s3vfs: S3VFS,
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
    assert convert_flags(1) == "0x000001"
    assert convert_flags([1, 4]) == ["0x000001", "0x000004"]
