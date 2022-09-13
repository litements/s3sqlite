import itertools
import sqlite3
import subprocess
import uuid
from contextlib import contextmanager
from typing import Tuple

import apsw
import boto3
import pytest
import s3fs
from fsspec.implementations.local import LocalFileSystem

import s3sqlite

PAGE_SIZES = [512, 1024, 2048, 4096, 8192, 16384, 32768, 65536]
JOURNAL_MODES = ["DELETE", "TRUNCATE", "PERSIST", "MEMORY", "OFF"]

PAGE_JOURNAL_MIX = itertools.product(PAGE_SIZES, JOURNAL_MODES)

QUERIES = [
    """
-- Get the total sales per country.

SELECT BillingCountry, sum(total) AS Total FROM Invoice
GROUP BY BillingCountry
ORDER BY Total desc
    """,
    """
-- Get the Invoice Total, Customer name, Country and Sale Agent name for all invoices and customers.

SELECT i.`Total`, c.FirstName || " " || c.LastName AS CustomerName, c.Country, e.FirstName || " " || e.LastName AS SalesAgent FROM Invoice i
JOIN Customer c ON c.CustomerId = i.CustomerId
JOIN Employee e ON e.EmployeeId = c.SupportRepId
ORDER BY SalesAgent;
    """,
    """
-- Get the invoices associated with each sales agent. The resultant table should include the Sales Agent's full name.

SELECT i.InvoiceId, i.InvoiceDate, i.`Total`, i.CustomerId, e.FirstName || " " || e.LastName AS SalesAgent FROM Invoice i, Customer c, Employee e
WHERE c.CustomerId = i.CustomerId
AND e.EmployeeId = c.SupportRepId
ORDER BY SalesAgent == "Sales Support Agent";
    """,
    """
-- Which sales agent made the most in sales over all?

SELECT "Sales Winner", max("Total") AS "Total" FROM (
SELECT e.firstName || " " || e.lastName AS "Sales Winner", sum(i.total) AS "Total" FROM Invoice AS i
JOIN customer AS c ON c.customerid =  i.customerid
JOIN employee AS e ON e.employeeid = c.supportrepid
GROUP BY e.Employeeid
)
""",
]

dbname = "chinook.sqlite3"


@pytest.fixture(autouse=True, scope="session")
def minio():
    proc = subprocess.Popen(
        [
            "docker",
            "run",
            "--rm",
            "--name",
            "s3sqlite-minio",
            "-p",
            "9000:9000",
            "-p",
            "9001:9001",
            "-e",
            "MINIO_ROOT_USER=AKIAIDIDIDIDIDIDIDID",
            "-e",
            "MINIO_ROOT_PASSWORD=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "quay.io/minio/minio",
            "server",
            "/data",
            "--console-address",
            ":9001",
        ],
        text=True,
    )

    yield proc

    proc.terminate()


@pytest.fixture
def bucket():
    session = boto3.Session(
        aws_access_key_id="AKIAIDIDIDIDIDIDIDID",
        aws_secret_access_key="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        region_name="us-east-1",
    )
    s3 = session.resource("s3", endpoint_url="http://localhost:9000/")

    name = f"s3vfs-{str(uuid.uuid4())}"

    bucket = s3.create_bucket(Bucket=name)
    yield name
    bucket.objects.all().delete()
    bucket.delete()


@pytest.fixture
def s3_data():
    return dict(
        key="AKIAIDIDIDIDIDIDIDID",
        secret="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        endpoint_url="http://localhost:9000/",
    )


# @pytest.fixture(params=BLOCK_SIZES)
@pytest.fixture
def s3vfs(s3_data):
    s3 = s3fs.S3FileSystem(
        key=s3_data["key"],
        secret=s3_data["secret"],
        client_kwargs={"endpoint_url": s3_data["endpoint_url"]},
    )

    yield s3sqlite.AbstractVFS(name="s3-vfs", fs=s3)


@pytest.fixture
def local_fs():
    fs = LocalFileSystem()
    yield fs


@pytest.fixture
def localvfs(local_fs):
    return s3sqlite.AbstractVFS(name="local-vfs", fs=local_fs)


@contextmanager
def transaction(conn):
    conn.execute("BEGIN;")
    try:
        yield conn
    except:
        conn.execute("ROLLBACK;")
        raise
    else:
        conn.execute("COMMIT;")


def set_pragmas(conn, page_size, journal_mode):
    sqls = [
        f"PRAGMA journal_mode = {journal_mode};",
        f"PRAGMA page_size = {page_size};",
        "VACUUM;",
    ]
    for sql in sqls:
        print(f"Running: {sql}")
        conn.execute(sql)


def create_db(conn):
    with open("chinook.sql") as f:
        sql = f.read()

    # with transaction(conn):
    conn.executescript(sql)


@pytest.fixture(params=itertools.product(PAGE_SIZES, JOURNAL_MODES))
def get_db(request) -> Tuple[str, sqlite3.Connection]:
    # if dbname in os.listdir():
    #     os.system(f"rm -rf {dbname}*")

    conn = sqlite3.connect(dbname, isolation_level=None)
    set_pragmas(conn, page_size=request.param[0], journal_mode=request.param[1])
    # create_db(conn)

    assert conn.execute("PRAGMA page_size;").fetchone()[0] == request.param[0]
    assert (
        conn.execute("PRAGMA journal_mode;").fetchone()[0].lower()
        == request.param[1].lower()
    )
    return dbname, conn


# I haven't been able to make this work as I want for DBs in WAL mode. For now
# I'll just document that the DB needs to be set to a different journal mode
# before uploading it to S3. This functions will test a database that is in WAL
# mode and then changed to a different mode. This is because I spect the typica
# workflow to start with a DB in WAL mode to load data in it, then the
# journal_mode gets changed to something else for uploading:
#
# set page size -> vacuum -> set WAL -> truncate WAL -> change journal model before uploadig


def set_wal_pragmas(conn, page_size, journal_mode):
    # Page size can't be changed after setting WAL mode, so we need to do
    # it before.
    sqls = [
        f"PRAGMA page_size = {page_size};",
        "VACUUM;",
        "PRAGMA journal_mode = WAL;",
        "PRAGMA wal_checkpoint(truncate);",
        f"PRAGMA journal_mode = {journal_mode};",
    ]
    for sql in sqls:
        print(f"Running: {sql}")
        conn.execute(sql)


@pytest.fixture(params=itertools.product(PAGE_SIZES, JOURNAL_MODES))
def get_db_wal(request) -> Tuple[str, sqlite3.Connection]:

    conn = sqlite3.connect(dbname, isolation_level=None)
    set_wal_pragmas(conn, page_size=request.param[0], journal_mode=request.param[1])

    assert conn.execute("PRAGMA page_size;").fetchone()[0] == request.param[0]
    assert (
        conn.execute("PRAGMA journal_mode;").fetchone()[0].lower()
        == request.param[1].lower()
    )
    return dbname, conn


@pytest.mark.parametrize("query", QUERIES)
def test_s3vfs_query_wal(bucket, s3vfs, get_db_wal, query):

    key_prefix = f"{bucket}/{dbname}"
    s3vfs.upload_file(get_db_wal[0], dest=key_prefix)

    # Create a database and query it
    with apsw.Connection(
        key_prefix, vfs=s3vfs.name, flags=apsw.SQLITE_OPEN_READONLY
    ) as conn:

        local_c = get_db_wal[1].execute(query)
        c = conn.execute(query)
        assert c.fetchall() == local_c.fetchall()


@pytest.mark.parametrize("query", QUERIES)
def test_s3vfs_query(bucket, s3vfs, get_db, query):

    key_prefix = f"{bucket}/{dbname}"
    s3vfs.upload_file(get_db[0], dest=key_prefix)

    # Create a database and query it
    with apsw.Connection(
        key_prefix, vfs=s3vfs.name, flags=apsw.SQLITE_OPEN_READONLY
    ) as conn:

        local_c = get_db[1].execute(query)
        c = conn.execute(query)
        assert c.fetchall() == local_c.fetchall()
