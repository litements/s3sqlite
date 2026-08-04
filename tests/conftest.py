from __future__ import annotations

import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import boto3
import pytest
import s3fs
from botocore.config import Config
from testcontainers.core.container import DockerContainer

from s3sqlite.vfs import S3VFS

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
def s3_filesystem(garage: str) -> Iterator[s3fs.S3FileSystem]:
    """Provide an authenticated fsspec filesystem for one test."""
    filesystem = s3fs.S3FileSystem(
        key=S3_ACCESS_KEY,
        secret=S3_SECRET_KEY,
        client_kwargs={
            "endpoint_url": garage,
            "region_name": S3_REGION,
        },
        config_kwargs={"s3": {"addressing_style": "path"}},
    )
    yield filesystem


@pytest.fixture
def s3vfs(
    s3_filesystem: s3fs.S3FileSystem,
    tmp_path: Path,
) -> Iterator[S3VFS]:
    """Provide an S3 VFS configured for Garage range reads."""
    yield S3VFS(
        name="s3-vfs",
        fs=s3_filesystem,
        cache_path=tmp_path / "cache.sqlite3",
    )
