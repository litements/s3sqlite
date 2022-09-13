#!/usr/bin/env bash

set -euo pipefail

docker run \
  --rm \
  --name s3sqlite-minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -e "MINIO_ROOT_USER=AKIAIDIDIDIDIDIDIDID" \
  -e "MINIO_ROOT_PASSWORD=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" \
  quay.io/minio/minio server /data --console-address ":9001"
