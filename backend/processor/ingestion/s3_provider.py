from __future__ import annotations

from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
import re

import boto3
from botocore import UNSIGNED
from botocore.client import Config


LEVEL2_KEY_RE = re.compile(r"(?P<site>K[A-Z0-9]{3})(?P<date>\d{8})_(?P<time>\d{6})")


@lru_cache(maxsize=1)
def anonymous_s3_client():
    return boto3.client("s3", config=Config(signature_version=UNSIGNED))


def list_level2_keys(bucket: str, prefix: str) -> list[str]:
    client = anonymous_s3_client()
    paginator = client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for entry in page.get("Contents", []):
            key = entry["Key"]
            if "MDM" in key:
                continue
            if LEVEL2_KEY_RE.search(Path(key).name):
                keys.append(key)
    return sorted(keys)


def download_key(bucket: str, key: str, destination: str | Path) -> Path:
    target = Path(destination)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        return target
    anonymous_s3_client().download_file(bucket, key, str(target))
    return target


def parse_scan_time(key: str) -> datetime:
    match = LEVEL2_KEY_RE.search(Path(key).name)
    if not match:
        raise ValueError(f"Unable to parse scan time from S3 key: {key}")
    return datetime.strptime(match.group("date") + match.group("time"), "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
