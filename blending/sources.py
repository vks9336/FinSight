"""Data access for the blending layer.

The Alteryx workflows read from HDFS and MongoDB through ODBC connectors. These
substitutes run on the host outside the container network, so they reach HDFS
over WebHDFS (the NameNode's HTTP API on :9870) and MongoDB over pymongo.

Every reader accepts a local-directory fallback so the blend can be demonstrated
from exported files when the cluster is not running.
"""

from __future__ import annotations

import io
import os
from pathlib import Path

import pandas as pd
import requests

WEBHDFS = os.environ.get("FINSIGHT_WEBHDFS", "http://localhost:9870/webhdfs/v1")
HDFS_USER = os.environ.get("FINSIGHT_HDFS_USER", "root")
MONGO_URI = os.environ.get(
    "FINSIGHT_MONGO_URI", "mongodb://finsight:finsight@localhost:27017/?authSource=admin"
)
MONGO_DB = os.environ.get("FINSIGHT_MONGO_DB", "novacrest")
MONGO_COLLECTION = os.environ.get("FINSIGHT_MONGO_COLLECTION", "customers")

LOCAL_EXPORTS = Path(__file__).resolve().parents[1] / "exports"

TIMEOUT = 60


class SourceUnavailable(RuntimeError):
    """Raised when neither HDFS nor a local fallback can satisfy a read."""


# --------------------------------------------------------------------------
# WebHDFS
# --------------------------------------------------------------------------

def _webhdfs(path: str, op: str, **params):
    url = f"{WEBHDFS}{path}"
    params = {"op": op, "user.name": HDFS_USER, **params}
    return requests.get(url, params=params, timeout=TIMEOUT, allow_redirects=True)


def hdfs_available() -> bool:
    try:
        return _webhdfs("/", "LISTSTATUS").ok
    except requests.RequestException:
        return False


def list_dir(path: str) -> list[dict]:
    resp = _webhdfs(path, "LISTSTATUS")
    if not resp.ok:
        raise SourceUnavailable(f"cannot list {path}: HTTP {resp.status_code}")
    return resp.json()["FileStatuses"]["FileStatus"]


def read_file(path: str) -> bytes:
    resp = _webhdfs(path, "OPEN")
    if not resp.ok:
        raise SourceUnavailable(f"cannot read {path}: HTTP {resp.status_code}")
    return resp.content


def read_csv(hdfs_path: str, local_name: str | None = None) -> pd.DataFrame:
    """Read a single CSV from HDFS, or from exports/ if HDFS is unreachable."""
    try:
        return pd.read_csv(io.BytesIO(read_file(hdfs_path)))
    except (SourceUnavailable, requests.RequestException) as exc:
        local = LOCAL_EXPORTS / (local_name or Path(hdfs_path).name)
        if local.exists():
            print(f"    (HDFS unavailable, reading local {local.name})")
            return pd.read_csv(local)
        raise SourceUnavailable(
            f"{hdfs_path} unreadable and no local fallback at {local}"
        ) from exc


def read_parquet_dir(hdfs_dir: str, local_name: str | None = None) -> pd.DataFrame:
    """Read every part file in an HDFS Parquet directory into one frame.

    Spark writes a directory of parts plus _SUCCESS markers; only the .parquet
    entries are fetched. Partition directories (step=N) are recursed into so the
    partition column survives as a real column.
    """
    try:
        frames = []
        for entry in list_dir(hdfs_dir):
            name = entry["pathSuffix"]
            child = f"{hdfs_dir}/{name}"
            if entry["type"] == "DIRECTORY":
                sub = read_parquet_dir(child)
                if "=" in name:                    # step=42 -> column step
                    key, value = name.split("=", 1)
                    sub[key] = pd.to_numeric(value, errors="ignore")
                frames.append(sub)
            elif name.endswith(".parquet"):
                frames.append(pd.read_parquet(io.BytesIO(read_file(child))))
        if not frames:
            raise SourceUnavailable(f"no parquet files under {hdfs_dir}")
        return pd.concat(frames, ignore_index=True)
    except (SourceUnavailable, requests.RequestException) as exc:
        if local_name:
            local = LOCAL_EXPORTS / local_name
            if local.exists():
                print(f"    (HDFS unavailable, reading local {local.name})")
                return pd.read_csv(local)
        raise SourceUnavailable(f"{hdfs_dir} unreadable") from exc


# --------------------------------------------------------------------------
# MongoDB
# --------------------------------------------------------------------------

def read_customers() -> pd.DataFrame:
    """Customer profiles from MongoDB, falling back to the generated JSON.

    The generated file is the same content that was imported, so the blend
    produces identical output either way.
    """
    try:
        from pymongo import MongoClient

        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=4000)
        client.admin.command("ping")
        docs = list(client[MONGO_DB][MONGO_COLLECTION].find({}, {"_id": 0}))
        if docs:
            print(f"    read {len(docs):,} customer documents from MongoDB")
            return pd.DataFrame(docs)
        raise SourceUnavailable("collection is empty")
    except Exception as exc:  # noqa: BLE001
        local = Path(__file__).resolve().parents[1] / "data" / "raw" / "novacrest_customers.json"
        if local.exists():
            print(f"    (MongoDB unavailable: {type(exc).__name__}; reading {local.name})")
            return pd.read_json(local, lines=True)
        raise SourceUnavailable("no MongoDB and no local customer JSON") from exc
