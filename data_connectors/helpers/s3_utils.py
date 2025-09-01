# s3_utils.py
# pip install minio
from __future__ import annotations

import hashlib
import io
import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

try:
    from minio import Minio
    from minio.error import S3Error
except Exception as e:  # pragma: no cover
    Minio = None  # type: ignore
    S3Error = Exception  # type: ignore


# ---------- Public helpers ----------

def is_s3_uri(uri: str) -> bool:
    return isinstance(uri, str) and uri.startswith("s3://")


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    """
    s3://<bucket>/<key...>  ->  (bucket, key)
    """
    if not is_s3_uri(uri):
        raise ValueError(f"Not an s3 URI: {uri!r}")
    without = uri[5:]
    parts = without.split("/", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(f"Malformed s3 URI (expect s3://bucket/key): {uri!r}")
    return parts[0], parts[1]


@dataclass
class S3Config:
    """
    If you don't pass one, env vars are used:

    Preferred (MinIO-style):
      MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE=true|false

    Also supported (S3-style):
      S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_SECURE=true|false

    Defaults to AWS S3 public endpoint if nothing is set.
    """
    endpoint: str = "s3.amazonaws.com"
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    secure: bool = True

    @staticmethod
    def from_env() -> "S3Config":
        endpoint = (
            os.getenv("MINIO_ENDPOINT")
            or os.getenv("S3_ENDPOINT")
            or "s3.amazonaws.com"
        )
        access = os.getenv("MINIO_ACCESS_KEY") or os.getenv("S3_ACCESS_KEY")
        secret = os.getenv("MINIO_SECRET_KEY") or os.getenv("S3_SECRET_KEY")
        secure_str = (
            os.getenv("MINIO_SECURE")
            or os.getenv("S3_SECURE")
            or "true"
        ).lower()
        secure = secure_str not in ("0", "false", "no")
        return S3Config(endpoint=endpoint, access_key=access, secret_key=secret, secure=secure)

    def client(self) -> "Minio":
        if Minio is None:
            raise RuntimeError("minio package not installed. Run: pip install minio")
        if not self.endpoint:
            raise ValueError("S3/MinIO endpoint is required.")
        return Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
        )


# ---------- Cache (ETag/Last-Modified aware) ----------

def cached_s3_fetch(
    uri: str,
    *,
    cache_root: Optional[str | Path] = None,
    ttl_seconds: Optional[int] = None,
    min_free_bytes: Optional[int] = 50_000_000_000,  # 50 GB default guardrail (set None to disable)
    s3_config: Optional[S3Config] = None,
    s3_client: Optional["Minio"] = None,
    logger=None,
) -> Path:
    """
    Download an S3 object addressed by s3://bucket/key using MinIO and return a
    local cached path. New versions are detected by ETag (or Last-Modified+Size fallback).

    - cache_root: directory to keep cached files. If None, uses $BINANCE_TEMP_FILES
                  or ~/.cache/s3_cache  (if BINANCE_TEMP_FILES is an s3:// URI, falls back
                  to ~/.cache/s3_cache)
    - ttl_seconds: if set and a cached item is newer than TTL, skip HEAD (fast path).
                   If None, always validate against remote (consistent).
    - min_free_bytes: ensure this many free bytes are available before writing.
    """
    if not is_s3_uri(uri):
        raise ValueError(f"Not an s3 URI: {uri}")
    bucket, key = parse_s3_uri(uri)

    client = s3_client or (s3_config.client() if s3_config else S3Config.from_env().client())
    cache_root = _resolve_cache_root(cache_root)

    # Optional TTL: return latest cached if still "fresh"
    latest_dir = _uri_dir(cache_root, uri)
    latest_meta = _load_meta(latest_dir / "meta.json")
    now = _utcnow()

    if latest_meta and ttl_seconds is not None:
        created = _parse_iso(latest_meta.get("created_at"))
        payload_path = latest_dir / "payload.bin"
        if created and (now - created).total_seconds() < ttl_seconds and payload_path.exists():
            if logger: logger.info(f"[S3 cache TTL hit] {uri} -> {payload_path}")
            return payload_path

    # Always stat unless TTL hit
    try:
        st = client.stat_object(bucket, key)
        etag = _sanitize_etag(getattr(st, "etag", None))
        last_modified = getattr(st, "last_modified", None)
        size = getattr(st, "size", None)
        lm_iso = _to_iso(last_modified) if last_modified else None
    except S3Error as e:
        raise FileNotFoundError(f"S3 object not found or not accessible: {uri} ({e})") from e

    # Versioned directory (by ETag or LM+Size fallback)
    version_fingerprint = etag or f"lm:{lm_iso}|sz:{size}"
    version_dir = _uri_dir(cache_root, f"{uri}#v={version_fingerprint}")
    payload_path = version_dir / "payload.bin"
    meta_path = version_dir / "meta.json"

    if payload_path.exists():
        # Cache hit on current version
        _save_meta(latest_dir / "meta.json", {
            "source_uri": uri,
            "created_at": _to_iso(now),
            "etag": etag, "last_modified": lm_iso, "size": size
        })
        if logger: logger.info(f"[S3 cache hit] {uri} -> {payload_path}")
        return payload_path

    # Ensure free space if requested
    if min_free_bytes is not None:
        _ensure_free_space(cache_root, min_free_bytes)

    # Download atomically
    version_dir.mkdir(parents=True, exist_ok=True)
    tmp = payload_path.with_suffix(".tmp")

    if logger: logger.info(f"[S3 download] {uri} -> {payload_path}")
    response = None
    try:
        response = client.get_object(bucket, key)
        with open(tmp, "wb") as f:
            for chunk in iter(lambda: response.read(8 * 1024 * 1024), b""):
                if not chunk:
                    break
                f.write(chunk)
            f.flush()
            os.fsync(f.fileno())
    finally:
        try:
            if response is not None:
                response.close()
                response.release_conn()
        except Exception:
            pass

    os.replace(tmp, payload_path)

    meta = {
        "source_uri": uri,
        "created_at": _to_iso(now),
        "etag": etag,
        "last_modified": lm_iso,
        "size": size,
    }
    _save_meta(meta_path, meta)
    _save_meta(latest_dir / "meta.json", meta)

    return payload_path


# ---------- Upload (write) ----------

def upload_file_to_s3(
    uri: str,
    local_path: str | Path,
    *,
    s3_config: Optional[S3Config] = None,
    s3_client: Optional["Minio"] = None,
    content_type: Optional[str] = None,
    metadata: Optional[dict] = None,
    logger=None,
    create_bucket: bool = False,
) -> dict:
    """
    Upload a local file to an S3 path using MinIO.

    Parameters
    ----------
    uri : str
        Full S3 URI: s3://<bucket>/<key>
    local_path : str | Path
        Path to the local file to upload.
    s3_config : S3Config | None
        Configuration; if None, use S3Config.from_env().
    s3_client : Minio | None
        Reuse an existing MinIO client; otherwise one is created from s3_config.
    content_type : str | None
        Optional content-type (e.g., "application/zip").
    metadata : dict | None
        Optional user metadata dict.
    logger : object | None
        Optional logger with .info/.warning methods.
    create_bucket : bool
        If True, attempt to create the bucket if it doesn't exist.

    Returns
    -------
    dict with keys: {"bucket", "key", "etag", "version_id"}
    """
    if not is_s3_uri(uri):
        raise ValueError(f"Not an s3 URI: {uri}")
    bucket, key = parse_s3_uri(uri)

    client = s3_client or (s3_config.client() if s3_config else S3Config.from_env().client())

    lp = Path(local_path)
    if not lp.exists():
        raise FileNotFoundError(f"Local file does not exist: {lp}")

    # Optionally create bucket (disabled by default to avoid surprises)
    if create_bucket:
        try:
            exists = False
            try:
                exists = client.bucket_exists(bucket)  # type: ignore[attr-defined]
            except Exception:
                # Some S3 providers may not allow bucket_exists; ignore and try to create
                raise e
            if not exists:
                client.make_bucket(bucket)
                if logger: logger.info(f"[S3] Created bucket {bucket}")
        except Exception as e:
            raise e

    # Perform upload
    try:
        result = client.fput_object(
            bucket, key, str(lp),
            content_type=content_type,
            metadata=metadata,
        )
        etag = getattr(result, "etag", None)
        version_id = getattr(result, "version_id", None)
        if logger: logger.info(f"[S3 upload] {lp} -> s3://{bucket}/{key} (etag={etag}, version_id={version_id})")
        return {"bucket": bucket, "key": key, "etag": etag, "version_id": version_id}
    except S3Error as e:
        raise RuntimeError(f"Failed to upload {lp} to s3://{bucket}/{key}: {e}") from e


# ---------- Internals ----------

def _resolve_cache_root(cache_root: Optional[str | Path]) -> Path:
    """
    Decide where to place the local on-disk cache for cached_s3_fetch.
    - If cache_root is provided, use it.
    - Else, use BINANCE_TEMP_FILES unless it is an s3:// URI; in that case,
      fall back to ~/.cache/s3_cache.
    """
    if cache_root:
        root_str = str(cache_root)
    else:
        env_val = os.getenv("BINANCE_TEMP_FILES", "~/.cache/s3_cache")
        # If env is an s3:// path, we cannot use it as a local folder
        root_str = "~/.cache/s3_cache" if is_s3_uri(env_val) else env_val

    root = Path(os.path.expanduser(root_str))
    root.mkdir(parents=True, exist_ok=True)
    return root


def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _uri_dir(cache_root: Path, uri: str) -> Path:
    return cache_root / _sha256(uri)


def _save_meta(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _load_meta(path: Path) -> Optional[dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except Exception:
        return None


def _ensure_free_space(directory: Path | str, min_free_bytes: int) -> None:
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(directory).free
    if free < min_free_bytes:
        raise OSError(f"Not enough free space in {directory} (free={free} < required={min_free_bytes}).")


def _sanitize_etag(etag: Optional[str]) -> Optional[str]:
    if etag is None:
        return None
    # Some providers include double quotes
    return etag.strip('"').strip("'") or None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _to_iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None
