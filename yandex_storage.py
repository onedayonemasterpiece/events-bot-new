from __future__ import annotations

from typing import Any
from urllib.parse import quote, urlparse

_DEFAULT_ENDPOINT = "https://storage.yandexcloud.net"
_DEFAULT_REGION = "ru-central1"
_DEFAULT_BUCKET = "kenigevents"

_CLIENT_CACHE: dict[tuple[str, str, str, str], Any] = {}


def _first_env(*names: str) -> str:
    import os

    for name in names:
        value = (os.getenv(name) or "").strip()
        if value:
            return value
    return ""


def get_yandex_storage_credentials() -> tuple[str, str] | None:
    access_key = _first_env("YC_SA_BOT_STORAGE", "YC_SA_ML_DEV")
    secret_key = _first_env("YC_SA_BOT_STORAGE_KEY", "YC_SA_ML_DEV_key", "YC_SA_ML_DEV_KEY")
    if access_key and secret_key:
        return access_key, secret_key
    return None


def yandex_storage_enabled() -> bool:
    return get_yandex_storage_credentials() is not None


def get_yandex_storage_bucket() -> str:
    return _first_env("YC_STORAGE_BUCKET") or _DEFAULT_BUCKET


def get_yandex_storage_endpoint() -> str:
    return (_first_env("YC_STORAGE_ENDPOINT", "YC_STORAGE_PUBLIC_BASE_URL") or _DEFAULT_ENDPOINT).rstrip("/")


def get_yandex_storage_region() -> str:
    return _first_env("YC_STORAGE_REGION") or _DEFAULT_REGION


def get_yandex_storage_client() -> Any | None:
    creds = get_yandex_storage_credentials()
    if creds is None:
        return None
    endpoint = get_yandex_storage_endpoint()
    region = get_yandex_storage_region()
    access_key, secret_key = creds
    cache_key = (access_key, secret_key, endpoint, region)
    cached = _CLIENT_CACHE.get(cache_key)
    if cached is not None:
        return cached

    try:
        import boto3
        from botocore.client import Config
    except Exception:
        return None

    client = boto3.session.Session().client(
        "s3",
        region_name=region,
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    _CLIENT_CACHE[cache_key] = client
    return client


def build_yandex_public_url(*, bucket: str | None = None, object_path: str) -> str | None:
    b = (bucket or get_yandex_storage_bucket()).strip()
    p = str(object_path or "").strip().lstrip("/")
    if not b or not p:
        return None
    return f"{get_yandex_storage_endpoint()}/{quote(b)}/{quote(p, safe='/')}"


def parse_yandex_storage_url(url: str | None) -> tuple[str, str] | None:
    raw = str(url or "").strip()
    if not raw:
        return None
    try:
        parsed = urlparse(raw)
    except Exception:
        return None

    host = (parsed.netloc or "").strip().lower()
    path_parts = [p for p in (parsed.path or "").split("/") if p]

    if host == "storage.yandexcloud.net":
        if len(path_parts) < 2:
            return None
        bucket = path_parts[0]
        object_path = "/".join(path_parts[1:])
        return (bucket, object_path) if bucket and object_path else None

    suffix = ".storage.yandexcloud.net"
    if host.endswith(suffix):
        bucket = host[: -len(suffix)].strip(".")
        object_path = "/".join(path_parts)
        return (bucket, object_path) if bucket and object_path else None

    return None


def is_yandex_storage_url(url: str | None) -> bool:
    return parse_yandex_storage_url(url) is not None


def is_managed_storage_url(url: str | None) -> bool:
    raw = str(url or "").strip().lower()
    if not raw:
        return False
    if is_yandex_storage_url(raw):
        return True
    return "/storage/v1/object/" in raw or "supabase.co/storage/" in raw


def build_public_storage_url(*, bucket: str, object_path: str) -> str | None:
    b = str(bucket or "").strip()
    p = str(object_path or "").strip().lstrip("/")
    if not b or not p:
        return None
    yandex_bucket = get_yandex_storage_bucket()
    if b == yandex_bucket and yandex_storage_enabled():
        return build_yandex_public_url(bucket=b, object_path=p)

    import os

    base = (os.getenv("SUPABASE_URL") or "").strip().rstrip("/")
    if not base:
        return None
    return f"{base}/storage/v1/object/public/{b}/{p}"


def yandex_storage_object_exists(
    *,
    bucket: str,
    object_path: str,
    client: Any | None = None,
) -> bool | None:
    b = str(bucket or "").strip()
    p = str(object_path or "").strip().lstrip("/")
    if not b or not p:
        return None
    client = client or get_yandex_storage_client()
    if client is None:
        return None

    try:
        client.head_object(Bucket=b, Key=p)
        return True
    except Exception as exc:
        response = getattr(exc, "response", {}) or {}
        status = int((response.get("ResponseMetadata", {}) or {}).get("HTTPStatusCode") or 0)
        code = str((response.get("Error", {}) or {}).get("Code") or "").strip()
        if status == 404 or code in {"404", "NoSuchKey", "NotFound"}:
            return False
        return None


def upload_yandex_public_bytes(
    data: bytes,
    *,
    object_path: str,
    content_type: str,
    bucket: str | None = None,
    cache_control: str = "public, max-age=31536000",
    client: Any | None = None,
) -> str | None:
    if not data:
        return None
    b = (bucket or get_yandex_storage_bucket()).strip()
    p = str(object_path or "").strip().lstrip("/")
    if not b or not p:
        return None
    client = client or get_yandex_storage_client()
    if client is None:
        return None

    try:
        client.put_object(
            Bucket=b,
            Key=p,
            Body=data,
            ContentType=str(content_type or "application/octet-stream"),
            CacheControl=str(cache_control or "public, max-age=31536000"),
        )
    except Exception:
        return None
    return build_yandex_public_url(bucket=b, object_path=p)


def delete_yandex_objects(
    *,
    bucket: str,
    object_paths: list[str],
    client: Any | None = None,
) -> int:
    b = str(bucket or "").strip()
    keys = [str(p or "").strip().lstrip("/") for p in list(object_paths or []) if str(p or "").strip()]
    if not b or not keys:
        return 0
    client = client or get_yandex_storage_client()
    if client is None:
        raise RuntimeError("yandex storage client unavailable")

    removed = 0
    for start in range(0, len(keys), 1000):
        chunk = keys[start : start + 1000]
        client.delete_objects(
            Bucket=b,
            Delete={"Objects": [{"Key": key} for key in chunk], "Quiet": True},
        )
        removed += len(chunk)
    return removed
