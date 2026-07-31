"""Fail-closed blue/green publication for the static production root.

The active Object Storage bucket is never an upload target.  A checked
``production_root`` archive is reconciled into the inactive bucket, verified
byte-for-byte, and only then made active through the ALB control plane.  The
two weight updates intentionally enable the new complete tree before disabling
the old complete tree: an interrupted convergence may serve old or new bytes,
but it can never expose a partially copied tree.

The module is importable by ``main.py`` and also provides a plan/apply CLI for
operators.  Production enablement is deliberately outside this file and stays
default-off in ``.env.example``.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import shutil
import subprocess
import tarfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence
from urllib.error import HTTPError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen

from static_site_release import (
    StaticSitePermanentError,
    StaticSiteRetryableError,
    iso_utc,
)


ROOT_MANIFEST_SCHEMA = "static_release_manifest_v1"
ROOT_RECEIPT_SCHEMA = "static_site_atomic_root_receipt_v1"
ROOT_REQUIRED_CHECKS = (
    "astro_build",
    "template_matrix",
    "production_contract",
    "catalog_parity",
    "fixture_isolation",
    "canonical_and_indexing",
    "tree_hashes",
    "browser_visual",
)
ROOT_REQUIRED_ROUTES = (
    "index.html",
    "segodnya/index.html",
    "zavtra/index.html",
    "vyhodnye/index.html",
    "vystavki/index.html",
    "festivali/index.html",
    "populyarnoe/index.html",
    "poisk/index.html",
    "dlya-menya/index.html",
    "kluby-po-interesam/index.html",
    "partners/index.html",
    "partnerstvo/index.html",
    "podborki/besplatnye-sobytiya/index.html",
    "fokus-gruppa/priglashenie/index.html",
    "fokus-gruppa/manifest.webmanifest",
    "robots.txt",
    "sitemap.xml",
    "manifest.webmanifest",
)
ROOT_SMOKE_KEYS = (
    "index.html",
    "segodnya/index.html",
    "poisk/index.html",
    "manifest.webmanifest",
    "fokus-gruppa/priglashenie/index.html",
    "fokus-gruppa/manifest.webmanifest",
)
ROOT_PROTECTED_PREFIXES = ("_review/", "_static/", "ics/", "p/")
ROOT_PROTECTED_KEYS = {"current.json", "previous.json", "promotion-lease.json"}
_HASH_RE = re.compile(r"[0-9a-f]{64}")
_BUILD_RE = re.compile(r"production-[A-Za-z0-9][A-Za-z0-9._-]{0,191}")
_BACKEND_RE = re.compile(r"[a-z][-a-z0-9]{1,61}[a-z0-9]")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _tree_sha256(files: Sequence[Mapping[str, Any]]) -> str:
    payload = "".join(
        f"{item['key']}\0{item['sha256']}\0{int(item['size'])}\n"
        for item in sorted(files, key=lambda item: str(item["key"]))
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _safe_key(value: Any) -> str:
    key = str(value or "")
    parts = key.split("/")
    if (
        not key
        or key.startswith("/")
        or "\\" in key
        or any(part in {"", ".", ".."} for part in parts)
    ):
        raise StaticSitePermanentError("atomic_root_object_key_invalid")
    if key in ROOT_PROTECTED_KEYS or key.startswith(ROOT_PROTECTED_PREFIXES):
        raise StaticSitePermanentError(f"atomic_root_non_page_key_rejected:{key[:160]}")
    return key


def _content_type(key: str) -> str:
    suffix = Path(key).suffix.lower()
    return {
        ".css": "text/css; charset=utf-8",
        ".html": "text/html; charset=utf-8",
        ".ics": "text/calendar; charset=utf-8",
        ".js": "text/javascript; charset=utf-8",
        ".json": "application/json; charset=utf-8",
        ".map": "application/json; charset=utf-8",
        ".webmanifest": "application/manifest+json; charset=utf-8",
        ".svg": "image/svg+xml",
        ".txt": "text/plain; charset=utf-8",
        ".xml": "application/xml; charset=utf-8",
        ".webp": "image/webp",
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".gif": "image/gif",
        ".woff": "font/woff",
        ".woff2": "font/woff2",
    }.get(suffix, "application/octet-stream")


@dataclass(frozen=True)
class RootSlot:
    name: str
    bucket: str
    backend: str


@dataclass(frozen=True)
class RootPublisherConfig:
    mode: str
    blue: RootSlot
    green: RootSlot
    shared_asset_bucket: str
    backend_group_id: str
    folder_id: str
    public_base_url: str
    receipt_path: Path
    extraction_root: Path
    yc_cli: str
    endpoint: str
    region: str
    access_key_id: str
    secret_access_key: str
    active_weight: int = 100

    @classmethod
    def from_env(
        cls,
        *,
        extraction_root: str | os.PathLike[str] | None = None,
    ) -> "RootPublisherConfig":
        mode = (os.getenv("STATIC_SITE_ROOT_PROMOTION_MODE") or "plan").strip().lower()
        if mode not in {"plan", "apply"}:
            raise StaticSitePermanentError("STATIC_SITE_ROOT_PROMOTION_MODE must be plan or apply")
        blue_bucket = (os.getenv("STATIC_SITE_ROOT_BLUE_BUCKET") or "").strip()
        green_bucket = (os.getenv("STATIC_SITE_ROOT_GREEN_BUCKET") or "").strip()
        blue_backend = (os.getenv("STATIC_SITE_ROOT_BLUE_BACKEND") or "").strip()
        green_backend = (os.getenv("STATIC_SITE_ROOT_GREEN_BACKEND") or "").strip()
        shared = (
            os.getenv("STATIC_SITE_ROOT_SHARED_ASSET_BUCKET")
            or os.getenv("KENIGEVENTS_SITE_YC_BUCKET")
            or ""
        ).strip()
        group = (os.getenv("STATIC_SITE_ROOT_ALB_BACKEND_GROUP_ID") or "").strip()
        folder = (os.getenv("STATIC_SITE_ROOT_YC_FOLDER_ID") or "").strip()
        required = {
            "STATIC_SITE_ROOT_BLUE_BUCKET": blue_bucket,
            "STATIC_SITE_ROOT_GREEN_BUCKET": green_bucket,
            "STATIC_SITE_ROOT_BLUE_BACKEND": blue_backend,
            "STATIC_SITE_ROOT_GREEN_BACKEND": green_backend,
            "STATIC_SITE_ROOT_SHARED_ASSET_BUCKET": shared,
            "STATIC_SITE_ROOT_ALB_BACKEND_GROUP_ID": group,
            "STATIC_SITE_ROOT_YC_FOLDER_ID": folder,
        }
        missing = sorted(name for name, value in required.items() if not value)
        if missing:
            raise StaticSitePermanentError(
                "atomic_root_inventory_config_missing:" + ",".join(missing)
            )
        if len({blue_bucket, green_bucket, shared}) != 3:
            raise StaticSitePermanentError("atomic_root_buckets_must_be_three_distinct_buckets")
        if blue_backend == green_backend or not all(
            _BACKEND_RE.fullmatch(value) for value in (blue_backend, green_backend)
        ):
            raise StaticSitePermanentError("atomic_root_backend_names_invalid")
        try:
            active_weight = int(os.getenv("STATIC_SITE_ROOT_ACTIVE_WEIGHT") or "100")
        except ValueError as exc:
            raise StaticSitePermanentError("STATIC_SITE_ROOT_ACTIVE_WEIGHT invalid") from exc
        if not 1 <= active_weight <= 1000:
            raise StaticSitePermanentError("STATIC_SITE_ROOT_ACTIVE_WEIGHT out of range")
        receipt_path = Path(
            os.getenv("STATIC_SITE_ROOT_RECEIPT_PATH")
            or "/data/static_site_builder/atomic-root-receipt.json"
        )
        extract = Path(
            extraction_root
            or os.getenv("STATIC_SITE_ROOT_SCRATCH_DIR")
            or "/data/static_site_builder/atomic-root-tmp"
        )
        access_key_id = (os.getenv("STATIC_SITE_ROOT_YC_ACCESS_KEY_ID") or "").strip()
        secret_access_key = (os.getenv("STATIC_SITE_ROOT_YC_SECRET_ACCESS_KEY") or "").strip()
        if mode == "apply" and (not access_key_id or not secret_access_key):
            raise StaticSitePermanentError("atomic_root_storage_credentials_missing")
        public_base_url = (
            os.getenv("STATIC_SITE_ROOT_PUBLIC_BASE_URL") or "https://kenigevents.ru"
        ).strip().rstrip("/")
        parsed_origin = urlsplit(public_base_url)
        if (
            parsed_origin.scheme != "https"
            or not parsed_origin.netloc
            or parsed_origin.path
            or parsed_origin.query
            or parsed_origin.fragment
            or parsed_origin.username
            or parsed_origin.password
        ):
            raise StaticSitePermanentError("STATIC_SITE_ROOT_PUBLIC_BASE_URL invalid")
        if mode == "apply":
            for label, path in (("receipt", receipt_path), ("scratch", extract)):
                resolved = path.expanduser().resolve()
                if not resolved.is_absolute() or resolved == Path("/") or resolved.is_relative_to("/tmp"):
                    raise StaticSitePermanentError(
                        f"atomic_root_{label}_must_use_persistent_storage"
                    )
        return cls(
            mode=mode,
            blue=RootSlot("blue", blue_bucket, blue_backend),
            green=RootSlot("green", green_bucket, green_backend),
            shared_asset_bucket=shared,
            backend_group_id=group,
            folder_id=folder,
            public_base_url=public_base_url,
            receipt_path=receipt_path,
            extraction_root=extract,
            yc_cli=(os.getenv("STATIC_SITE_ROOT_YC_CLI") or "/home/dev/yandex-cloud/bin/yc").strip(),
            endpoint=(
                os.getenv("STATIC_SITE_ROOT_YC_ENDPOINT")
                or "https://storage.yandexcloud.net"
            ).strip(),
            region=(os.getenv("STATIC_SITE_ROOT_YC_REGION") or "ru-central1").strip(),
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            active_weight=active_weight,
        )

    @property
    def slots(self) -> tuple[RootSlot, RootSlot]:
        return self.blue, self.green


@dataclass(frozen=True)
class RoutingState:
    weights: dict[str, int]
    buckets: dict[str, str]
    revision: str


@dataclass(frozen=True)
class SwitchResult:
    operation_ids: tuple[str, ...]
    state: RoutingState


class AlbControlPlane(Protocol):
    def inspect(self) -> RoutingState: ...

    def converge(
        self,
        *,
        target_backend: str,
        source_backend: str,
        active_weight: int,
    ) -> SwitchResult: ...


class RootObjectStore(Protocol):
    def check_bucket(self, bucket: str) -> None: ...

    def list_keys(self, bucket: str) -> list[str]: ...

    def read(self, bucket: str, key: str) -> tuple[bytes, str, Mapping[str, str]]: ...

    def stat(self, bucket: str, key: str) -> tuple[int, str, Mapping[str, str]]: ...

    def write(self, bucket: str, item: Mapping[str, Any]) -> None: ...

    def delete(self, bucket: str, keys: Sequence[str]) -> None: ...


class StableSmoke(Protocol):
    def verify(self, *, release: "CheckedRootRelease", nonce: str) -> None: ...


class YandexAlbCliAdapter:
    """Small mockable adapter over the supported ``yc alb`` control plane."""

    def __init__(self, config: RootPublisherConfig, *, runner: Callable[..., Any] = subprocess.run):
        self.config = config
        self.runner = runner

    def _run(self, *args: str) -> dict[str, Any]:
        command = [
            self.config.yc_cli,
            *args,
            "--folder-id",
            self.config.folder_id,
            "--format",
            "json",
        ]
        try:
            completed = self.runner(
                command,
                check=True,
                capture_output=True,
                text=True,
                timeout=180,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise StaticSiteRetryableError(f"atomic_root_yc_command_failed:{args[0]}") from exc
        try:
            return json.loads(completed.stdout or "{}")
        except json.JSONDecodeError as exc:
            raise StaticSiteRetryableError("atomic_root_yc_json_invalid") from exc

    @staticmethod
    def _backends(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        group = payload.get("http") if isinstance(payload.get("http"), Mapping) else payload
        raw = group.get("backends") or group.get("http_backends") or group.get("httpBackends")
        if not isinstance(raw, list):
            raise StaticSitePermanentError("atomic_root_alb_http_backends_missing")
        return [item for item in raw if isinstance(item, Mapping)]

    @staticmethod
    def _bucket(item: Mapping[str, Any]) -> str:
        raw = item.get("storage_bucket") or item.get("storageBucket")
        if isinstance(raw, Mapping):
            return str(raw.get("bucket") or raw.get("name") or "")
        backend = item.get("backend_type") or item.get("backendType")
        if isinstance(backend, Mapping):
            raw = backend.get("storage_bucket") or backend.get("storageBucket")
            if isinstance(raw, Mapping):
                return str(raw.get("bucket") or raw.get("name") or "")
        return ""

    def inspect(self) -> RoutingState:
        payload = self._run(
            "alb",
            "backend-group",
            "get",
            self.config.backend_group_id,
        )
        weights: dict[str, int] = {}
        buckets: dict[str, str] = {}
        for item in self._backends(payload):
            name = str(item.get("name") or "")
            if not name:
                continue
            raw_weight = item.get("backend_weight", item.get("backendWeight", item.get("weight", 0)))
            weights[name] = int(raw_weight or 0)
            buckets[name] = self._bucket(item)
        revision_payload = json.dumps(
            {"weights": weights, "buckets": buckets},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        return RoutingState(
            weights=weights,
            buckets=buckets,
            revision=hashlib.sha256(revision_payload.encode()).hexdigest(),
        )

    def _set_weight(self, backend: str, weight: int) -> str:
        operation = self._run(
            "alb",
            "backend-group",
            "update-http-backend",
            self.config.backend_group_id,
            "--name",
            backend,
            "--weight",
            str(weight),
            "--async",
        )
        operation_id = str(operation.get("id") or "")
        if not operation_id:
            raise StaticSiteRetryableError("atomic_root_yc_operation_id_missing")
        completed = self._run("operation", "wait", operation_id)
        if completed.get("error"):
            raise StaticSiteRetryableError("atomic_root_yc_operation_failed")
        return operation_id

    def converge(
        self,
        *,
        target_backend: str,
        source_backend: str,
        active_weight: int,
    ) -> SwitchResult:
        before = self.inspect()
        if before.weights.get(target_backend) == active_weight and before.weights.get(source_backend) == 0:
            return SwitchResult((), before)
        operations: list[str] = []
        # Enable the already-complete new tree first.  During this bounded
        # overlap requests may land on old or new, never on an incomplete tree.
        if before.weights.get(target_backend) != active_weight:
            operations.append(self._set_weight(target_backend, active_weight))
        overlap = self.inspect()
        if overlap.weights.get(target_backend) != active_weight:
            raise StaticSiteRetryableError("atomic_root_target_backend_did_not_activate")
        if overlap.weights.get(source_backend) != 0:
            operations.append(self._set_weight(source_backend, 0))
        final = self.inspect()
        if final.weights.get(target_backend) != active_weight or final.weights.get(source_backend) != 0:
            raise StaticSiteRetryableError("atomic_root_alb_weights_did_not_converge")
        return SwitchResult(tuple(operations), final)


class S3RootObjectStore:
    def __init__(self, config: RootPublisherConfig, *, client: Any | None = None):
        if client is None:
            import boto3

            client = boto3.client(
                "s3",
                endpoint_url=config.endpoint,
                region_name=config.region,
                aws_access_key_id=config.access_key_id or None,
                aws_secret_access_key=config.secret_access_key or None,
            )
        self.client = client

    def check_bucket(self, bucket: str) -> None:
        try:
            self.client.head_bucket(Bucket=bucket)
        except Exception as exc:
            raise StaticSiteRetryableError(f"atomic_root_bucket_unavailable:{bucket}") from exc

    def list_keys(self, bucket: str) -> list[str]:
        keys: list[str] = []
        token: str | None = None
        while True:
            kwargs: dict[str, Any] = {"Bucket": bucket, "MaxKeys": 1000}
            if token:
                kwargs["ContinuationToken"] = token
            response = self.client.list_objects_v2(**kwargs)
            keys.extend(str(item.get("Key") or "") for item in response.get("Contents") or [])
            if not response.get("IsTruncated"):
                return sorted(key for key in keys if key)
            token = str(response.get("NextContinuationToken") or "")
            if not token:
                raise StaticSiteRetryableError("atomic_root_bucket_listing_token_missing")

    def read(self, bucket: str, key: str) -> tuple[bytes, str, Mapping[str, str]]:
        response = self.client.get_object(Bucket=bucket, Key=key)
        return (
            response["Body"].read(),
            str(response.get("ContentType") or ""),
            response.get("Metadata") or {},
        )

    def stat(self, bucket: str, key: str) -> tuple[int, str, Mapping[str, str]]:
        response = self.client.head_object(Bucket=bucket, Key=key)
        return (
            int(response.get("ContentLength") or 0),
            str(response.get("ContentType") or ""),
            response.get("Metadata") or {},
        )

    def write(self, bucket: str, item: Mapping[str, Any]) -> None:
        with Path(item["local_path"]).open("rb") as handle:
            self.client.put_object(
                Bucket=bucket,
                Key=str(item["key"]),
                Body=handle,
                ContentType=str(item["content_type"]),
                CacheControl=str(item["cache_control"]),
                Metadata={"sha256": str(item["sha256"])},
            )

    def delete(self, bucket: str, keys: Sequence[str]) -> None:
        for start in range(0, len(keys), 1000):
            batch = keys[start : start + 1000]
            if batch:
                response = self.client.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": [{"Key": key} for key in batch], "Quiet": True},
                )
                if response.get("Errors"):
                    raise StaticSiteRetryableError("atomic_root_stale_delete_failed")


class HttpStableSmoke:
    def __init__(self, public_base_url: str, *, opener: Callable[..., Any] = urlopen):
        self.public_base_url = public_base_url.rstrip("/")
        self.opener = opener

    @staticmethod
    def _url_path(key: str) -> str:
        if key == "index.html":
            return "/"
        if key.endswith("/index.html"):
            return "/" + key[: -len("index.html")]
        return "/" + key

    def verify(self, *, release: "CheckedRootRelease", nonce: str) -> None:
        files = {str(item["key"]): item for item in release.files}
        for key in ROOT_SMOKE_KEYS:
            item = files[key]
            url = f"{self.public_base_url}{self._url_path(key)}?{urlencode({'root-release': nonce})}"
            request = Request(
                url,
                headers={"Cache-Control": "no-cache", "Accept-Encoding": "identity"},
            )
            try:
                with self.opener(request, timeout=30) as response:
                    body = response.read()
                    status = int(response.status)
                    content_type = str(response.headers.get("Content-Type") or "").split(";")[0]
            except HTTPError as exc:
                raise StaticSiteRetryableError(f"atomic_root_smoke_http:{key}:{exc.code}") from exc
            if status != 200 or _sha256_bytes(body) != item["sha256"]:
                raise StaticSiteRetryableError(f"atomic_root_smoke_hash_mismatch:{key}")
            expected_type = str(item["content_type"]).split(";")[0]
            if content_type != expected_type:
                raise StaticSiteRetryableError(f"atomic_root_smoke_mime_mismatch:{key}")


@dataclass(frozen=True)
class CheckedRootRelease:
    build_id: str
    run_id: str
    repo_sha: str
    snapshot_id: str
    manifest_sha256: str
    tree_sha256: str
    root: Path
    files: tuple[dict[str, Any], ...]

    def identity(self, *, slot: RootSlot, healthy: bool = True) -> dict[str, Any]:
        return {
            "slot": slot.name,
            "bucket": slot.bucket,
            "backend": slot.backend,
            "build_id": self.build_id,
            "run_id": self.run_id,
            "repo_sha": self.repo_sha,
            "snapshot_id": self.snapshot_id,
            "manifest_sha256": self.manifest_sha256,
            "tree_sha256": self.tree_sha256,
            "healthy": healthy,
        }


def _safe_extract_root_archive(archive: Path, destination: Path, build_id: str) -> Path:
    expected = f"{build_id}-root/"
    if destination.exists():
        raise StaticSitePermanentError("atomic_root_extract_destination_exists")
    destination.mkdir(parents=True)
    total = 0
    try:
        with tarfile.open(archive, "r:gz") as bundle:
            members = bundle.getmembers()
            if not members or len(members) > 100_000:
                raise StaticSitePermanentError("atomic_root_archive_member_count_invalid")
            for member in members:
                name = member.name.lstrip("./")
                if name.rstrip("/") == expected.rstrip("/") and member.isdir():
                    continue
                if (
                    not name.startswith(expected)
                    or member.issym()
                    or member.islnk()
                    or not (member.isdir() or member.isfile())
                ):
                    raise StaticSitePermanentError(f"atomic_root_archive_path_invalid:{name[:160]}")
                relative = name[len(expected) :]
                parts = Path(relative).parts
                if not relative or any(part in {"", ".", ".."} for part in parts):
                    raise StaticSitePermanentError("atomic_root_archive_relative_path_invalid")
                target = destination.joinpath(*parts)
                if member.isdir():
                    target.mkdir(parents=True, exist_ok=True)
                    continue
                total += int(member.size)
                if total > 2 * 1024 * 1024 * 1024:
                    raise StaticSitePermanentError("atomic_root_archive_unpacked_size_exceeded")
                target.parent.mkdir(parents=True, exist_ok=True)
                source = bundle.extractfile(member)
                if source is None:
                    raise StaticSitePermanentError("atomic_root_archive_member_unreadable")
                with source, target.open("xb") as handle:
                    shutil.copyfileobj(source, handle, length=1024 * 1024)
    except Exception:
        shutil.rmtree(destination, ignore_errors=True)
        raise
    return destination


def validate_root_archive(
    archive_path: str | os.PathLike[str],
    *,
    build_result: Mapping[str, Any],
    extraction_root: str | os.PathLike[str],
    expected_site_origin: str,
) -> CheckedRootRelease:
    """Public validation entry point used by focused release tooling/tests."""

    return _extract_and_validate_root(
        archive_path,
        build_result=build_result,
        extraction_root=extraction_root,
        expected_site_origin=expected_site_origin,
    )


def _extract_and_validate_root(
    archive_path: str | os.PathLike[str],
    *,
    build_result: Mapping[str, Any],
    extraction_root: str | os.PathLike[str],
    expected_site_origin: str,
) -> CheckedRootRelease:
    build_id = str(build_result.get("build_id") or "")
    if not _BUILD_RE.fullmatch(build_id):
        raise StaticSitePermanentError("atomic_root_build_id_invalid")
    extraction = Path(extraction_root)
    extraction.mkdir(parents=True, exist_ok=True)
    archive = Path(archive_path)
    if not archive.is_file():
        raise StaticSitePermanentError("atomic_root_archive_missing")
    free_bytes = shutil.disk_usage(extraction).free
    required_free = max(512 * 1024 * 1024, archive.stat().st_size * 3)
    if free_bytes < required_free:
        raise StaticSiteRetryableError("atomic_root_scratch_capacity_insufficient")
    destination = extraction / f"root-{build_id}-{os.getpid()}-{os.urandom(6).hex()}"
    root = _safe_extract_root_archive(archive, destination, build_id)
    try:
        return _validate_extracted_root(
            root,
            build_result=build_result,
            expected_site_origin=expected_site_origin,
        )
    except Exception:
        shutil.rmtree(root, ignore_errors=True)
        raise


def _validate_extracted_root(
    root: Path,
    *,
    build_result: Mapping[str, Any],
    expected_site_origin: str,
) -> CheckedRootRelease:
    manifest_path = root / "static-release-manifest.json"
    try:
        manifest_bytes = manifest_path.read_bytes()
        manifest = json.loads(manifest_bytes)
    except Exception as exc:
        raise StaticSitePermanentError("atomic_root_manifest_invalid") from exc
    expected = {
        "schema_version": ROOT_MANIFEST_SCHEMA,
        "publication_mode": "artifact_only",
        "site_mode": "production",
        "build_id": str(build_result.get("build_id") or ""),
        "run_id": str(build_result.get("run_id") or ""),
        "repo_sha": str(build_result.get("repo_sha") or ""),
        "base_path": "/",
        "site_origin": expected_site_origin.rstrip("/"),
        "hash_algorithm": "sha256",
    }
    for key, value in expected.items():
        if manifest.get(key) != value:
            raise StaticSitePermanentError(f"atomic_root_manifest_identity_mismatch:{key}")
    result_snapshot = build_result.get("snapshot") if isinstance(build_result.get("snapshot"), Mapping) else {}
    manifest_snapshot = manifest.get("snapshot") if isinstance(manifest.get("snapshot"), Mapping) else {}
    if (
        manifest_snapshot.get("snapshot_id") != result_snapshot.get("snapshot_id")
        or manifest_snapshot.get("sha256") != result_snapshot.get("snapshot_sha256")
    ):
        raise StaticSitePermanentError("atomic_root_manifest_snapshot_mismatch")
    checks = manifest.get("checks") if isinstance(manifest.get("checks"), Mapping) else {}
    for check in ROOT_REQUIRED_CHECKS:
        if checks.get(check) != "ok":
            raise StaticSitePermanentError(f"atomic_root_required_check_missing:{check}")
    raw_files = manifest.get("files")
    if not isinstance(raw_files, list) or not raw_files or len(raw_files) > 100_000:
        raise StaticSitePermanentError("atomic_root_manifest_files_invalid")
    seen: set[str] = set()
    files: list[dict[str, Any]] = []
    for raw in raw_files:
        if not isinstance(raw, Mapping):
            raise StaticSitePermanentError("atomic_root_manifest_file_invalid")
        key = _safe_key(raw.get("key"))
        if key in seen:
            raise StaticSitePermanentError(f"atomic_root_manifest_duplicate:{key}")
        seen.add(key)
        sha256 = str(raw.get("sha256") or "")
        content_type = str(raw.get("content_type") or "")
        cache_control = str(raw.get("cache_control") or "")
        try:
            size = int(raw.get("size"))
        except (TypeError, ValueError) as exc:
            raise StaticSitePermanentError(f"atomic_root_manifest_size_invalid:{key}") from exc
        if (
            not _HASH_RE.fullmatch(sha256)
            or size < 0
            or content_type != _content_type(key)
            or not cache_control
        ):
            raise StaticSitePermanentError(f"atomic_root_manifest_metadata_invalid:{key}")
        local_path = root.joinpath(*key.split("/"))
        if (
            not local_path.is_file()
            or local_path.stat().st_size != size
            or _sha256_file(local_path) != sha256
        ):
            raise StaticSitePermanentError(f"atomic_root_file_hash_mismatch:{key}")
        files.append(
            {
                "key": key,
                "sha256": sha256,
                "size": size,
                "content_type": content_type,
                "cache_control": cache_control,
                "local_path": local_path,
            }
        )
    if _tree_sha256(files) != manifest.get("tree_sha256"):
        raise StaticSitePermanentError("atomic_root_tree_hash_mismatch")
    missing_routes = sorted(set(ROOT_REQUIRED_ROUTES) - seen)
    if missing_routes:
        raise StaticSitePermanentError("atomic_root_required_routes_missing:" + ",".join(missing_routes))
    try:
        pwa = json.loads((root / "manifest.webmanifest").read_text(encoding="utf-8"))
    except Exception as exc:
        raise StaticSitePermanentError("atomic_root_pwa_manifest_invalid") from exc
    pwa_item = next(item for item in files if item["key"] == "manifest.webmanifest")
    if pwa_item["content_type"] != "application/manifest+json; charset=utf-8":
        raise StaticSitePermanentError("atomic_root_pwa_mime_invalid")
    if (
        pwa.get("id") != "/"
        or pwa.get("scope") != "/"
        or pwa.get("start_url") != "/"
        or pwa.get("display") != "standalone"
        or not str(pwa.get("name") or "").strip()
        or not str(pwa.get("short_name") or "").strip()
    ):
        raise StaticSitePermanentError("atomic_root_pwa_contract_invalid")
    icons = pwa.get("icons")
    if not isinstance(icons, list):
        raise StaticSitePermanentError("atomic_root_pwa_icons_invalid")
    icon_sizes = {str(item.get("sizes") or "") for item in icons if isinstance(item, Mapping)}
    if not {"192x192", "512x512"}.issubset(icon_sizes):
        raise StaticSitePermanentError("atomic_root_pwa_icon_sizes_missing")
    for icon in icons:
        if not isinstance(icon, Mapping):
            raise StaticSitePermanentError("atomic_root_pwa_icon_invalid")
        src = str(icon.get("src") or "")
        if not src.startswith("/") or src.startswith("//") or src[1:] not in seen:
            raise StaticSitePermanentError("atomic_root_pwa_icon_file_missing")
    try:
        focus_pwa = json.loads(
            (root / "fokus-gruppa/manifest.webmanifest").read_text(encoding="utf-8")
        )
    except Exception as exc:
        raise StaticSitePermanentError("atomic_root_focus_pwa_manifest_invalid") from exc
    focus_pwa_item = next(
        item for item in files if item["key"] == "fokus-gruppa/manifest.webmanifest"
    )
    if focus_pwa_item["content_type"] != "application/manifest+json; charset=utf-8":
        raise StaticSitePermanentError("atomic_root_focus_pwa_mime_invalid")
    if (
        focus_pwa.get("id") != "/fokus-gruppa/pwa"
        or focus_pwa.get("scope") != "/"
        or focus_pwa.get("start_url") != "/fokus-gruppa/priglashenie/?launch=pwa"
        or focus_pwa.get("display") != "standalone"
        or not str(focus_pwa.get("name") or "").strip()
        or not str(focus_pwa.get("short_name") or "").strip()
    ):
        raise StaticSitePermanentError("atomic_root_focus_pwa_contract_invalid")
    focus_icons = focus_pwa.get("icons")
    if not isinstance(focus_icons, list):
        raise StaticSitePermanentError("atomic_root_focus_pwa_icons_invalid")
    focus_icon_sizes = {
        str(item.get("sizes") or "") for item in focus_icons if isinstance(item, Mapping)
    }
    if not {"192x192", "512x512"}.issubset(focus_icon_sizes):
        raise StaticSitePermanentError("atomic_root_focus_pwa_icon_sizes_missing")
    for icon in focus_icons:
        if not isinstance(icon, Mapping):
            raise StaticSitePermanentError("atomic_root_focus_pwa_icon_invalid")
        src = str(icon.get("src") or "")
        if not src.startswith("/") or src.startswith("//") or src[1:] not in seen:
            raise StaticSitePermanentError("atomic_root_focus_pwa_icon_file_missing")
    manifest_sha = _sha256_bytes(manifest_bytes)
    manifest_item = {
        "key": "static-release-manifest.json",
        "sha256": manifest_sha,
        "size": len(manifest_bytes),
        "content_type": "application/json; charset=utf-8",
        "cache_control": "public, max-age=60, must-revalidate",
        "local_path": manifest_path,
    }
    return CheckedRootRelease(
        build_id=expected["build_id"],
        run_id=expected["run_id"],
        repo_sha=expected["repo_sha"],
        snapshot_id=str(manifest_snapshot.get("snapshot_id") or ""),
        manifest_sha256=manifest_sha,
        tree_sha256=str(manifest.get("tree_sha256") or ""),
        root=root,
        files=tuple([*files, manifest_item]),
    )


def _manifest_identity(store: RootObjectStore, slot: RootSlot) -> dict[str, Any] | None:
    try:
        body, content_type, _metadata = store.read(slot.bucket, "static-release-manifest.json")
        manifest = json.loads(body)
    except Exception:
        return None
    if (
        manifest.get("schema_version") != ROOT_MANIFEST_SCHEMA
        or manifest.get("site_mode") != "production"
        or not _HASH_RE.fullmatch(str(manifest.get("tree_sha256") or ""))
        or not isinstance(manifest.get("files"), list)
    ):
        return None
    expected_keys = {"static-release-manifest.json"}
    files: list[dict[str, Any]] = []
    try:
        for raw in manifest["files"]:
            if not isinstance(raw, Mapping):
                return None
            key = _safe_key(raw.get("key"))
            sha256 = str(raw.get("sha256") or "")
            size = int(raw.get("size"))
            expected_type = str(raw.get("content_type") or "")
            if key in expected_keys or not _HASH_RE.fullmatch(sha256) or size < 0 or not expected_type:
                return None
            object_size, object_type, object_metadata = store.stat(slot.bucket, key)
            if (
                object_size != size
                or object_type != expected_type
                or str(object_metadata.get("sha256") or "") != sha256
            ):
                return None
            expected_keys.add(key)
            files.append({"key": key, "sha256": sha256, "size": size})
    except Exception:
        return None
    if _tree_sha256(files) != manifest.get("tree_sha256"):
        return None
    try:
        if set(store.list_keys(slot.bucket)) != expected_keys:
            return None
    except Exception:
        return None
    return {
        "slot": slot.name,
        "bucket": slot.bucket,
        "backend": slot.backend,
        "build_id": str(manifest.get("build_id") or ""),
        "run_id": str(manifest.get("run_id") or ""),
        "repo_sha": str(manifest.get("repo_sha") or ""),
        "snapshot_id": str((manifest.get("snapshot") or {}).get("snapshot_id") or ""),
        "manifest_sha256": _sha256_bytes(body),
        "tree_sha256": str(manifest.get("tree_sha256") or ""),
        "content_type": content_type,
        "healthy": True,
    }


def _inventory(config: RootPublisherConfig, control: AlbControlPlane, store: RootObjectStore) -> RoutingState:
    for slot in config.slots:
        store.check_bucket(slot.bucket)
    state = control.inspect()
    expected_backends = {slot.backend for slot in config.slots}
    if set(state.weights) != expected_backends or set(state.buckets) != expected_backends:
        raise StaticSitePermanentError("atomic_root_alb_backend_inventory_mismatch")
    expected_buckets = {slot.backend: slot.bucket for slot in config.slots}
    if state.buckets != expected_buckets:
        raise StaticSitePermanentError("atomic_root_alb_bucket_mapping_mismatch")
    positive = [name for name, weight in state.weights.items() if int(weight) > 0]
    if not positive:
        raise StaticSitePermanentError("atomic_root_alb_has_no_active_backend")
    return state


def _verify_remote_tree(store: RootObjectStore, slot: RootSlot, release: CheckedRootRelease) -> None:
    expected = {str(item["key"]): item for item in release.files}
    actual = set(store.list_keys(slot.bucket))
    if actual != set(expected):
        raise StaticSiteRetryableError("atomic_root_remote_inventory_mismatch")
    for key, item in expected.items():
        body, content_type, metadata = store.read(slot.bucket, key)
        if len(body) != int(item["size"]) or _sha256_bytes(body) != item["sha256"]:
            raise StaticSiteRetryableError(f"atomic_root_remote_hash_mismatch:{key}")
        if content_type != item["content_type"]:
            raise StaticSiteRetryableError(f"atomic_root_remote_mime_mismatch:{key}")
        metadata_hash = str(metadata.get("sha256") or "")
        if metadata_hash and metadata_hash != item["sha256"]:
            raise StaticSiteRetryableError(f"atomic_root_remote_metadata_mismatch:{key}")


def _reconcile_inactive(
    store: RootObjectStore,
    slot: RootSlot,
    release: CheckedRootRelease,
) -> dict[str, Any]:
    expected = {str(item["key"]): item for item in release.files}
    before = set(store.list_keys(slot.bucket))
    uploaded = 0
    reused = 0
    for key, item in expected.items():
        exact = False
        if key in before:
            try:
                size, content_type, metadata = store.stat(slot.bucket, key)
                exact = (
                    size == int(item["size"])
                    and content_type == item["content_type"]
                    and str(metadata.get("sha256") or "") == item["sha256"]
                )
            except Exception:
                exact = False
        if exact:
            reused += 1
        else:
            store.write(slot.bucket, item)
            uploaded += 1
    # The complete desired tree is verified before any stale inactive bytes
    # are deleted.  No deletion language exists for the active bucket.
    for key, item in expected.items():
        body, content_type, metadata = store.read(slot.bucket, key)
        if (
            len(body) != int(item["size"])
            or _sha256_bytes(body) != item["sha256"]
            or content_type != item["content_type"]
            or str(metadata.get("sha256") or item["sha256"]) != item["sha256"]
        ):
            raise StaticSiteRetryableError(f"atomic_root_staged_verification_failed:{key}")
    stale = sorted(before - set(expected))
    store.delete(slot.bucket, stale)
    _verify_remote_tree(store, slot, release)
    return {"uploaded": uploaded, "reused": reused, "stale_deleted": len(stale)}


def _atomic_receipt(path: Path, receipt: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{os.urandom(6).hex()}.tmp"
    )
    with temporary.open("x", encoding="utf-8") as handle:
        json.dump(receipt, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)
    directory_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _slot_by_backend(config: RootPublisherConfig, backend: str) -> RootSlot:
    for slot in config.slots:
        if slot.backend == backend:
            return slot
    raise StaticSitePermanentError("atomic_root_backend_not_configured")


def publish_atomic_root_archive(
    archive_path: str | os.PathLike[str],
    *,
    build_result: Mapping[str, Any],
    config: RootPublisherConfig,
    control: AlbControlPlane,
    store: RootObjectStore,
    smoke: StableSmoke,
) -> dict[str, Any]:
    """Plan or publish one checked root without ever mutating the active slot."""

    config.receipt_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = config.receipt_path.with_suffix(config.receipt_path.suffix + ".lock")
    with lock_path.open("a+b") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise StaticSiteRetryableError("atomic_root_promotion_locked") from exc
        release = _extract_and_validate_root(
            archive_path,
            build_result=build_result,
            extraction_root=config.extraction_root,
            expected_site_origin=config.public_base_url,
        )
        try:
            state = _inventory(config, control, store)
            positive = [name for name, weight in state.weights.items() if int(weight) > 0]
            identities = {slot.name: _manifest_identity(store, slot) for slot in config.slots}
            for backend in positive:
                slot = _slot_by_backend(config, backend)
                if identities.get(slot.name) is None:
                    raise StaticSitePermanentError(
                        f"atomic_root_active_tree_incomplete:{slot.name}"
                    )
            if len(positive) == 1 and state.weights[positive[0]] != config.active_weight:
                raise StaticSitePermanentError("atomic_root_active_weight_drift")
            if config.mode == "plan" and len(positive) != 1:
                raise StaticSitePermanentError("atomic_root_plan_requires_one_active_backend")
            desired_slots = [
                slot
                for slot in config.slots
                if identities[slot.name]
                and identities[slot.name]["manifest_sha256"] == release.manifest_sha256
                and identities[slot.name]["tree_sha256"] == release.tree_sha256
            ]
            sole_active = positive[0] if len(positive) == 1 else None
            if sole_active and any(slot.backend == sole_active for slot in desired_slots):
                active_slot = _slot_by_backend(config, sole_active)
                other = next(slot for slot in config.slots if slot.name != active_slot.name)
                if config.mode == "apply":
                    try:
                        smoke.verify(release=release, nonce=release.manifest_sha256[:16])
                    except Exception as smoke_error:
                        former_identity = identities.get(other.name)
                        if former_identity is None:
                            raise StaticSitePermanentError(
                                "atomic_root_noop_smoke_failed_without_previous"
                            ) from smoke_error
                        rollback = control.converge(
                            target_backend=other.backend,
                            source_backend=active_slot.backend,
                            active_weight=config.active_weight,
                        )
                        receipt = {
                            "schema_version": ROOT_RECEIPT_SCHEMA,
                            "status": "rolled_back",
                            "mode": "apply",
                            "operation_id": (
                                rollback.operation_ids[-1]
                                if rollback.operation_ids
                                else None
                            ),
                            "operation_ids": list(rollback.operation_ids),
                            "revision": rollback.state.revision,
                            "current": former_identity,
                            "previous": release.identity(
                                slot=active_slot, healthy=False
                            ),
                            "cleanup": {"slot": None, "stale_deleted": 0},
                            "failure_class": type(smoke_error).__name__,
                            "verified_at": iso_utc(),
                        }
                        _atomic_receipt(config.receipt_path, receipt)
                        return receipt
                receipt = {
                    "schema_version": ROOT_RECEIPT_SCHEMA,
                    "status": "noop" if config.mode == "apply" else "planned_noop",
                    "mode": config.mode,
                    "operation_id": None,
                    "operation_ids": [],
                    "revision": state.revision,
                    "current": release.identity(slot=active_slot),
                    "previous": identities.get(other.name),
                    "cleanup": {"slot": None, "stale_deleted": 0},
                    "verified_at": iso_utc(),
                }
                _atomic_receipt(config.receipt_path, receipt)
                return receipt

            if len(positive) == 1:
                source_slot = _slot_by_backend(config, positive[0])
                target_slot = next(slot for slot in config.slots if slot.name != source_slot.name)
                # It is impossible for the selected upload target to be active.
                if state.weights.get(target_slot.backend, 0) > 0:
                    raise StaticSitePermanentError("atomic_root_active_bucket_write_rejected")
                if config.mode == "plan":
                    receipt = {
                        "schema_version": ROOT_RECEIPT_SCHEMA,
                        "status": "planned",
                        "mode": "plan",
                        "operation_id": None,
                        "operation_ids": [],
                        "revision": state.revision,
                        "current": identities.get(source_slot.name),
                        "previous": identities.get(target_slot.name),
                        "target": release.identity(slot=target_slot),
                        "cleanup": {"slot": target_slot.name, "stale_deleted": 0},
                        "verified_at": iso_utc(),
                    }
                    _atomic_receipt(config.receipt_path, receipt)
                    return receipt
                cleanup = _reconcile_inactive(store, target_slot, release)
            else:
                # Recover only the bounded interrupted-switch shape: exactly
                # two positive backends and one already-complete desired tree.
                if len(positive) != 2 or len(desired_slots) != 1:
                    raise StaticSitePermanentError("atomic_root_alb_ambiguous_convergence")
                target_slot = desired_slots[0]
                source_slot = next(slot for slot in config.slots if slot.name != target_slot.name)
                _verify_remote_tree(store, target_slot, release)
                cleanup = {"uploaded": 0, "reused": len(release.files), "stale_deleted": 0}
            previous_identity = identities.get(source_slot.name)
            if previous_identity is None:
                raise StaticSitePermanentError("atomic_root_active_manifest_missing")
            switched = control.converge(
                target_backend=target_slot.backend,
                source_backend=source_slot.backend,
                active_weight=config.active_weight,
            )
            try:
                smoke.verify(release=release, nonce=release.manifest_sha256[:16])
            except Exception as smoke_error:
                rollback = control.converge(
                    target_backend=source_slot.backend,
                    source_backend=target_slot.backend,
                    active_weight=config.active_weight,
                )
                receipt = {
                    "schema_version": ROOT_RECEIPT_SCHEMA,
                    "status": "rolled_back",
                    "mode": "apply",
                    "operation_id": (rollback.operation_ids[-1] if rollback.operation_ids else None),
                    "operation_ids": [*switched.operation_ids, *rollback.operation_ids],
                    "revision": rollback.state.revision,
                    "current": previous_identity,
                    "previous": release.identity(slot=target_slot, healthy=False),
                    "cleanup": {"slot": target_slot.name, **cleanup},
                    "failure_class": type(smoke_error).__name__,
                    "verified_at": iso_utc(),
                }
                _atomic_receipt(config.receipt_path, receipt)
                return receipt
            receipt = {
                "schema_version": ROOT_RECEIPT_SCHEMA,
                "status": "promoted",
                "mode": "apply",
                "operation_id": (switched.operation_ids[-1] if switched.operation_ids else None),
                "operation_ids": list(switched.operation_ids),
                "revision": switched.state.revision,
                "current": release.identity(slot=target_slot),
                "previous": previous_identity,
                "cleanup": {"slot": target_slot.name, **cleanup},
                "verified_at": iso_utc(),
            }
            _atomic_receipt(config.receipt_path, receipt)
            return receipt
        finally:
            shutil.rmtree(release.root, ignore_errors=True)


def publish_atomic_root_from_env(
    archive_path: str | os.PathLike[str],
    *,
    build_result: Mapping[str, Any],
    extraction_root: str | os.PathLike[str] | None = None,
) -> dict[str, Any]:
    config = RootPublisherConfig.from_env(extraction_root=extraction_root)
    control = YandexAlbCliAdapter(config)
    store = S3RootObjectStore(config)
    smoke = HttpStableSmoke(config.public_base_url)
    return publish_atomic_root_archive(
        archive_path,
        build_result=build_result,
        config=config,
        control=control,
        store=store,
        smoke=smoke,
    )


def _cli() -> int:
    parser = argparse.ArgumentParser(description="Plan/apply atomic static-site root publication")
    parser.add_argument("--archive", required=True)
    parser.add_argument("--result", required=True)
    parser.add_argument("--mode", choices=("plan", "apply"), default=None)
    args = parser.parse_args()
    if args.mode:
        os.environ["STATIC_SITE_ROOT_PROMOTION_MODE"] = args.mode
    result = json.loads(Path(args.result).read_text(encoding="utf-8"))
    receipt = publish_atomic_root_from_env(args.archive, build_result=result)
    # Receipt has no credentials or bearer tokens and is safe for operator IO.
    print(json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if receipt.get("status") not in {"rolled_back"} else 2


if __name__ == "__main__":
    raise SystemExit(_cli())
