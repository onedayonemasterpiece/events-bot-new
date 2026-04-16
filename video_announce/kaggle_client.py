from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import shutil
import tempfile
import time
from fnmatch import fnmatch
from pathlib import Path
from typing import Any, Iterable

KaggleApi = None  # type: ignore[assignment]
_KAGGLE_IMPORT_ERROR: Exception | None = None

try:  # pragma: no cover - optional dependency
    from kaggle.api.kaggle_api_extended import KaggleApi as ImportedKaggleApi
except SystemExit as exc:  # pragma: no cover - missing credentials trigger sys.exit
    _KAGGLE_IMPORT_ERROR = exc
except Exception as exc:  # pragma: no cover - handled at runtime
    _KAGGLE_IMPORT_ERROR = exc
else:
    KaggleApi = ImportedKaggleApi  # type: ignore[assignment]
    _KAGGLE_IMPORT_ERROR = None

from models import Event

logger = logging.getLogger(__name__)

# Root directory containing all kernel folders
KERNELS_ROOT_PATH = Path(__file__).resolve().parent.parent / "kaggle"
# Default kernel (first local one added)
DEFAULT_KERNEL_PATH = KERNELS_ROOT_PATH / "VideoAfishaEventsBot"
# Prefix to identify local kernels in kernel_ref
LOCAL_KERNEL_PREFIX = "local:"
DEFAULT_KERNEL_IGNORE_PATTERNS = (
    ".kaggleignore",
    ".ipynb_checkpoints/",
    "__pycache__/",
    ".pytest_cache/",
    "*.pyc",
    ".DS_Store",
    "Thumbs.db",
    "output/",
    "output*/",
    "frames/",
    "frames*/",
    "render/",
    "render*/",
    "sequence/",
    "sequence*/",
)


async def await_kernel_dataset_sources(
    client: "KaggleClient",
    kernel_ref: str,
    expected_sources: list[str],
    *,
    timeout_seconds: int = 120,
    poll_interval_seconds: int = 10,
) -> dict[str, Any]:
    expected_clean = [str(item).strip() for item in expected_sources if str(item).strip()]
    if not expected_clean:
        return {}

    started = time.monotonic()
    deadline = started + max(1, int(timeout_seconds))
    last_meta: dict[str, Any] | None = None
    last_error: str | None = None

    while time.monotonic() < deadline:
        try:
            matched, meta = await asyncio.to_thread(
                client.kernel_has_dataset_sources,
                kernel_ref,
                expected_clean,
            )
            last_meta = meta or {}
            if matched:
                return last_meta
            last_error = None
        except Exception as exc:
            last_error = str(exc) or exc.__class__.__name__
        await asyncio.sleep(max(1, int(poll_interval_seconds)))

    actual_sources = list((last_meta or {}).get("dataset_sources") or [])
    details = (
        f"expected={expected_clean} actual={actual_sources}"
        if actual_sources
        else f"expected={expected_clean}"
    )
    if last_error:
        details = f"{details} last_error={last_error}"
    raise RuntimeError(
        f"Kaggle kernel metadata did not bind expected datasets in time ({details})"
    )


async def await_dataset_ready(
    client: "KaggleClient",
    dataset_ref: str,
    *,
    timeout_seconds: int = 180,
    poll_interval_seconds: int = 5,
    expected_files: list[str] | None = None,
) -> dict[str, Any]:
    expected_clean = [
        str(item).strip() for item in (expected_files or []) if str(item).strip()
    ]
    started = time.monotonic()
    deadline = started + max(1, int(timeout_seconds))
    last_status: str | None = None
    last_files: list[str] = []
    last_error: str | None = None

    while time.monotonic() < deadline:
        try:
            status = await asyncio.to_thread(client.dataset_status, dataset_ref)
            files = await asyncio.to_thread(
                client.dataset_list_files,
                dataset_ref,
                page_size=max(20, len(expected_clean) + 5),
            )
            file_names = [
                str(item.get("name") or "").strip()
                for item in files
                if str(item.get("name") or "").strip()
            ]
            status_ready = status.strip().lower() == "ready"
            files_ready = all(name in file_names for name in expected_clean)
            logger.info(
                "kaggle: dataset ready check dataset=%s status=%s files=%s ready=%s",
                dataset_ref,
                status,
                file_names,
                status_ready and files_ready,
            )
            last_status = status
            last_files = file_names
            last_error = None
            if status_ready and files_ready:
                return {
                    "status": status,
                    "files": file_names,
                }
        except Exception as exc:
            last_error = str(exc) or exc.__class__.__name__
            logger.warning(
                "kaggle: dataset ready check error dataset=%s err=%s",
                dataset_ref,
                last_error,
            )
        await asyncio.sleep(max(1, int(poll_interval_seconds)))

    details = f"dataset={dataset_ref}"
    if last_status:
        details = f"{details} status={last_status}"
    if last_files:
        details = f"{details} files={last_files}"
    if expected_clean:
        details = f"{details} expected_files={expected_clean}"
    if last_error:
        details = f"{details} last_error={last_error}"
    raise RuntimeError(f"Kaggle dataset did not become ready in time ({details})")


def _response_error_suffix(exc: Exception) -> str:
    response = getattr(exc, "response", None)
    if response is None:
        return ""
    status_code = getattr(response, "status_code", None)
    try:
        body = str(response.text or "").strip()
    except Exception:
        body = ""
    parts: list[str] = []
    if status_code is not None:
        parts.append(f"status={status_code}")
    if body:
        parts.append(body[:800])
    return f" ({'; '.join(parts)})" if parts else ""


def _should_force_gpu_for_local_kernel(folder_name: str, meta_data: dict[str, Any]) -> bool:
    if str(folder_name or "").strip().casefold() == "crumplevideo":
        return True
    kernel_id = str(meta_data.get("id") or "").strip().casefold()
    slug = str(meta_data.get("slug") or "").strip().casefold()
    title = str(meta_data.get("title") or "").strip().casefold()
    haystack = " ".join(part for part in (kernel_id, slug, title) if part)
    return "crumple-video" in haystack or "crumple video" in haystack


def _load_kernel_ignore_patterns(base_path: Path) -> list[str]:
    patterns = list(DEFAULT_KERNEL_IGNORE_PATTERNS)
    ignore_path = base_path / ".kaggleignore"
    if not ignore_path.exists():
        return patterns

    for raw_line in ignore_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        patterns.append(line)
    return patterns


def _matches_kernel_ignore(rel_path: Path, *, is_dir: bool, patterns: Iterable[str]) -> bool:
    rel = rel_path.as_posix()
    name = rel_path.name
    for pattern in patterns:
        dir_only = pattern.endswith("/")
        normalized = pattern.rstrip("/")
        if not normalized:
            continue
        if dir_only and not is_dir:
            continue
        if fnmatch(rel, normalized) or fnmatch(name, normalized):
            return True
    return False


def _copy_kernel_tree(src_root: Path, dst_root: Path) -> None:
    patterns = _load_kernel_ignore_patterns(src_root)

    def _copy_dir(src_dir: Path, dst_dir: Path, rel_dir: Path) -> None:
        dst_dir.mkdir(parents=True, exist_ok=True)
        for item in sorted(src_dir.iterdir(), key=lambda p: p.name):
            rel_path = rel_dir / item.name
            if _matches_kernel_ignore(rel_path, is_dir=item.is_dir(), patterns=patterns):
                logger.info("kaggle: skipping ignored kernel path=%s", rel_path.as_posix())
                continue
            dest = dst_dir / item.name
            if item.is_dir():
                _copy_dir(item, dest, rel_path)
            else:
                shutil.copy2(item, dest)

    _copy_dir(src_root, dst_root, Path())


def _prune_kernel_tree(root: Path) -> None:
    patterns = _load_kernel_ignore_patterns(root)
    if not patterns:
        return

    paths = sorted(
        (p for p in root.rglob("*")),
        key=lambda p: (len(p.relative_to(root).parts), p.as_posix()),
        reverse=True,
    )
    for path in paths:
        rel_path = path.relative_to(root)
        if not _matches_kernel_ignore(rel_path, is_dir=path.is_dir(), patterns=patterns):
            continue
        logger.info("kaggle: pruning ignored kernel path=%s", rel_path.as_posix())
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        elif path.exists():
            path.unlink()



def list_local_kernels() -> list[dict]:
    """List all valid kernel folders in the repository's kaggle/ directory.
    
    Returns list of dicts with 'ref', 'title', 'path' keys.
    A valid kernel folder must contain kernel-metadata.json.
    """
    if not KERNELS_ROOT_PATH.exists():
        return []
    
    kernels = []
    for folder in KERNELS_ROOT_PATH.iterdir():
        if not folder.is_dir():
            continue
        meta_path = folder / "kernel-metadata.json"
        if not meta_path.exists():
            continue
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            title = meta.get("title") or folder.name
            # Use local: prefix to distinguish from Kaggle kernels
            ref = f"{LOCAL_KERNEL_PREFIX}{folder.name}"
            kernels.append({
                "ref": ref,
                "title": title,
                "path": str(folder),
                "is_local": True,
                "id": meta.get("id"),
                "slug": meta.get("slug"),
            })
        except Exception:
            logger.warning("Failed to parse kernel metadata in %s", folder)
            continue
    return kernels


def _kernel_slug(kernel_ref: str) -> str:
    ref = str(kernel_ref or "").strip()
    if not ref:
        return ""
    if ref.startswith(LOCAL_KERNEL_PREFIX):
        return ref[len(LOCAL_KERNEL_PREFIX):]
    if "/" in ref:
        return ref.rsplit("/", 1)[-1]
    return ref


def find_local_kernel(kernel_ref: str) -> dict[str, Any] | None:
    """Return the repo-local kernel matching a requested local or Kaggle ref."""
    normalized_ref = str(kernel_ref or "").strip()
    if not normalized_ref:
        return None

    requested_slug = _kernel_slug(normalized_ref).casefold()
    for kernel in list_local_kernels():
        local_ref = str(kernel.get("ref") or "").strip()
        if local_ref and local_ref == normalized_ref:
            return kernel
        local_id = str(kernel.get("id") or "").strip()
        if local_id and local_id == normalized_ref:
            return kernel
        local_slug = str(kernel.get("slug") or "").strip().casefold()
        if requested_slug and local_slug and local_slug == requested_slug:
            return kernel
    return None


class KaggleClient:
    """Helper for interacting with Kaggle kernels and datasets.

    Besides providing lightweight scoring for local ranking, this client wraps
    a few Kaggle API calls needed to publish kernels that render the video
    announcement.
    """

    def __init__(self, seed: int | None = None):
        self._rand = random.Random(seed)
        self._api: KaggleApi | None = None

    # --- Local scoring fallback used in selection.py ---
    def score(self, events: Iterable[Event]) -> dict[int, float]:
        scores: dict[int, float] = {}
        for e in events:
            weight = e.video_include_count or 0
            weight += min(e.photo_count, 4) * 0.5
            if e.is_free:
                weight += 0.25
            rarity = 1.0 / (1 + (len(e.topics or []))) if hasattr(e, "topics") else 1.0
            jitter = self._rand.random() * 0.1
            scores[e.id] = round(weight + rarity + jitter, 3)
        return scores

    def rank(self, events: Iterable[Event]) -> list[Event]:
        scored = self.score(events)
        return sorted(
            events,
            key=lambda ev: (-scored.get(ev.id, 0.0), ev.date, ev.time, ev.id),
        )

    # --- Kaggle API helpers ---
    def _get_api(self) -> KaggleApi:
        if self._api is None:
            if KaggleApi is None:
                raise RuntimeError(
                    "Kaggle API is unavailable. Install kaggle and configure credentials."
                ) from _KAGGLE_IMPORT_ERROR
            api = KaggleApi()
            api.authenticate()
            self._api = api
        return self._api

    def create_dataset(
        self,
        folder: str | Path,
        *,
        public: bool = False,
        quiet: bool = True,
        convert_to_csv: bool = False,
        dir_mode: str = "zip",
    ) -> None:
        api = self._get_api()
        logger.info("kaggle: creating dataset from folder=%s", folder)
        try:
            api.dataset_create_new(
                str(folder),
                public=public,
                quiet=quiet,
                convert_to_csv=convert_to_csv,
                dir_mode=dir_mode,
            )
        except Exception as exc:
            raise RuntimeError(
                "Kaggle dataset_create_new failed"
                + _response_error_suffix(exc)
            ) from exc
        logger.info("kaggle: dataset created successfully from folder=%s", folder)

    def create_dataset_version(
        self,
        folder: str | Path,
        *,
        version_notes: str = "update",
        quiet: bool = True,
        convert_to_csv: bool = False,
        delete_old_versions: bool = False,
        dir_mode: str = "zip",
    ) -> None:
        api = self._get_api()
        logger.info(
            "kaggle: creating dataset version folder=%s notes=%s",
            folder,
            version_notes,
        )
        try:
            api.dataset_create_version(
                str(folder),
                version_notes=version_notes,
                quiet=quiet,
                convert_to_csv=convert_to_csv,
                delete_old_versions=delete_old_versions,
                dir_mode=dir_mode,
            )
        except Exception as exc:
            raise RuntimeError(
                "Kaggle dataset_create_version failed"
                + _response_error_suffix(exc)
            ) from exc
        logger.info("kaggle: dataset version created successfully folder=%s", folder)

    def dataset_status(self, dataset: str) -> str:
        api = self._get_api()
        logger.info("kaggle: dataset status dataset=%s", dataset)
        return str(api.dataset_status(dataset))

    def dataset_list_files(self, dataset: str, *, page_size: int = 20) -> list[dict[str, Any]]:
        api = self._get_api()
        logger.info("kaggle: dataset list files dataset=%s", dataset)
        result: list[dict[str, Any]] = []
        seen_names: set[str] = set()
        next_page_token: str | None = None

        while True:
            response = api.dataset_list_files(
                dataset,
                page_token=next_page_token,
                page_size=page_size,
            )
            files = getattr(response, "files", None)
            if files is None and isinstance(response, list):
                files = response
            for item in files or []:
                name = getattr(item, "name", None) or str(item)
                if name in seen_names:
                    continue
                seen_names.add(name)
                result.append(
                    {
                        "name": name,
                        "totalBytes": getattr(item, "totalBytes", None),
                        "creationDate": getattr(item, "creationDate", None),
                    }
                )
            next_page_token = (
                getattr(response, "nextPageToken", None)
                or getattr(response, "next_page_token", None)
                or None
            )
            if not next_page_token or isinstance(response, list):
                break
        logger.info(
            "kaggle: dataset files dataset=%s names=%s",
            dataset,
            [entry.get("name") for entry in result],
        )
        return result

    def delete_dataset(self, dataset: str, *, no_confirm: bool = True) -> None:
        api = self._get_api()
        if "/" in dataset:
            owner_slug, dataset_slug = dataset.split("/", 1)
        else:
            owner_slug = os.getenv("KAGGLE_USERNAME") or ""
            dataset_slug = dataset
        try:
            api.dataset_delete(owner_slug, dataset_slug, no_confirm=no_confirm)
        except Exception as exc:
            raise RuntimeError(
                f"Kaggle dataset_delete failed for {owner_slug}/{dataset_slug}"
                + _response_error_suffix(exc)
            ) from exc

    def push_kernel(
        self,
        *,
        dataset_sources: list[str] | None = None,
        kernel_path: str | Path | None = None,
        timeout: str | None = None,
    ) -> None:
        base_path = Path(kernel_path) if kernel_path else DEFAULT_KERNEL_PATH
        if not base_path.exists():
            raise FileNotFoundError(f"Kernel path not found: {base_path}")
        logger.info("kaggle: preparing kernel push from %s", base_path.resolve())
        api = self._get_api()
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            _copy_kernel_tree(base_path, tmp_path)
            meta_path = tmp_path / "kernel-metadata.json"
            meta_data = json.loads(meta_path.read_text(encoding="utf-8"))
            username = (os.getenv("KAGGLE_USERNAME") or "").strip()
            kernel_id = str(meta_data.get("id") or "").strip()
            if username and kernel_id:
                if "/" in kernel_id:
                    owner, slug = kernel_id.split("/", 1)
                else:
                    owner, slug = "", kernel_id
                if slug and owner != username:
                    new_id = f"{username}/{slug}"
                    logger.info(
                        "kaggle: overriding kernel owner old_id=%s new_id=%s",
                        kernel_id,
                        new_id,
                    )
                    meta_data["id"] = new_id
            if dataset_sources is not None:
                meta_data["dataset_sources"] = dataset_sources
                meta_path.write_text(json.dumps(meta_data, ensure_ascii=False, indent=2))
            files = sorted(
                (f.relative_to(tmp_path).as_posix(), f.stat().st_size)
                for f in tmp_path.rglob("*")
                if f.is_file()
            )
            logger.info("kaggle: pushing kernel files=%s", files)
            api.kernels_push(str(tmp_path), timeout=timeout)

    def kernels_list(self, user: str, page_size: int = 20) -> list[dict]:
        api = self._get_api()
        # api.kernels_list returns a list of objects, convert to dict for easier usage
        kernels = api.kernels_list(user=user, page_size=page_size)
        return [
            {
                "ref": getattr(k, "ref", ""),
                "title": getattr(k, "title", ""),
                "slug": getattr(k, "slug", ""),
                "lastRunTime": getattr(k, "lastRunTime", None),
            }
            for k in kernels
        ]

    def kernels_pull(
        self, kernel_ref: str, path: Path | str, metadata: bool = True
    ) -> None:
        api = self._get_api()
        api.kernels_pull(kernel_ref, path=str(path), metadata=metadata)

    def deploy_kernel_update(
        self, kernel_ref: str, dataset_sources: str | list[str]
    ) -> str:
        """Deploy kernel with dataset sources updated.
        
        HYBRID approach:
        - If a matching repo-local kernel exists, use repo code/metadata as source of truth
        - Otherwise, pull from Kaggle as a fallback
        """
        import time
        api = self._get_api()

        local_kernel = find_local_kernel(kernel_ref)
        is_local = local_kernel is not None
        
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            
            if is_local:
                local_kernel_path = Path(str(local_kernel.get("path") or ""))
                if not local_kernel_path.exists():
                    raise FileNotFoundError(f"Local kernel path not found: {local_kernel_path}")
                
                logger.info(
                    "kaggle: deploying REPO kernel source=%s requested_ref=%s datasets=%s",
                    local_kernel_path.name,
                    kernel_ref,
                    dataset_sources,
                )
                logger.info(
                    "kaggle: local kernel path resolved=%s",
                    local_kernel_path.resolve(),
                )
                
                # Copy local kernel files to temp directory
                _copy_kernel_tree(local_kernel_path, tmp_path)
                logger.info("kaggle: copied local kernel from %s", local_kernel_path)
            else:
                # Pull from Kaggle (original behavior)
                logger.info(
                    "kaggle: deploying REMOTE kernel ref=%s datasets=%s",
                    kernel_ref,
                    dataset_sources,
                )
                api.kernels_pull(kernel_ref, path=str(tmp_path), metadata=True)
                _prune_kernel_tree(tmp_path)
                logger.info("kaggle: pulled kernel from Kaggle")
            
            meta_path = tmp_path / "kernel-metadata.json"
            if not meta_path.exists():
                raise FileNotFoundError(f"kernel-metadata.json not found")

            meta_data = json.loads(meta_path.read_text(encoding="utf-8"))
            requested_ref = str(kernel_ref or "").strip()
            if requested_ref and not requested_ref.startswith(LOCAL_KERNEL_PREFIX):
                meta_data["id"] = requested_ref
            username = (os.getenv("KAGGLE_USERNAME") or "").strip()
            kernel_id = str(meta_data.get("id") or "").strip()
            if username and kernel_id:
                if "/" in kernel_id:
                    owner, slug = kernel_id.split("/", 1)
                else:
                    owner, slug = "", kernel_id
                if slug and owner != username:
                    new_id = f"{username}/{slug}"
                    logger.info(
                        "kaggle: overriding deployed kernel owner old_id=%s new_id=%s",
                        kernel_id,
                        new_id,
                    )
                    meta_data["id"] = new_id
            
            # Set dataset sources for this session while preserving any static inputs
            requested_sources = (
                [dataset_sources]
                if isinstance(dataset_sources, str)
                else list(dataset_sources)
            )
            existing_sources = meta_data.get("dataset_sources", [])
            for dataset_slug in requested_sources:
                if dataset_slug not in existing_sources:
                    existing_sources.append(dataset_slug)
            meta_data["dataset_sources"] = existing_sources
            # Ensure internet is enabled for pip installs
            meta_data["enable_internet"] = True
            local_kernel_name = (
                local_kernel_path.name
                if is_local
                else str(meta_data.get("slug") or meta_data.get("id") or "")
            )
            if is_local and _should_force_gpu_for_local_kernel(local_kernel_name, meta_data):
                meta_data["enable_gpu"] = True

            logger.info(
                "kaggle: kernel metadata updated id=%s dataset_sources=%s enable_gpu=%s",
                meta_data.get("id"),
                meta_data.get("dataset_sources"),
                meta_data.get("enable_gpu"),
            )

            meta_path.write_text(json.dumps(meta_data, ensure_ascii=False, indent=2))

            files = sorted(
                (f.relative_to(tmp_path).as_posix(), f.stat().st_size)
                for f in tmp_path.rglob("*")
                if f.is_file()
            )
            logger.info("kaggle: pushing kernel files=%s", files)
            api.kernels_push(str(tmp_path))
            result_ref = str(meta_data.get("id") or meta_data.get("slug") or kernel_ref)
            logger.info("kaggle: kernel deployed successfully ref=%s", result_ref)
            
            # Wait for Kaggle to propagate metadata changes before kernel starts
            logger.info("kaggle: waiting 10s for metadata to propagate...")
            time.sleep(10)
            
            return result_ref


    def get_kernel_status(self, kernel_ref: str) -> dict:
        api = self._get_api()
        logger.debug("kaggle: getting kernel status for %s", kernel_ref)
        response = api.kernels_status(kernel_ref)
        
        # Convert API response object to dict for .get() access
        # Priority: to_dict() > parse string repr > getattr status
        if hasattr(response, 'to_dict'):
            result = response.to_dict()
        elif hasattr(response, '__str__'):
            # Response might be like {"status": "COMPLETE", "failureMessage": null}
            try:
                result = json.loads(str(response))
            except (json.JSONDecodeError, TypeError):
                result = {}
        else:
            result = {}
        
        # Fallback: get status directly from response object
        if not result.get("status"):
            status_val = getattr(response, 'status', None)
            if status_val is not None:
                # Handle enum values like KernelWorkerStatus.COMPLETE
                result["status"] = status_val.name if hasattr(status_val, 'name') else str(status_val)
        
        # Also try to get failure message
        if not result.get("failureMessage"):
            fail_msg = getattr(response, 'failure_message', None) or getattr(response, 'failureMessage', None)
            if fail_msg:
                result["failureMessage"] = fail_msg
        
        logger.info(
            "kaggle: kernel status kernel=%s status=%s failure=%s",
            kernel_ref,
            result.get("status"),
            result.get("failureMessage") or result.get("failure_message"),
        )
        return result

    def kernel_has_dataset_sources(
        self,
        kernel_ref: str,
        expected_sources: list[str],
    ) -> tuple[bool, dict[str, Any]]:
        expected_clean = [str(item).strip() for item in expected_sources if str(item).strip()]
        if not expected_clean:
            return True, {"dataset_sources": []}

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            self.kernels_pull(kernel_ref, tmp_path, metadata=True)
            meta_path = tmp_path / "kernel-metadata.json"
            if not meta_path.exists():
                raise FileNotFoundError(
                    f"kernel-metadata.json not found after pulling {kernel_ref}"
                )
            meta = json.loads(meta_path.read_text(encoding="utf-8"))

        actual_sources = [
            str(item).strip()
            for item in (meta.get("dataset_sources") or [])
            if str(item).strip()
        ]
        matched = all(item in actual_sources for item in expected_clean)
        meta["dataset_sources"] = actual_sources
        logger.info(
            "kaggle: kernel dataset sources kernel=%s matched=%s expected=%s actual=%s",
            kernel_ref,
            matched,
            expected_clean,
            actual_sources,
        )
        return matched, meta

    def download_kernel_output(
        self, kernel_ref: str, *, path: str | Path, force: bool = True, quiet: bool = False
    ) -> list[str]:
        api = self._get_api()
        logger.info("kaggle: downloading kernel output kernel=%s path=%s", kernel_ref, path)
        files, _ = api.kernels_output(
            kernel_ref, path=str(path), force=force, quiet=quiet
        )
        logger.info("kaggle: downloaded %s files: %s", len(files), files)
        return files

    def kaggle_test(self) -> str:
        api = self._get_api()
        datasets = api.dataset_list(page=1) or []
        titles = [d.title for d in datasets if getattr(d, "title", None)]
        if titles:
            return titles[0]
        return f"ok (datasets={len(datasets)})"
