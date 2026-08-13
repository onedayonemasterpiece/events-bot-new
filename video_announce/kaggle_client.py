from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import shutil
import tempfile
import time
import textwrap
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
KERNEL_PUSH_INVALID_DATASET_RETRY_SECONDS = 180
KERNEL_PUSH_INVALID_DATASET_RETRY_POLL_SECONDS = 10


def _normalize_kernel_ref(ref: str | None) -> str:
    value = str(ref or "").strip()
    if value.startswith("/code/"):
        return value[len("/code/") :]
    if value.startswith("/") and value.count("/") >= 2:
        return value[1:]
    return value


def _extract_save_kernel_response(response: Any) -> dict[str, Any]:
    def _attr(*names: str) -> Any:
        for name in names:
            if hasattr(response, name):
                return getattr(response, name)
        return None

    invalid_dataset_sources = _attr("invalidDatasetSources", "invalid_dataset_sources") or []
    return {
        "ref": _normalize_kernel_ref(_attr("ref")),
        "url": str(_attr("url") or "").strip(),
        "version_number": _attr("versionNumber", "version_number"),
        "error": str(_attr("error") or "").strip(),
        "invalid_dataset_sources": [
            str(item).strip()
            for item in invalid_dataset_sources
            if str(item).strip()
        ],
    }


def _dataset_slug_tail(dataset_slug: str) -> str:
    return str(dataset_slug or "").strip().split("/", 1)[-1].casefold()


def _is_ephemeral_session_dataset(dataset_slug: str) -> bool:
    tail = _dataset_slug_tail(dataset_slug)
    return tail.startswith((
        "cherryflash-session-",
        "kenigsberg-session-",
        "video-afisha-session-",
        "crumple-story-publish-session-",
    ))


def _is_session_kernel_id(kernel_id: str | None) -> bool:
    lowered = str(kernel_id or "").strip().casefold()
    slug = lowered.split("/", 1)[-1]
    return (
        slug in {
            "cherryflash",
            "koenigsberg-stories",
            "crumple-video",
            "crumple-story-publish-only",
        }
        or slug.startswith("cherryflash-")
        or slug.startswith("crumple-video-")
    )


def _is_gpu_quota_error(message: str) -> bool:
    lowered = str(message or "").casefold()
    return "gpu quota" in lowered or "weekly gpu quota" in lowered


def _is_crumple_video_kernel_id(kernel_id: str | None) -> bool:
    lowered = str(kernel_id or "").strip().casefold()
    slug = lowered.split("/", 1)[-1]
    return slug == "crumple-video" or slug.startswith("crumple-video-")


def _allows_gpu_quota_cpu_fallback(kernel_id: str | None) -> bool:
    """Return whether a session kernel may be pushed without GPU on quota errors.

    CherryFlash and Koenigsberg can still finish acceptably without an
    accelerator, but CrumpleVideo's Blender/Cycles render becomes hours-long
    and blocks the production lane. For CrumpleVideo a GPU quota error must be
    a loud launch failure instead of an implicit CPU run.
    """

    return _is_session_kernel_id(kernel_id) and not _is_crumple_video_kernel_id(kernel_id)


def _duration_seconds(value: Any) -> float | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("s"):
        raw = raw[:-1]
    try:
        return float(raw)
    except ValueError:
        return None


def _quota_remaining_seconds_from_raw(data: dict[str, Any]) -> float | None:
    quota = data.get("gpuQuota") or data.get("gpu_quota") or {}
    if not isinstance(quota, dict):
        return None
    used = _duration_seconds(quota.get("timeUsed") or quota.get("time_used"))
    reserved = _duration_seconds(quota.get("timeReserved") or quota.get("time_reserved")) or 0.0
    total = _duration_seconds(
        quota.get("totalTimeAllowed")
        or quota.get("total_time_allowed")
        or quota.get("minimumTimeAllowed")
        or quota.get("minimum_time_allowed")
    )
    if used is None or total is None:
        return None
    return float(total) - float(used) - float(reserved)


def _read_gpu_quota_remaining_seconds(api: Any) -> float | None:
    """Read Kaggle GPU quota with a raw fallback for SDK duration parser bugs.

    Kaggle CLI 2.2.1 can fail parsing quota durations like ``"108000s"``
    because it expects a fractional part.  The raw endpoint still returns
    valid JSON, so use the same authenticated HTTP client and parse durations
    locally.
    """

    try:
        response = api.quota_view()
        quota = getattr(response, "gpu_quota", None)
        if quota is not None:
            used = getattr(quota, "time_used", None)
            reserved = getattr(quota, "time_reserved", None)
            total = getattr(quota, "total_time_allowed", None)
            used_seconds = float(used.total_seconds()) if used is not None else None
            reserved_seconds = float(reserved.total_seconds()) if reserved is not None else 0.0
            total_seconds = float(total.total_seconds()) if total is not None else None
            if used_seconds is not None and total_seconds is not None:
                return total_seconds - used_seconds - reserved_seconds
    except Exception as exc:
        logger.info("kaggle: quota_view high-level parser failed; trying raw quota endpoint: %s", exc)

    try:
        from kagglesdk.kernels.types.kernels_api_service import (  # type: ignore
            ApiGetAcceleratorQuotaStatisticsRequest,
        )

        with api.build_kaggle_client() as kaggle:
            http = getattr(kaggle, "_http_client", None)
            if http is None:
                return None
            http._init_session()
            request = http._prepare_request(
                "kernels.KernelsApiService",
                "GetAcceleratorQuotaStatistics",
                ApiGetAcceleratorQuotaStatisticsRequest(),
            )
            settings = http._session.merge_environment_settings(
                request.url,
                {},
                None,
                None,
                None,
            )
            response = http._session.send(request, **settings)
            response.raise_for_status()
            data = response.json()
            if isinstance(data, dict):
                return _quota_remaining_seconds_from_raw(data)
    except Exception as exc:
        logger.warning("kaggle: failed to read raw GPU quota: %s", exc)
    return None


def _kaggle_kernel_exists(api: Any, kernel_ref: str | None) -> bool | None:
    ref = str(kernel_ref or "").strip()
    if not ref or ref.startswith(LOCAL_KERNEL_PREFIX):
        return None
    try:
        api.kernels_status(ref)
        return True
    except Exception as exc:
        response = getattr(exc, "response", None)
        status_code = getattr(response, "status_code", None)
        message = str(exc).casefold()
        if (
            status_code == 404
            or "404" in message
            or "not found" in message
            or "cannot access kernel" in message
            or "wrong kernel slug" in message
        ):
            return False
        logger.warning("kaggle: failed to check kernel existence ref=%s: %s", ref, exc)
        return None


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


def _copy_status_client_to_kernel(dst_root: Path) -> None:
    client_src = KERNELS_ROOT_PATH / "kaggle_status_client.py"
    if not client_src.exists():
        return
    client_dst = dst_root / "kaggle_status_client.py"
    if client_dst.exists():
        return
    shutil.copy2(client_src, client_dst)


_NOTEBOOK_STATUS_TAG = "events_bot_kaggle_status"


_STATUS_CLIENT_LOADER_SOURCE = r'''
def _events_bot_load_status_loader():
    import importlib.util as _events_bot_importlib_util
    from pathlib import Path as _EventsBotPath

    try:
        from kaggle_status_client import load_status_client as _loader
        return _loader
    except Exception as _import_error:
        print(f"[kaggle_status] import failed: {_import_error}", flush=True)

    roots = [
        _EventsBotPath.cwd(),
        _EventsBotPath("/kaggle/working"),
        _EventsBotPath("/kaggle/input"),
    ]
    try:
        roots.append(_EventsBotPath(__file__).resolve().parent)
    except Exception:
        pass
    seen = set()
    for root in roots:
        if not root.exists():
            continue
        candidates = [root / "kaggle_status_client.py"]
        try:
            candidates.extend(sorted(root.rglob("kaggle_status_client.py")))
        except Exception:
            pass
        for candidate in candidates:
            key = str(candidate)
            if key in seen or not candidate.exists():
                continue
            seen.add(key)
            try:
                spec = _events_bot_importlib_util.spec_from_file_location(
                    "events_bot_kaggle_status_client",
                    candidate,
                )
                if spec and spec.loader:
                    module = _events_bot_importlib_util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    print(f"[kaggle_status] loaded helper from {candidate}", flush=True)
                    return module.load_status_client
            except Exception as _path_error:
                print(f"[kaggle_status] helper load failed from {candidate}: {_path_error}", flush=True)
    return None
'''


def _notebook_code_cell(source: str) -> dict[str, Any]:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {_NOTEBOOK_STATUS_TAG: True},
        "outputs": [],
        "source": [line + "\n" for line in source.rstrip("\n").splitlines()],
    }


def _notebook_cell_source(cell: dict[str, Any]) -> str:
    raw = cell.get("source") or []
    if isinstance(raw, str):
        return raw
    return "".join(str(part) for part in raw)


def _notebook_phase_label(source: str) -> str:
    lowered = source.casefold()
    if "publish_story_from_kaggle" in lowered or "story_publish" in lowered:
        return "publish"
    if "render_event(" in lowered or "blender" in lowered or "render" in lowered:
        return "render"
    if "json.dump" in lowered or ".to_json(" in lowered or "output.json" in lowered:
        return "report"
    if "pip install" in lowered or "apt-get" in lowered or "install_libs" in lowered:
        return "preflight"
    return "run"


def _instrument_notebook_kernel(tmp_path: Path, meta_data: dict[str, Any]) -> None:
    code_file = str(meta_data.get("code_file") or "").strip()
    if not code_file.endswith(".ipynb"):
        return
    nb_path = tmp_path / code_file
    if not nb_path.exists():
        logger.warning("kaggle: notebook code_file not found for status instrumentation path=%s", nb_path)
        return
    try:
        notebook = json.loads(nb_path.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("kaggle: failed to parse notebook for status instrumentation path=%s", nb_path, exc_info=True)
        return
    cells = [
        cell
        for cell in (notebook.get("cells") or [])
        if not (isinstance(cell, dict) and (cell.get("metadata") or {}).get(_NOTEBOOK_STATUS_TAG))
    ]
    code_cells = [cell for cell in cells if isinstance(cell, dict) and cell.get("cell_type") == "code"]
    if not code_cells:
        return
    notebook_name = str(meta_data.get("title") or meta_data.get("slug") or code_file)
    loader_source = textwrap.indent(_STATUS_CLIENT_LOADER_SOURCE.strip(), "")
    bootstrap_source = f"""
# Auto-injected by events-bot Kaggle status framework.
import atexit as _kaggle_status_atexit
import os as _kaggle_status_os
import time as _kaggle_status_time

{loader_source}

_load_kaggle_status_client = _events_bot_load_status_loader()
KAGGLE_STATUS_PROGRESS = {{
    "phase": "bootstrap",
    "notebook": {notebook_name!r},
    "cell_index": 0,
    "cell_total": {len(code_cells)},
}}
KAGGLE_STATUS_CLIENT = (
    _load_kaggle_status_client(log=lambda message: print(message, flush=True))
    if _load_kaggle_status_client
    else None
)
KAGGLE_STATUS_STARTED_AT = _kaggle_status_time.monotonic()
KAGGLE_STATUS_TERMINAL_SENT = False
KAGGLE_STATUS_ACQUIRED_RESOURCES = []

def kaggle_status_update(**items):
    KAGGLE_STATUS_PROGRESS.update({{k: v for k, v in items.items() if v is not None}})
    KAGGLE_STATUS_PROGRESS["elapsed_seconds"] = int(_kaggle_status_time.monotonic() - KAGGLE_STATUS_STARTED_AT)
    return dict(KAGGLE_STATUS_PROGRESS)

def kaggle_status_progress():
    return kaggle_status_update(working_dir=_kaggle_status_os.getcwd())

def kaggle_status_event(event, *, phase=None, status=None, progress=None, message=None):
    if KAGGLE_STATUS_CLIENT is None:
        return {{"ok": False, "error": "status client unavailable"}}
    merged = kaggle_status_progress()
    if progress:
        merged.update(progress)
    return KAGGLE_STATUS_CLIENT.event(
        event,
        phase=phase or str(merged.get("phase") or event),
        status=status,
        progress=merged,
        message=message,
    )

def _kaggle_status_on_exit():
    try:
        if KAGGLE_STATUS_CLIENT is not None and KAGGLE_STATUS_CLIENT.enabled and not KAGGLE_STATUS_TERMINAL_SENT:
            kaggle_status_event("kernel_exited", phase=str(KAGGLE_STATUS_PROGRESS.get("phase") or "unknown"), status="unknown")
    finally:
        if KAGGLE_STATUS_CLIENT is not None:
            for _kaggle_status_resource in list(KAGGLE_STATUS_ACQUIRED_RESOURCES):
                try:
                    KAGGLE_STATUS_CLIENT.release_resource(str(_kaggle_status_resource))
                except Exception as _kaggle_status_release_error:
                    print(f"[kaggle_status] resource release failed: {{_kaggle_status_release_error}}", flush=True)
            KAGGLE_STATUS_CLIENT.stop_alive()

_kaggle_status_atexit.register(_kaggle_status_on_exit)
if KAGGLE_STATUS_CLIENT is not None and KAGGLE_STATUS_CLIENT.enabled:
    kaggle_status_event("kernel_started", phase="preflight", status="running")
    for _kaggle_status_resource in KAGGLE_STATUS_CLIENT.config.get("resource_leases") or []:
        if not KAGGLE_STATUS_CLIENT.acquire_resource(str(_kaggle_status_resource), ttl_seconds=3 * 60 * 60):
            raise RuntimeError(f"Required Kaggle resource is busy: {{_kaggle_status_resource}}")
        KAGGLE_STATUS_ACQUIRED_RESOURCES.append(str(_kaggle_status_resource))
    KAGGLE_STATUS_CLIENT.start_alive(interval_seconds=60, progress_provider=kaggle_status_progress)
"""
    new_cells: list[dict[str, Any]] = [_notebook_code_cell(bootstrap_source)]
    cell_index = 0
    render_seen = False
    for cell in cells:
        if isinstance(cell, dict) and cell.get("cell_type") == "code":
            cell_index += 1
            source = _notebook_cell_source(cell)
            phase = _notebook_phase_label(source)
            render_seen = render_seen or phase == "render"
            phase_source = f"""
# Auto-injected Kaggle status phase marker.
try:
    kaggle_status_update(phase={phase!r}, cell_index={cell_index}, cell_total={len(code_cells)})
    kaggle_status_event("cell_started", phase={phase!r}, status="running")
except Exception as _kaggle_status_phase_error:
    print(f"[kaggle_status] phase marker failed: {{_kaggle_status_phase_error}}", flush=True)
"""
            new_cells.append(_notebook_code_cell(phase_source))
        new_cells.append(cell)
    final_source = f"""
# Auto-injected Kaggle status terminal marker.
try:
    kaggle_status_update(phase="report", cell_index={len(code_cells)}, cell_total={len(code_cells)})
    if {render_seen!r}:
        kaggle_status_event("render_done", phase="render", status="done")
    kaggle_status_event("report_written", phase="report", status="done")
    KAGGLE_STATUS_TERMINAL_SENT = True
finally:
    if KAGGLE_STATUS_CLIENT is not None:
        for _kaggle_status_resource in list(KAGGLE_STATUS_ACQUIRED_RESOURCES):
            try:
                KAGGLE_STATUS_CLIENT.release_resource(str(_kaggle_status_resource))
                KAGGLE_STATUS_ACQUIRED_RESOURCES.remove(_kaggle_status_resource)
            except Exception as _kaggle_status_release_error:
                print(f"[kaggle_status] resource release failed: {{_kaggle_status_release_error}}", flush=True)
        KAGGLE_STATUS_CLIENT.stop_alive()
"""
    new_cells.append(_notebook_code_cell(final_source))
    notebook["cells"] = new_cells
    nb_path.write_text(json.dumps(notebook, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    logger.info(
        "kaggle: instrumented notebook status path=%s original_cells=%d instrumented_cells=%d",
        nb_path.name,
        len(cells),
        len(new_cells),
    )


def _instrument_script_kernel(tmp_path: Path, meta_data: dict[str, Any]) -> None:
    if bool(meta_data.get("events_bot_disable_status_instrumentation")):
        logger.info(
            "kaggle: script status instrumentation disabled by metadata id=%s",
            meta_data.get("id") or meta_data.get("slug"),
        )
        return
    code_file = str(meta_data.get("code_file") or "").strip()
    if not code_file.endswith(".py"):
        return
    script_path = tmp_path / code_file
    if not script_path.exists():
        logger.warning("kaggle: script code_file not found for status instrumentation path=%s", script_path)
        return
    try:
        source = script_path.read_text(encoding="utf-8")
    except Exception:
        logger.warning("kaggle: failed to read script for status instrumentation path=%s", script_path, exc_info=True)
        return
    if "kaggle_status_client" in source:
        return
    original_name = f"_events_bot_original_{script_path.name}"
    original_path = script_path.with_name(original_name)
    if original_path.exists():
        return
    script_path.rename(original_path)
    notebook_name = str(meta_data.get("title") or meta_data.get("slug") or code_file)
    wrapper_source = f"""from __future__ import annotations

# Auto-injected by events-bot Kaggle status framework.
{_STATUS_CLIENT_LOADER_SOURCE.strip()}

import os
import runpy
import time
import traceback
from pathlib import Path

load_status_client = _events_bot_load_status_loader()

KAGGLE_STATUS_PROGRESS = {{
    "phase": "bootstrap",
    "notebook": {notebook_name!r},
    "script": {script_path.name!r},
}}
KAGGLE_STATUS_CLIENT = load_status_client(log=lambda message: print(message, flush=True)) if load_status_client else None
KAGGLE_STATUS_STARTED_AT = time.monotonic()
KAGGLE_STATUS_ACQUIRED_RESOURCES = []
ORIGINAL_SCRIPT = Path(__file__).with_name({original_name!r})

def kaggle_status_progress():
    KAGGLE_STATUS_PROGRESS["elapsed_seconds"] = int(time.monotonic() - KAGGLE_STATUS_STARTED_AT)
    KAGGLE_STATUS_PROGRESS["working_dir"] = os.getcwd()
    return dict(KAGGLE_STATUS_PROGRESS)

def kaggle_status_event(event, *, phase=None, status=None, message=None):
    if KAGGLE_STATUS_CLIENT is None or not KAGGLE_STATUS_CLIENT.enabled:
        return {{"ok": False, "error": "callbacks disabled"}}
    return KAGGLE_STATUS_CLIENT.event(
        event,
        phase=phase or str(KAGGLE_STATUS_PROGRESS.get("phase") or event),
        status=status,
        progress=kaggle_status_progress(),
        message=message,
    )

def kaggle_status_release_resources():
    if KAGGLE_STATUS_CLIENT is None:
        return
    for resource_key in list(KAGGLE_STATUS_ACQUIRED_RESOURCES):
        try:
            KAGGLE_STATUS_CLIENT.release_resource(str(resource_key))
            KAGGLE_STATUS_ACQUIRED_RESOURCES.remove(resource_key)
        except Exception as exc:
            print(f"[kaggle_status] resource release failed: {{exc}}", flush=True)

try:
    if KAGGLE_STATUS_CLIENT is not None and KAGGLE_STATUS_CLIENT.enabled:
        kaggle_status_event("kernel_started", phase="preflight", status="running")
        for resource_key in KAGGLE_STATUS_CLIENT.config.get("resource_leases") or []:
            if not KAGGLE_STATUS_CLIENT.acquire_resource(str(resource_key), ttl_seconds=3 * 60 * 60):
                raise RuntimeError(f"Required Kaggle resource is busy: {{resource_key}}")
            KAGGLE_STATUS_ACQUIRED_RESOURCES.append(str(resource_key))
        KAGGLE_STATUS_CLIENT.start_alive(interval_seconds=60, progress_provider=kaggle_status_progress)
    KAGGLE_STATUS_PROGRESS["phase"] = "run"
    runpy.run_path(str(ORIGINAL_SCRIPT), run_name="__main__")
except SystemExit as exc:
    code = exc.code if isinstance(exc.code, int) else (0 if exc.code is None else 1)
    if code == 0:
        KAGGLE_STATUS_PROGRESS["phase"] = "report"
        kaggle_status_event("report_written", phase="report", status="done")
    else:
        KAGGLE_STATUS_PROGRESS["phase"] = "failed"
        kaggle_status_event("report_written", phase="failed", status="failed", message=f"SystemExit: {{exc.code}}")
    raise
except Exception as exc:
    KAGGLE_STATUS_PROGRESS["phase"] = "failed"
    kaggle_status_event(
        "report_written",
        phase="failed",
        status="failed",
        message="".join(traceback.format_exception_only(type(exc), exc)).strip(),
    )
    raise
else:
    KAGGLE_STATUS_PROGRESS["phase"] = "report"
    kaggle_status_event("report_written", phase="report", status="done")
finally:
    kaggle_status_release_resources()
    if KAGGLE_STATUS_CLIENT is not None:
        KAGGLE_STATUS_CLIENT.stop_alive()
"""
    script_path.write_text(wrapper_source, encoding="utf-8")
    logger.info("kaggle: instrumented script status path=%s original=%s", script_path.name, original_name)


def _push_kernel_request_with_retries(
    api: Any,
    tmp_path: Path,
    meta_path: Path,
    meta_data: dict[str, Any],
    *,
    timeout: str | None = None,
    allow_cpu_fallback: bool = False,
) -> dict[str, Any]:
    requested_sources = [
        str(item).strip()
        for item in (meta_data.get("dataset_sources") or [])
        if str(item).strip()
    ]
    retry_deadline = (
        time.monotonic() + KERNEL_PUSH_INVALID_DATASET_RETRY_SECONDS
        if requested_sources
        else time.monotonic()
    )

    while True:
        meta_path.write_text(json.dumps(meta_data, ensure_ascii=False, indent=2))
        response = api.kernels_push(str(tmp_path), timeout=timeout)
        response_info = _extract_save_kernel_response(response)
        logger.info(
            "kaggle: kernels_push response ref=%s version=%s error=%s invalid_dataset_sources=%s",
            response_info.get("ref"),
            response_info.get("version_number"),
            response_info.get("error"),
            response_info.get("invalid_dataset_sources"),
        )

        error = str(response_info.get("error") or "").strip()
        if error:
            if (
                allow_cpu_fallback
                and bool(meta_data.get("enable_gpu"))
                and _is_gpu_quota_error(error)
            ):
                logger.warning(
                    "kaggle: kernels_push hit GPU quota; retrying without GPU for kernel=%s",
                    meta_data.get("id") or meta_data.get("slug"),
                )
                meta_data["enable_gpu"] = False
                continue
            raise RuntimeError(f"Kaggle kernels_push failed: {error}")

        invalid_requested = [
            item
            for item in response_info.get("invalid_dataset_sources") or []
            if item in requested_sources
        ]
        if invalid_requested:
            if time.monotonic() < retry_deadline:
                logger.warning(
                    "kaggle: kernels_push invalid dataset sources=%s; waiting %ss and retrying",
                    invalid_requested,
                    KERNEL_PUSH_INVALID_DATASET_RETRY_POLL_SECONDS,
                )
                time.sleep(KERNEL_PUSH_INVALID_DATASET_RETRY_POLL_SECONDS)
                continue
            raise RuntimeError(
                "Kaggle kernels_push rejected dataset sources: "
                + ", ".join(invalid_requested)
            )

        return response_info


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


def resolve_kaggle_slug(kernel_ref: str | None) -> str | None:
    """Resolve a kernel_ref (local: or owner/slug) to the Kaggle kernel id used at push time.

    `local:CherryFlash` and `zigomaro/cherryflash` both refer to the same
    Kaggle kernel; the lock against concurrent same-kernel pushes must
    normalize them. For ``local:`` refs we read ``kernel-metadata.json`` (the
    same file ``kernels_push`` would send) and return its ``id`` field; for
    other refs we return them unchanged.
    """

    ref = str(kernel_ref or "").strip()
    if not ref:
        return None
    if not ref.startswith(LOCAL_KERNEL_PREFIX):
        return ref
    local = find_local_kernel(ref)
    if local:
        kernel_id = str(local.get("id") or "").strip()
        if kernel_id:
            return kernel_id
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
    ) -> dict[str, Any]:
        base_path = Path(kernel_path) if kernel_path else DEFAULT_KERNEL_PATH
        if not base_path.exists():
            raise FileNotFoundError(f"Kernel path not found: {base_path}")
        logger.info("kaggle: preparing kernel push from %s", base_path.resolve())
        api = self._get_api()
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            _copy_kernel_tree(base_path, tmp_path)
            _copy_status_client_to_kernel(tmp_path)
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
                meta_data["dataset_sources"] = [
                    str(item).strip()
                    for item in dataset_sources
                    if str(item).strip()
                ]
            _instrument_notebook_kernel(tmp_path, meta_data)
            _instrument_script_kernel(tmp_path, meta_data)
            files = sorted(
                (f.relative_to(tmp_path).as_posix(), f.stat().st_size)
                for f in tmp_path.rglob("*")
                if f.is_file()
            )
            logger.info("kaggle: pushing kernel files=%s", files)
            response_info = _push_kernel_request_with_retries(
                api,
                tmp_path,
                meta_path,
                meta_data,
                timeout=timeout,
            )
            if not response_info.get("ref"):
                logger.info(
                    "kaggle: kernels_push completed without explicit ref for kernel=%s",
                    meta_data.get("id") or meta_data.get("slug"),
                )
            return dict(response_info)

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
                "current_version_number": int(
                    getattr(k, "current_version_number", None)
                    or getattr(k, "currentVersionNumber", None)
                    or 0
                ),
            }
            for k in kernels
        ]

    def get_kernel_revision(self, kernel_ref: str) -> int:
        """Return the exact positive current remote version.

        Use Kaggle's exact ``kernels/pull`` metadata endpoint rather than a
        bounded list/search result. Absence from a list is ambiguous and must
        never become a zero baseline that could later clear a launch barrier.
        """

        clean_ref = str(kernel_ref or "").strip()
        if "/" not in clean_ref:
            raise ValueError("Kaggle kernel revision requires owner/slug")
        owner, slug = clean_ref.split("/", 1)
        api = self._get_api()
        from kagglesdk.kernels.types.kernels_api_service import ApiGetKernelRequest

        request = ApiGetKernelRequest()
        request.user_name = owner
        request.kernel_slug = slug
        with api.build_kaggle_client() as kaggle:
            response = kaggle.kernels.kernels_api_client.get_kernel(request)
        metadata = getattr(response, "metadata", None)
        remote_ref = str(getattr(metadata, "ref", "") or "").strip()
        if remote_ref and _normalize_kernel_ref(remote_ref) != clean_ref:
            raise RuntimeError(
                f"Kaggle exact revision identity mismatch: {remote_ref} != {clean_ref}"
            )
        revision = int(
            getattr(metadata, "current_version_number", None)
            or getattr(metadata, "currentVersionNumber", None)
            or 0
        )
        if revision <= 0:
            raise RuntimeError(f"Kaggle kernel revision unavailable: {clean_ref}")
        return revision

    def kernels_pull(
        self, kernel_ref: str, path: Path | str, metadata: bool = True
    ) -> None:
        api = self._get_api()
        api.kernels_pull(kernel_ref, path=str(path), metadata=metadata)

    def deploy_kernel_update(
        self,
        kernel_ref: str,
        dataset_sources: str | list[str],
        *,
        target_kernel_ref: str | None = None,
        machine_shape: str | None = None,
    ) -> str:
        """Deploy kernel with dataset sources updated.

        HYBRID approach:
        - If a matching repo-local kernel exists, use repo code/metadata as source of truth
        - Otherwise, pull from Kaggle as a fallback

        ``target_kernel_ref`` lets one repo-local source (for example
        ``local:CherryFlash``) be pushed into an isolated Kaggle kernel slug for
        a video lane.

        ``machine_shape`` overrides the accelerator (e.g. ``NvidiaTeslaT4``).
        Pass ``None`` to keep the kernel's default. Used by the accel-pref
        fallback (INC-2026-05-26 round 3).
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
                _copy_status_client_to_kernel(tmp_path)
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
                _copy_status_client_to_kernel(tmp_path)
                logger.info("kaggle: pulled kernel from Kaggle")
            
            meta_path = tmp_path / "kernel-metadata.json"
            if not meta_path.exists():
                raise FileNotFoundError(f"kernel-metadata.json not found")

            meta_data = json.loads(meta_path.read_text(encoding="utf-8"))
            requested_ref = str(target_kernel_ref or kernel_ref or "").strip()
            if requested_ref and not requested_ref.startswith(LOCAL_KERNEL_PREFIX):
                meta_data["id"] = requested_ref
                requested_slug = requested_ref.split("/", 1)[-1].strip()
                if requested_slug:
                    meta_data["slug"] = requested_slug
                    title_slug = str(meta_data.get("title") or "").strip().casefold().replace(" ", "-")
                    if title_slug != requested_slug.casefold():
                        meta_data["title"] = requested_slug
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
            
            # Set dataset sources for this session while preserving static inputs.
            # Session kernels must not keep old per-run datasets attached:
            # Kaggle can otherwise execute a stale mounted bundle while the
            # server records a fresh handoff.
            requested_sources = (
                [dataset_sources]
                if isinstance(dataset_sources, str)
                else list(dataset_sources)
            )
            existing_sources = [
                str(item).strip()
                for item in (meta_data.get("dataset_sources") or [])
                if str(item).strip()
            ]
            if _is_session_kernel_id(str(meta_data.get("id") or "")):
                existing_sources = [
                    item
                    for item in existing_sources
                    if not _is_ephemeral_session_dataset(item)
                ]
            for dataset_slug in requested_sources:
                dataset_slug = str(dataset_slug).strip()
                if dataset_slug and dataset_slug not in existing_sources:
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

            if machine_shape:
                meta_data["machine_shape"] = machine_shape
                logger.warning(
                    "kaggle: applying accel-pref override id=%s machine_shape=%s",
                    meta_data.get("id"),
                    machine_shape,
                )

            if (
                is_local
                and bool(meta_data.get("enable_gpu"))
                and _is_session_kernel_id(str(meta_data.get("id") or ""))
                and _allows_gpu_quota_cpu_fallback(str(meta_data.get("id") or ""))
                and _kaggle_kernel_exists(api, str(meta_data.get("id") or "")) is False
            ):
                remaining_seconds = _read_gpu_quota_remaining_seconds(api)
                logger.info(
                    "kaggle: target kernel is new; GPU quota remaining seconds=%s id=%s",
                    remaining_seconds,
                    meta_data.get("id"),
                )
                if remaining_seconds is not None and remaining_seconds <= 0:
                    logger.warning(
                        "kaggle: target kernel does not exist and GPU quota is exhausted; "
                        "creating/updating lane target with CPU first id=%s",
                        meta_data.get("id"),
                    )
                    meta_data["enable_gpu"] = False
                    meta_data.pop("machine_shape", None)

            _instrument_notebook_kernel(tmp_path, meta_data)
            _instrument_script_kernel(tmp_path, meta_data)

            logger.info(
                "kaggle: kernel metadata updated id=%s dataset_sources=%s enable_gpu=%s machine_shape=%s",
                meta_data.get("id"),
                meta_data.get("dataset_sources"),
                meta_data.get("enable_gpu"),
                meta_data.get("machine_shape"),
            )

            meta_path.write_text(json.dumps(meta_data, ensure_ascii=False, indent=2))

            files = sorted(
                (f.relative_to(tmp_path).as_posix(), f.stat().st_size)
                for f in tmp_path.rglob("*")
                if f.is_file()
            )
            logger.info("kaggle: pushing kernel files=%s", files)
            response_info = _push_kernel_request_with_retries(
                api,
                tmp_path,
                meta_path,
                meta_data,
                allow_cpu_fallback=(
                    is_local
                    and _allows_gpu_quota_cpu_fallback(str(meta_data.get("id") or ""))
                ),
            )
            result_ref = str(
                response_info.get("ref")
                or meta_data.get("id")
                or meta_data.get("slug")
                or kernel_ref
            ).strip()
            logger.info(
                "kaggle: kernel deployed successfully ref=%s version=%s",
                result_ref,
                response_info.get("version_number"),
            )
            
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
