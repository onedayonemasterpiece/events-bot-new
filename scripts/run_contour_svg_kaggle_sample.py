#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shutil
import tarfile
import tempfile
import time
import urllib.request
import uuid
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in os.sys.path:
    os.sys.path.insert(0, str(PROJECT_ROOT))

try:
    from cryptography.fernet import Fernet
except ImportError as exc:  # pragma: no cover - environment preflight
    raise RuntimeError("cryptography is required for Kaggle split-secret datasets") from exc

logger = logging.getLogger("contour_svg_kaggle_sample")

DEFAULT_KERNEL_PATH = PROJECT_ROOT / "kaggle" / "ContourSvgGenerator"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "artifacts" / "codex" / "contour-svg-sample"
DEFAULT_CONFIG = "docs/features/countur_svg_generator/examples/sample_building.yaml"
DEFAULT_ENV_FILE = PROJECT_ROOT / ".env"
PAYLOAD_PREFIX = "csv-payload"
SECRET_PREFIX = "csv-secrets"
KEY_PREFIX = "csv-key"
SAM2_SDIST_URL = "https://files.pythonhosted.org/packages/ce/11/d07fc96688f731a85de6d5260e98b709051eded2b7b5667ae292530bcf90/sam2-1.1.0.tar.gz"
SAM2_VENDOR_ROOT = PROJECT_ROOT / "artifacts" / "codex" / "contour-svg-sam2-vendor"


def load_env_file_values(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#") or "=" not in raw_line:
            continue
        key, value = raw_line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        if key:
            values[key] = value
    return values


def apply_env_file(path: Path) -> None:
    for key, value in load_env_file_values(path).items():
        os.environ.setdefault(key, value)


def slugify(value: str, *, max_len: int = 48) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return (slug or uuid.uuid4().hex[:8])[:max_len].rstrip("-")


def compact_unique_slug(value: str, *, max_len: int = 32, suffix_len: int = 13) -> str:
    slug = slugify(value, max_len=256)
    if len(slug) <= max_len:
        return slug
    suffix = slug[-suffix_len:].strip("-")
    prefix = slug[: max(1, max_len - len(suffix) - 1)].rstrip("-")
    return f"{prefix}-{suffix}".strip("-")[:max_len].rstrip("-")


def require_kaggle_username() -> str:
    username = (os.getenv("KAGGLE_USERNAME") or "").strip()
    if not username:
        raise RuntimeError("KAGGLE_USERNAME is required")
    return username


def get_kaggle_api():
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
    except Exception as exc:  # pragma: no cover - environment preflight
        raise RuntimeError("kaggle package and credentials are required") from exc
    api = KaggleApi()
    api.authenticate()
    return api


def create_dataset(api, folder: Path) -> None:
    api.dataset_create_new(
        str(folder),
        public=False,
        quiet=True,
        convert_to_csv=False,
        dir_mode="zip",
    )


def delete_dataset(api, dataset_ref: str) -> None:
    owner, slug = dataset_ref.split("/", 1)
    api.dataset_delete(owner, slug, no_confirm=True)


def create_or_replace_dataset(api, username: str, slug: str, title: str, writer) -> str:
    dataset_ref = f"{username}/{slug}"
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        writer(tmp_path)
        (tmp_path / "dataset-metadata.json").write_text(
            json.dumps(
                {"title": title, "id": dataset_ref, "licenses": [{"name": "CC0-1.0"}]},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        try:
            create_dataset(api, tmp_path)
        except Exception:
            try:
                delete_dataset(api, dataset_ref)
            except Exception:
                logger.info("dataset delete before recreate did not complete dataset=%s", dataset_ref, exc_info=True)
            create_dataset(api, tmp_path)
    return dataset_ref


def encrypt_secret(secret: str) -> tuple[bytes, bytes]:
    static_key = (os.getenv("TG_MONITORING_FERNET_KEY") or "").strip()
    key_path = (os.getenv("TG_MONITORING_FERNET_KEY_PATH") or "").strip()
    if static_key:
        key = static_key.encode("utf-8")
    elif key_path and Path(key_path).exists():
        key = Path(key_path).read_bytes().strip()
    else:
        key = Fernet.generate_key()
    return Fernet(key).encrypt(secret.encode("utf-8")), key


def copy_tree(src: Path, dst: Path) -> None:
    if dst.exists():
        shutil.rmtree(dst)
    ignore = shutil.ignore_patterns("__pycache__", "*.pyc", ".pytest_cache")
    shutil.copytree(src, dst, ignore=ignore)


def project_relative(path: str | Path) -> str:
    raw = Path(path).expanduser()
    if raw.is_absolute():
        try:
            return str(raw.resolve().relative_to(PROJECT_ROOT))
        except ValueError:
            return str(raw)
    return str(raw)


def build_run_config(*, run_id: str, config_path: str | Path, output_dir: str | None = None) -> dict[str, Any]:
    data: dict[str, Any] = {
        "run_id": run_id,
        "config_path": project_relative(config_path),
    }
    if output_dir:
        data["output_dir"] = output_dir
    return data


def ensure_sam2_vendor() -> Path:
    package_dir = SAM2_VENDOR_ROOT / "sam2"
    if (package_dir / "__init__.py").exists():
        return package_dir
    if SAM2_VENDOR_ROOT.exists():
        shutil.rmtree(SAM2_VENDOR_ROOT)
    SAM2_VENDOR_ROOT.mkdir(parents=True, exist_ok=True)
    archive = SAM2_VENDOR_ROOT / "sam2-1.1.0.tar.gz"
    urllib.request.urlretrieve(SAM2_SDIST_URL, archive)
    with tarfile.open(archive, "r:gz") as tf:
        members = [m for m in tf.getmembers() if m.name.startswith("sam2-1.1.0/sam2/")]
        tf.extractall(SAM2_VENDOR_ROOT / "_extract", members=members)
    extracted = SAM2_VENDOR_ROOT / "_extract" / "sam2-1.1.0" / "sam2"
    if not (extracted / "__init__.py").exists():
        raise RuntimeError("SAM2 vendor extraction failed")
    shutil.copytree(extracted, package_dir)
    shutil.rmtree(SAM2_VENDOR_ROOT / "_extract", ignore_errors=True)
    return package_dir


def write_payload_dataset(
    path: Path,
    *,
    run_id: str,
    config_path: str | Path = DEFAULT_CONFIG,
    output_dir: str | None = None,
) -> None:
    repo = path / "repo_bundle"
    copy_tree(PROJECT_ROOT / "contour_svg", repo / "contour_svg")
    copy_tree(PROJECT_ROOT / "google_ai", repo / "google_ai")
    copy_tree(ensure_sam2_vendor(), repo / "sam2")
    copy_tree(
        PROJECT_ROOT / "docs" / "features" / "countur_svg_generator",
        repo / "docs" / "features" / "countur_svg_generator",
    )
    (repo / "kaggle").mkdir(parents=True, exist_ok=True)
    shutil.copy2(PROJECT_ROOT / "kaggle" / "kaggle_status_client.py", repo / "kaggle" / "kaggle_status_client.py")
    (path / "kaggle_run.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "kind": "contour_svg",
                "notebook": "ContourSvgGenerator",
                "resource_leases": [],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (path / "contour_run_config.json").write_text(
        json.dumps(
            build_run_config(run_id=run_id, config_path=config_path, output_dir=output_dir),
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def build_secret_payload() -> str:
    names = [
        "GOOGLE_API_KEY",
        "GOOGLE_API_KEY2",
        "GOOGLE_API_KEY3",
        "GOOGLE_API_KEY4",
        "GOOGLE_API_LOCALNAME",
        "GOOGLE_API_LOCALNAME_CONTOUR",
        "SUPABASE_URL",
        "SUPABASE_KEY",
        "SUPABASE_SERVICE_KEY",
        "SUPABASE_SCHEMA",
        "GOOGLE_AI_LIMITER_SUPABASE_URL",
        "GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY",
        "GOOGLE_AI_RESERVE_SCOPE_TO_DEFAULT_ENV",
        "GOOGLE_AI_RESERVE_OVERFLOW_KEY_ENVS",
    ]
    payload = {name: os.getenv(name) for name in names if (os.getenv(name) or "").strip()}
    if not payload.get("GOOGLE_AI_RESERVE_OVERFLOW_KEY_ENVS"):
        overflow_envs = [name for name in ("GOOGLE_API_KEY2", "GOOGLE_API_KEY3", "GOOGLE_API_KEY4") if payload.get(name)]
        if overflow_envs:
            payload["GOOGLE_AI_RESERVE_OVERFLOW_KEY_ENVS"] = ",".join(overflow_envs)
    missing = [
        name
        for name in [
            "GOOGLE_API_KEY",
            "GOOGLE_AI_LIMITER_SUPABASE_URL",
            "GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY",
        ]
        if not payload.get(name)
    ]
    if missing:
        raise RuntimeError(f"Missing required secret envs: {', '.join(missing)}")
    return json.dumps(payload, ensure_ascii=False)


def write_secret_datasets(api, username: str, run_slug: str, secret_payload: str) -> tuple[str, str]:
    encrypted, fernet_key = encrypt_secret(secret_payload)

    def write_secret(path: Path) -> None:
        (path / "secrets.enc").write_bytes(encrypted)

    def write_key(path: Path) -> None:
        (path / "fernet.key").write_bytes(fernet_key)

    secret_ref = create_or_replace_dataset(
        api,
        username,
        f"{SECRET_PREFIX}-{run_slug}",
        f"CSV secrets {run_slug[:24]}",
        write_secret,
    )
    key_ref = create_or_replace_dataset(
        api,
        username,
        f"{KEY_PREFIX}-{run_slug}",
        f"CSV key {run_slug[:24]}",
        write_key,
    )
    return secret_ref, key_ref


def copy_status_client_to_kernel(dst_root: Path) -> None:
    shutil.copy2(PROJECT_ROOT / "kaggle" / "kaggle_status_client.py", dst_root / "kaggle_status_client.py")


def push_kernel(
    api,
    *,
    kernel_slug: str,
    dataset_sources: list[str],
    accelerator: str,
    session_timeout_seconds: int,
) -> str:
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir) / "ContourSvgGenerator"
        shutil.copytree(DEFAULT_KERNEL_PATH, tmp_path)
        copy_status_client_to_kernel(tmp_path)
        meta_path = tmp_path / "kernel-metadata.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        username = require_kaggle_username()
        meta["id"] = f"{username}/{kernel_slug}"
        meta["slug"] = kernel_slug
        meta["title"] = "Contour SVG Generator"
        meta["dataset_sources"] = dataset_sources
        meta["enable_gpu"] = True
        meta["enable_internet"] = True
        meta["machine_shape"] = accelerator
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        api.kernels_push(str(tmp_path), timeout=str(session_timeout_seconds), acc=accelerator)
    return f"{require_kaggle_username()}/{kernel_slug}"


def dataset_files(api, dataset_ref: str) -> list[str]:
    response = api.dataset_list_files(dataset_ref, page_size=100)
    files = getattr(response, "files", response)
    return [str(getattr(item, "name", item)) for item in (files or [])]


def wait_dataset_ready(api, dataset_ref: str, *, expected_files: list[str], timeout_seconds: int = 180) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_files: list[str] = []
    last_status = ""
    while time.monotonic() < deadline:
        try:
            last_status = str(api.dataset_status(dataset_ref))
            last_files = dataset_files(api, dataset_ref)
            ready = last_status.lower() == "ready" and all(name in last_files for name in expected_files)
            logger.info("dataset ready check dataset=%s status=%s files=%s ready=%s", dataset_ref, last_status, last_files, ready)
            if ready:
                return
        except Exception:
            logger.info("dataset ready check failed dataset=%s", dataset_ref, exc_info=True)
        time.sleep(5)
    raise RuntimeError(f"Dataset not ready: {dataset_ref} status={last_status} files={last_files}")


def kernel_status(api, kernel_ref: str) -> dict[str, Any]:
    response = api.kernels_status(kernel_ref)
    if hasattr(response, "to_dict"):
        result = response.to_dict()
    else:
        try:
            result = json.loads(str(response))
        except Exception:
            result = {}
    if not result.get("status"):
        status = getattr(response, "status", None)
        result["status"] = status.name if hasattr(status, "name") else str(status or "")
    failure = getattr(response, "failure_message", None) or getattr(response, "failureMessage", None)
    if failure and not result.get("failureMessage"):
        result["failureMessage"] = failure
    return result


def kernel_logs(api, kernel_ref: str) -> str:
    try:
        return str(api.kernels_logs(kernel_ref) or "")
    except Exception as exc:
        logger.info("kernel logs unavailable ref=%s err=%s", kernel_ref, exc)
        return ""


def emit_new_status_log_lines(api, kernel_ref: str, *, seen: set[str]) -> None:
    logs = kernel_logs(api, kernel_ref)
    for raw in logs.splitlines():
        line = raw.strip()
        if not line or line in seen:
            continue
        if "[kaggle_status]" in line or "Traceback" in line or "RuntimeError" in line:
            logger.info("kernel log: %s", line[:1000])
            seen.add(line)


def poll_kernel(api, kernel_ref: str, *, timeout_minutes: int, poll_interval_seconds: int) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_minutes * 60
    last: dict[str, Any] = {}
    seen_logs: set[str] = set()
    while time.monotonic() < deadline:
        last = kernel_status(api, kernel_ref)
        state = str(last.get("status") or "").upper()
        failure = last.get("failureMessage") or last.get("failure_message")
        logger.info("kernel status ref=%s state=%s failure=%s", kernel_ref, state or "UNKNOWN", failure)
        emit_new_status_log_lines(api, kernel_ref, seen=seen_logs)
        if state in {"COMPLETE", "ERROR", "FAILED", "CANCELLED", "CANCEL_ACKNOWLEDGED"}:
            return last
        time.sleep(max(10, poll_interval_seconds))
    return {**last, "status": "TIMEOUT"}


def download_kernel_output(api, kernel_ref: str, output_dir: Path) -> list[str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    files, _ = api.kernels_output(kernel_ref, path=str(output_dir), force=True, quiet=False)
    return [str(item) for item in files]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout-minutes", type=int, default=90)
    parser.add_argument("--poll-interval-seconds", type=int, default=30)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--accelerator", default=os.getenv("CONTOUR_KAGGLE_ACCELERATOR", "NvidiaTeslaT4"))
    parser.add_argument("--kernel-slug", default=os.getenv("CONTOUR_KAGGLE_KERNEL_SLUG", "contour-svg-generator"))
    parser.add_argument("--config", default=os.getenv("CONTOUR_KAGGLE_CONFIG", DEFAULT_CONFIG))
    parser.add_argument("--kaggle-output-dir", default=os.getenv("CONTOUR_KAGGLE_OUTPUT_DIR"))
    parser.add_argument("--run-label", default=os.getenv("CONTOUR_KAGGLE_RUN_LABEL", "contour-svg-sample"))
    parser.add_argument("--session-timeout-seconds", type=int, default=7200)
    parser.add_argument("--keep-datasets", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    apply_env_file(DEFAULT_ENV_FILE)

    run_id = f"{slugify(args.run_label, max_len=32)}-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
    run_slug = compact_unique_slug(run_id, max_len=32)
    username = require_kaggle_username()
    api = get_kaggle_api()

    payload_ref = create_or_replace_dataset(
        api,
        username,
        f"{PAYLOAD_PREFIX}-{run_slug}",
        f"CSV payload {run_slug[:24]}",
        lambda path: write_payload_dataset(
            path,
            run_id=run_id,
            config_path=args.config,
            output_dir=args.kaggle_output_dir,
        ),
    )
    secret_ref, key_ref = write_secret_datasets(api, username, run_slug, build_secret_payload())
    dataset_sources = [payload_ref, secret_ref, key_ref]

    for dataset_ref, expected in [
        (payload_ref, ["kaggle_run.json", "contour_run_config.json"]),
        (secret_ref, ["secrets.enc"]),
        (key_ref, ["fernet.key"]),
    ]:
        wait_dataset_ready(api, dataset_ref, timeout_seconds=180, expected_files=expected)

    kernel_ref = push_kernel(
        api,
        kernel_slug=slugify(args.kernel_slug, max_len=48),
        dataset_sources=dataset_sources,
        accelerator=args.accelerator,
        session_timeout_seconds=args.session_timeout_seconds,
    )
    status = poll_kernel(
        api,
        kernel_ref,
        timeout_minutes=args.timeout_minutes,
        poll_interval_seconds=args.poll_interval_seconds,
    )

    out_dir = args.output_root / run_slug
    out_dir.mkdir(parents=True, exist_ok=True)
    downloaded = download_kernel_output(api, kernel_ref, out_dir)
    summary = {
        "run_id": run_id,
        "kernel_ref": kernel_ref,
        "kernel_slug": slugify(args.kernel_slug, max_len=48),
        "config": project_relative(args.config),
        "kaggle_output_dir": args.kaggle_output_dir,
        "kernel_status": status,
        "accelerator": args.accelerator,
        "session_timeout_seconds": args.session_timeout_seconds,
        "dataset_sources": dataset_sources,
        "download_dir": str(out_dir),
        "downloaded": downloaded,
    }
    (out_dir / "local_run_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    if not args.keep_datasets:
        for dataset_ref in dataset_sources:
            try:
                delete_dataset(api, dataset_ref)
            except Exception:
                logger.info("dataset cleanup failed dataset=%s", dataset_ref, exc_info=True)


if __name__ == "__main__":
    main()
