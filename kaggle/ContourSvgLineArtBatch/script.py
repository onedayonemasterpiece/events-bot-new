from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import traceback
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
KAGGLE_INPUT = Path("/kaggle/input")
DEFAULT_OUT = Path("/kaggle/working/contour_svg_line_art_batch")
STATUS_PROGRESS = {"phase": "bootstrap", "progress_percent": 0, "progress_label": "bootstrap"}
REQUIRED_IMPORTS = [
    ("PIL", "pillow"),
    ("numpy", "numpy"),
    ("cv2", "opencv-python-headless"),
    ("torch", "torch"),
    ("diffusers", "diffusers"),
    ("transformers", "transformers"),
    ("accelerate", "accelerate"),
    ("safetensors", "safetensors"),
]


def _find_repo_root() -> Path:
    local_root = Path(__file__).resolve().parents[2]
    if (local_root / "contour_svg").exists():
        return local_root
    for root in (KAGGLE_INPUT, Path("/kaggle/working")):
        if not root.exists():
            continue
        for candidate in root.rglob("contour_svg"):
            if candidate.is_dir() and (candidate / "line_art_experiments.py").exists():
                return candidate.parent
    return local_root


ROOT = _find_repo_root()
_CONFIG_CACHE: dict | None = None


def _load_status_client():
    for path in [SCRIPT_DIR, ROOT / "kaggle"]:
        if str(path) not in sys.path:
            sys.path.insert(0, str(path))
    from kaggle_status_client import load_status_client

    return load_status_client(output_dir=DEFAULT_OUT, log=lambda message: print(message, flush=True))


def _status_progress() -> dict:
    return dict(STATUS_PROGRESS)


def _status_event(client, event: str, *, phase: str, status: str = "running", progress: dict | None = None, message: str | None = None) -> None:
    payload = dict(progress or {})
    payload.setdefault("phase", phase)
    STATUS_PROGRESS.update(payload)
    STATUS_PROGRESS["phase"] = phase
    client.event(event, phase=phase, status=status, progress=payload, message=message)


def _install_requirements(status_client) -> None:
    if os.getenv("CONTOUR_LINE_ART_SKIP_PIP_INSTALL") == "1":
        _status_event(status_client, "install_done", phase="install", status="done", progress={"progress_percent": 5, "progress_label": "dependency install skipped"})
        return
    _status_event(status_client, "install_started", phase="install", progress={"progress_percent": 2, "progress_label": "install line-art dependencies"})
    missing = [package for module, package in REQUIRED_IMPORTS if importlib.util.find_spec(module) is None]
    if missing:
        subprocess.check_call([sys.executable, "-m", "pip", "install", *missing])
    _status_event(status_client, "install_done", phase="install", status="done", progress={"progress_percent": 5, "progress_label": "dependency install done", "missing_packages": missing})


def _torch_cuda_probe() -> dict:
    import torch

    info = {
        "torch": getattr(torch, "__version__", None),
        "cuda": getattr(torch.version, "cuda", None),
        "cuda_available": torch.cuda.is_available(),
        "device_name": torch.cuda.get_device_name(0) if torch.cuda.is_available() else None,
    }
    if not torch.cuda.is_available():
        raise RuntimeError(f"CUDA is not available: {info}")
    return info


def _find_first_input(filename: str) -> Path | None:
    for root in [KAGGLE_INPUT, Path("/kaggle/working"), ROOT]:
        if not root.exists():
            continue
        direct = root / filename
        if direct.exists():
            return direct
        matches = sorted(path for path in root.rglob(filename) if path.is_file())
        if matches:
            return matches[0]
    return None


def _load_config() -> dict:
    global _CONFIG_CACHE
    if _CONFIG_CACHE is not None:
        return _CONFIG_CACHE
    path = _find_first_input("line_art_batch_config.json")
    if path is None:
        raise RuntimeError("Cannot locate line_art_batch_config.json")
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise RuntimeError(f"Invalid line_art_batch_config.json at {path}")
    _CONFIG_CACHE = data
    return data


def _resolve_repo_path(raw: str) -> Path:
    path = Path(raw)
    if not path.is_absolute():
        path = ROOT / path
    return path


def _source_images() -> tuple[Path, ...]:
    config = _load_config()
    images = config.get("source_images") or []
    out = []
    for raw in images:
        path = _resolve_repo_path(str(raw))
        if not path.exists():
            raise RuntimeError(f"source image does not exist: {path}")
        out.append(path)
    if not out:
        todo = ROOT / "docs/features/countur_svg_generator/to_do"
        out = sorted([*todo.glob("*.jpg"), *todo.glob("*.jpeg"), *todo.glob("*.png"), *todo.glob("*.webp")])
    if not out:
        raise RuntimeError("No source images configured for line-art batch")
    return tuple(out)


def _tuple_str(key: str, default: str) -> tuple[str, ...]:
    raw = os.getenv(f"CONTOUR_LINE_ART_{key.upper()}", "").strip() or str(_load_config().get(key) or default)
    return tuple(item.strip() for item in raw.split(",") if item.strip())


def _tuple_int(key: str, default: str) -> tuple[int, ...]:
    return tuple(int(item) for item in _tuple_str(key, default))


def _style_reference() -> Path | None:
    raw = str(_load_config().get("style_reference") or "").strip()
    if not raw:
        return None
    path = _resolve_repo_path(raw)
    if not path.exists():
        raise RuntimeError(f"style_reference does not exist: {path}")
    return path


def _output_size() -> tuple[int, int]:
    raw = str(_load_config().get("output_size") or os.getenv("CONTOUR_LINE_ART_OUTPUT_SIZE") or "768,768")
    parts = [int(item.strip()) for item in raw.split(",") if item.strip()]
    if len(parts) != 2:
        raise RuntimeError(f"Invalid output_size: {raw}")
    return (parts[0], parts[1])


def main() -> None:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    status_client = _load_status_client()
    status_client.start_alive(interval_seconds=60, progress_provider=_status_progress)
    _status_event(status_client, "kernel_started", phase="bootstrap", progress={"progress_percent": 0, "progress_label": "kernel started"})
    try:
        _install_requirements(status_client)
        _status_event(status_client, "preflight_started", phase="preflight", progress={"progress_percent": 8, "progress_label": "line-art batch preflight"})
        cuda = _torch_cuda_probe()
        sources = _source_images()
        branches = _tuple_str("branches", "E1_lineart_control_only,E2_lineart_line_init,E3_scribble_control_only,E4_scribble_line_init")
        guide_ids = _tuple_str("guide_ids", "G3_edge_thickened,G4_edge_cleaned,CG1_silhouette_plus_structure,CG3_fused_balanced,CG4_minimal_clean")
        seeds = _tuple_int("seeds", "42")
        _status_event(status_client, "preflight_ok", phase="preflight", status="done", progress={"progress_percent": 12, "progress_label": "preflight ok", "source_count": len(sources), "branches": list(branches), "guide_ids": list(guide_ids), "cuda": cuda})

        from contour_svg.line_art_experiments import LineArtExperimentConfig, run_line_art_experiment_batch

        total_candidates = len(sources) * len(branches) * len(guide_ids) * len(seeds)

        def progress(phase: str, payload: dict) -> None:
            source_index = int(payload.get("source_index") or 0)
            done = int(payload.get("done") or 0)
            total = int(payload.get("total") or total_candidates or 1)
            base = 15 + int(80 * min(1.0, max(0.0, ((source_index - 1) / max(1, len(sources))) if source_index else 0.0)))
            if phase == "candidate":
                image_base = (source_index - 1) * max(1, total)
                progress_percent = 15 + int(80 * min(1.0, (image_base + done) / max(1, total_candidates)))
                label = f"{Path(str(payload.get('source_image'))).name}: {payload.get('guide_id')} · {payload.get('branch')} ({done + 1}/{total})"
            elif phase == "image_done":
                progress_percent = 15 + int(80 * min(1.0, source_index / max(1, len(sources))))
                label = f"image done {source_index}/{len(sources)}"
            else:
                progress_percent = min(94, max(15, base))
                label = str(payload.get("progress_label") or phase)
            _status_event(status_client, "alive", phase=phase, progress={**payload, "progress_percent": progress_percent, "progress_label": label})

        report = run_line_art_experiment_batch(
            LineArtExperimentConfig(
                source_images=sources,
                out_dir=DEFAULT_OUT,
                style_reference=_style_reference(),
                output_size=_output_size(),
                guide_ids=guide_ids,
                branches=branches,
                seeds=seeds,
                steps=int(_load_config().get("steps") or os.getenv("CONTOUR_LINE_ART_STEPS") or 22),
                guidance_scale=float(_load_config().get("guidance_scale") or os.getenv("CONTOUR_LINE_ART_GUIDANCE_SCALE") or 8.0),
                control_scale=float(_load_config().get("control_scale") or os.getenv("CONTOUR_LINE_ART_CONTROL_SCALE") or 0.85),
                strength=float(_load_config().get("strength") or os.getenv("CONTOUR_LINE_ART_STRENGTH") or 0.60),
                max_candidates_per_image=(int(_load_config()["max_candidates_per_image"]) if _load_config().get("max_candidates_per_image") else None),
            ),
            progress=progress,
        )
        _status_event(status_client, "report_written", phase="report", status="done", progress={"progress_percent": 100, "progress_label": "line-art batch report written", "output_dir": str(DEFAULT_OUT), "source_count": len(sources)})
        print(json.dumps({"status": "ok", "output_dir": str(DEFAULT_OUT), "source_count": len(sources), "sources": report.get("sources")}, ensure_ascii=False, indent=2))
    except Exception as exc:
        _status_event(status_client, "report_written", phase="failed", status="failed", progress={**_status_progress(), "progress_label": "line-art batch failed"}, message=f"{exc.__class__.__name__}: {exc}\n{traceback.format_exc(limit=4)}")
        raise
    finally:
        status_client.stop_alive()


if __name__ == "__main__":
    main()
