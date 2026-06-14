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
DEFAULT_OUT = Path("/kaggle/working/contour_svg_neural_branch")
STATUS_PROGRESS = {
    "phase": "bootstrap",
    "progress_percent": 0,
    "progress_label": "bootstrap",
}
REQUIRED_IMPORTS = [
    ("PIL", "pillow"),
    ("numpy", "numpy"),
    ("torch", "torch"),
    ("diffusers", "diffusers"),
    ("transformers", "transformers"),
    ("accelerate", "accelerate"),
]


def _find_repo_root() -> Path:
    local_root = Path(__file__).resolve().parents[2]
    if (local_root / "contour_svg").exists():
        return local_root
    for root in (KAGGLE_INPUT, Path("/kaggle/working")):
        if not root.exists():
            continue
        for candidate in root.rglob("contour_svg"):
            if candidate.is_dir() and (candidate / "neural_branch.py").exists():
                return candidate.parent
    return local_root


ROOT = _find_repo_root()


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
    if os.getenv("CONTOUR_NEURAL_SKIP_PIP_INSTALL") == "1":
        _status_event(
            status_client,
            "install_done",
            phase="install",
            status="done",
            progress={"progress_percent": 5, "progress_label": "dependency install skipped"},
        )
        return
    _status_event(
        status_client,
        "install_started",
        phase="install",
        progress={"progress_percent": 2, "progress_label": "install neural branch dependencies"},
    )
    missing = [package for module, package in REQUIRED_IMPORTS if importlib.util.find_spec(module) is None]
    if missing:
        subprocess.check_call([sys.executable, "-m", "pip", "install", *missing])
    _status_event(
        status_client,
        "install_done",
        phase="install",
        status="done",
        progress={"progress_percent": 5, "progress_label": "dependency install done", "missing_packages": missing},
    )


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


def _resolve_artifact_dir() -> Path:
    raw = os.getenv("CONTOUR_NEURAL_ARTIFACT_DIR", "").strip()
    if raw:
        path = Path(raw)
        if not path.is_absolute():
            path = ROOT / path
        if path.exists():
            return path
        raise RuntimeError(f"CONTOUR_NEURAL_ARTIFACT_DIR does not exist: {path}")
    default = ROOT / "docs/features/countur_svg_generator/samples/generated/audit_1527"
    if default.exists():
        return default
    matches = sorted(KAGGLE_INPUT.rglob("audit_1527")) if KAGGLE_INPUT.exists() else []
    for match in matches:
        if (match / "edge_map.png").exists():
            return match
    raise RuntimeError("Cannot locate audit_1527 artifact directory with edge_map.png")


def _style_reference_path() -> Path | None:
    raw = os.getenv("CONTOUR_NEURAL_STYLE_REFERENCE", "").strip()
    if raw:
        path = Path(raw)
        if not path.is_absolute():
            path = ROOT / path
        if not path.exists():
            raise RuntimeError(f"CONTOUR_NEURAL_STYLE_REFERENCE does not exist: {path}")
        return path
    default = ROOT / "docs/features/countur_svg_generator/samples/output/IMG_20260614_115550.webp"
    return default if default.exists() else None


def _source_image_path() -> Path:
    raw = os.getenv("CONTOUR_NEURAL_SOURCE_IMAGE", "").strip()
    if raw:
        path = Path(raw)
        if not path.is_absolute():
            path = ROOT / path
        if not path.exists():
            raise RuntimeError(f"CONTOUR_NEURAL_SOURCE_IMAGE does not exist: {path}")
        return path
    default = ROOT / "docs/features/countur_svg_generator/samples/input/image - 2026-06-14T115705.752.png"
    if default.exists():
        return default
    input_dir = ROOT / "docs/features/countur_svg_generator/samples/input"
    if input_dir.exists():
        images = sorted([*input_dir.glob("*.png"), *input_dir.glob("*.jpg"), *input_dir.glob("*.jpeg"), *input_dir.glob("*.webp")])
        if images:
            return images[0]
    raise RuntimeError("Cannot locate source photo for neural branch")


def _variants() -> tuple[str, ...]:
    raw = os.getenv("CONTOUR_NEURAL_VARIANTS", "A1,A3,C2,D1,E1")
    return tuple(item.strip() for item in raw.split(",") if item.strip())


def _seeds() -> tuple[int, ...]:
    raw = os.getenv("CONTOUR_NEURAL_SEEDS", "42")
    return tuple(int(item.strip()) for item in raw.split(",") if item.strip())


def main() -> None:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    status_client = _load_status_client()
    status_client.start_alive(interval_seconds=60, progress_provider=_status_progress)
    _status_event(
        status_client,
        "kernel_started",
        phase="bootstrap",
        progress={"progress_percent": 0, "progress_label": "kernel started"},
    )
    try:
        _install_requirements(status_client)
        _status_event(
            status_client,
            "preflight_started",
            phase="preflight",
            progress={"progress_percent": 8, "progress_label": "neural branch preflight"},
        )
        cuda_info = _torch_cuda_probe()
        artifact_dir = _resolve_artifact_dir()
        source_image = _source_image_path()
        style_reference = _style_reference_path()
        _status_event(
            status_client,
            "preflight_ok",
            phase="preflight",
            status="done",
            progress={
                "progress_percent": 12,
                "progress_label": "preflight ok",
                "artifact_dir": str(artifact_dir),
                "source_image": str(source_image),
                "style_reference": str(style_reference) if style_reference else None,
                "cuda": cuda_info,
            },
        )

        from contour_svg.neural_branch import NeuralBranchConfig, run_neural_branch

        _status_event(
            status_client,
            "neural_inputs_started",
            phase="neural_inputs",
            progress={"progress_percent": 18, "progress_label": "prepare neural input maps"},
        )
        config = NeuralBranchConfig(
            artifact_dir=artifact_dir,
            out_dir=DEFAULT_OUT,
            source_image=source_image,
            style_reference=style_reference,
            variants=_variants(),
            seeds=_seeds(),
            run_neural=True,
            steps=int(os.getenv("CONTOUR_NEURAL_STEPS", "24")),
            strength=float(os.getenv("CONTOUR_NEURAL_STRENGTH", "0.93")),
            style_rewrite_strength=float(os.getenv("CONTOUR_NEURAL_STYLE_REWRITE_STRENGTH", "0.95")),
            guidance_scale=float(os.getenv("CONTOUR_NEURAL_GUIDANCE_SCALE", "8.5")),
            control_scale=float(os.getenv("CONTOUR_NEURAL_CONTROL_SCALE", "1.35")),
            style_reference_adapter_scale=float(os.getenv("CONTOUR_NEURAL_STYLE_REFERENCE_ADAPTER_SCALE", "0.55")),
        )
        _status_event(
            status_client,
            "neural_img2img_started",
            phase="neural_img2img",
            progress={
                "progress_percent": 35,
                "progress_label": "run neural img2img candidates",
                "variants": list(config.variants),
                "seeds": list(config.seeds),
            },
        )
        report = run_neural_branch(config)
        candidate_count = len(report.get("candidates") or [])
        _status_event(
            status_client,
            "neural_report_written",
            phase="neural_report",
            status="done",
            progress={
                "progress_percent": 96,
                "progress_label": "neural branch report written",
                "candidate_count": candidate_count,
                "output_dir": str(DEFAULT_OUT),
            },
        )
        _status_event(
            status_client,
            "report_written",
            phase="report",
            status="done",
            progress={
                "progress_percent": 100,
                "progress_label": "report written",
                "candidate_count": candidate_count,
                "output_dir": str(DEFAULT_OUT),
            },
        )
        print(json.dumps({"status": "ok", "output_dir": str(DEFAULT_OUT), "candidate_count": candidate_count}, ensure_ascii=False, indent=2))
    except Exception as exc:
        _status_event(
            status_client,
            "report_written",
            phase="failed",
            status="failed",
            progress={**_status_progress(), "progress_label": "neural branch failed"},
            message=f"{exc.__class__.__name__}: {exc}\n{traceback.format_exc(limit=4)}",
        )
        raise
    finally:
        status_client.stop_alive()


if __name__ == "__main__":
    main()
