from __future__ import annotations

import json
import os
import importlib.util
import subprocess
import sys
import traceback
import urllib.request
import shutil
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
KAGGLE_INPUT = Path("/kaggle/input")


def _find_repo_root() -> Path:
    local_root = Path(__file__).resolve().parents[2]
    if (local_root / "contour_svg").exists():
        return local_root
    for root in (KAGGLE_INPUT, Path("/kaggle/working")):
        if not root.exists():
            continue
        for candidate in root.rglob("contour_svg"):
            if candidate.is_dir() and (candidate / "pipeline.py").exists():
                return candidate.parent
    return local_root


ROOT = _find_repo_root()
DEFAULT_CONFIG = ROOT / "docs/features/countur_svg_generator/examples/sample_building.yaml"
DEFAULT_OUT = Path("/kaggle/working/contour_svg_run")
STATUS_PROGRESS = {
    "phase": "bootstrap",
    "progress_percent": 0,
    "progress_label": "bootstrap",
}
REQUIRED_IMPORTS = [
    ("PIL", "pillow"),
    ("cv2", "opencv-python-headless"),
    ("numpy", "numpy"),
    ("skimage", "scikit-image"),
    ("svgwrite", "svgwrite"),
    ("cairosvg", "cairosvg"),
    ("torch", "torch"),
    ("torchvision", "torchvision"),
    ("transformers", "transformers"),
    ("ultralytics", "ultralytics"),
    ("diffusers", "diffusers"),
    ("controlnet_aux", "controlnet-aux"),
    ("google.genai", "google-genai"),
    ("supabase", "supabase"),
    ("yaml", "pyyaml"),
    ("hydra", "hydra-core"),
    ("omegaconf", "omegaconf"),
    ("iopath", "iopath"),
    ("tqdm", "tqdm"),
    ("sam2", "sam2"),
]
SAM2_TINY_CHECKPOINT_URL = "https://dl.fbaipublicfiles.com/segment_anything_2/092824/sam2.1_hiera_tiny.pt"
EXTERNAL_RUNTIME_DIR = Path("/tmp/contour_svg_external")
SAM2_TINY_CHECKPOINT_PATH = EXTERNAL_RUNTIME_DIR / "checkpoints/sam2.1_hiera_tiny.pt"
TORCH_CUDA_INDEX_URL = "https://download.pytorch.org/whl/cu121"
TORCH_CUDA_PACKAGES = ["torch==2.5.1", "torchvision==0.20.1"]
DEEPLSD_RUNTIME_PACKAGES = [
    "kornia>=0.6",
    "brewer2mpl",
    "h5py",
    "flow_vis",
    "seaborn",
    "future",
    "shapely",
]
DEEPLSD_REPO_URL = "https://github.com/cvg/DeepLSD.git"
DEEPLSD_REPO_DIR = EXTERNAL_RUNTIME_DIR / "DeepLSD"
DEEPLSD_CHECKPOINT_URL = "https://cvg-data.inf.ethz.ch/DeepLSD/deeplsd_md.tar"
DEEPLSD_CHECKPOINT_PATH = EXTERNAL_RUNTIME_DIR / "checkpoints/deeplsd_md.tar"
HAWP_REPO_URL = "https://github.com/cherubicXN/hawp.git"
HAWP_REPO_DIR = EXTERNAL_RUNTIME_DIR / "hawp"
HAWP_CHECKPOINT_URL = "https://github.com/cherubicXN/hawp-torchhub/releases/download/HAWPv3/hawpv3-imagenet-03a84.pth"
HAWP_CHECKPOINT_PATH = EXTERNAL_RUNTIME_DIR / "checkpoints/hawpv3-imagenet-03a84.pth"
YOLOWORLD_MODEL_URL = "https://github.com/ultralytics/assets/releases/download/v8.4.0/yolov8s-worldv2.pt"
YOLOWORLD_MODEL_PATH = EXTERNAL_RUNTIME_DIR / "models/yolov8s-worldv2.pt"


def _load_status_client():
    for path in [SCRIPT_DIR, ROOT / "kaggle"]:
        if str(path) not in sys.path:
            sys.path.insert(0, str(path))
    from kaggle_status_client import load_status_client

    return load_status_client(output_dir=DEFAULT_OUT, log=lambda message: print(message, flush=True))


def _status_progress(status=None) -> dict:
    if status is not None:
        return dict(status.progress)
    return dict(STATUS_PROGRESS)


def _status_event(client, event: str, *, phase: str, status: str = "running", progress: dict | None = None, message: str | None = None) -> None:
    payload = dict(progress or {})
    payload.setdefault("phase", phase)
    STATUS_PROGRESS.update(payload)
    STATUS_PROGRESS["phase"] = phase
    client.event(event, phase=phase, status=status, progress=payload, message=message)


def _find_first_input(filename: str) -> Path | None:
    roots = [KAGGLE_INPUT, Path("/kaggle/working"), ROOT]
    for root in roots:
        if not root.exists():
            continue
        direct = root / filename
        if direct.exists():
            return direct
        matches = sorted(path for path in root.rglob(filename) if path.is_file())
        if matches:
            return matches[0]
    return None


def _load_contour_run_config() -> dict:
    path = _find_first_input("contour_run_config.json")
    if path is None:
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"Invalid contour_run_config.json at {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise RuntimeError(f"Invalid contour_run_config.json at {path}: expected object")
    return data


def _repo_relative_path(path: str | Path) -> str:
    raw = Path(os.path.expandvars(str(path))).expanduser()
    if raw.is_absolute():
        return str(raw)
    return str(ROOT / raw)


def _load_encrypted_env(status_client) -> None:
    enc_path = _find_first_input("secrets.enc")
    key_path = _find_first_input("fernet.key")
    if enc_path is None or key_path is None:
        raise RuntimeError("Encrypted Kaggle secret datasets are required: secrets.enc and fernet.key")
    from cryptography.fernet import Fernet

    payload = Fernet(key_path.read_bytes().strip()).decrypt(enc_path.read_bytes())
    loaded = json.loads(payload.decode("utf-8"))
    if not isinstance(loaded, dict):
        raise RuntimeError("Encrypted secrets payload must be a JSON object")
    names: list[str] = []
    for key, value in loaded.items():
        env_name = str(key).strip()
        if not env_name or value is None:
            continue
        os.environ[env_name] = str(value)
        names.append(env_name)
    required = [
        "GOOGLE_API_KEY",
        "GOOGLE_AI_LIMITER_SUPABASE_URL",
        "GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY",
    ]
    missing = [name for name in required if not (os.getenv(name) or "").strip()]
    if missing:
        raise RuntimeError(f"Encrypted secrets payload is missing required keys: {', '.join(missing)}")
    _status_event(
        status_client,
        "secrets_loaded",
        phase="preflight",
        status="done",
        progress={
            "progress_percent": 6,
            "progress_label": "encrypted secrets loaded",
            "secret_names": sorted(names),
        },
    )


def _install_requirements(status_client) -> None:
    req = Path(__file__).resolve().parent / "requirements-kaggle.txt"
    if os.getenv("CONTOUR_SKIP_PIP_INSTALL") == "1":
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
        progress={"progress_percent": 2, "progress_label": "install contour dependencies"},
    )
    missing = [package for module, package in REQUIRED_IMPORTS if importlib.util.find_spec(module) is None]
    if missing:
        _status_event(
            status_client,
            "install_missing_started",
            phase="install",
            progress={
                "progress_percent": 3,
                "progress_label": "install missing dependencies",
                "missing_packages": missing,
            },
        )
        for index, package in enumerate(missing, start=1):
            _status_event(
                status_client,
                "install_package_started",
                phase="install",
                progress={
                    "progress_percent": 3,
                    "progress_label": f"install {package}",
                    "package_index": index,
                    "package_total": len(missing),
                    "package": package,
                    "missing_packages": missing,
                },
            )
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
    _ensure_torch_cuda_runtime(status_client)
    _status_event(
        status_client,
        "install_done",
        phase="install",
        status="done",
        progress={"progress_percent": 5, "progress_label": "dependency install done"},
    )


def _torch_cuda_probe() -> tuple[bool, str]:
    probe = """
import json
import torch
info = {
    "torch": getattr(torch, "__version__", None),
    "cuda": getattr(torch.version, "cuda", None),
    "cuda_available": torch.cuda.is_available(),
    "device_name": torch.cuda.get_device_name(0) if torch.cuda.is_available() else None,
    "capability": torch.cuda.get_device_capability(0) if torch.cuda.is_available() else None,
}
if not torch.cuda.is_available():
    raise RuntimeError("CUDA is not available")
x = torch.randn((16, 16), device="cuda")
y = (x @ x).sum()
y.backward() if y.requires_grad else None
torch.cuda.synchronize()
print(json.dumps(info, ensure_ascii=False))
"""
    proc = subprocess.run(
        [sys.executable, "-c", probe],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=45,
    )
    return proc.returncode == 0, proc.stdout.strip()


def _ensure_torch_cuda_runtime(status_client) -> None:
    ok, details = _torch_cuda_probe()
    if ok:
        _status_event(
            status_client,
            "torch_cuda_probe_ok",
            phase="install",
            status="done",
            progress={
                "progress_percent": 5,
                "progress_label": "torch CUDA runtime ok",
                "torch_cuda_probe": details[-1000:],
            },
        )
        return
    if os.getenv("CONTOUR_ALLOW_TORCH_CUDA_REINSTALL") != "1":
        _status_event(
            status_client,
            "torch_cuda_probe_failed",
            phase="install",
            status="failed",
            progress={
                "progress_percent": 4,
                "progress_label": "torch CUDA runtime incompatible",
                "torch_cuda_probe_error": details[-2000:],
            },
        )
        raise RuntimeError(
            "PyTorch CUDA runtime is incompatible with the assigned Kaggle GPU; "
            "run this contour pipeline with a T4 accelerator. "
            f"Probe: {details[-2000:]}"
        )
    if os.getenv("CONTOUR_TORCH_CUDA_REINSTALLED") == "1":
        raise RuntimeError(f"PyTorch CUDA runtime is still incompatible after reinstall: {details[-2000:]}")
    _status_event(
        status_client,
        "torch_cuda_reinstall_started",
        phase="install",
        progress={
            "progress_percent": 4,
            "progress_label": "install compatible torch CUDA runtime",
            "torch_cuda_probe_error": details[-2000:],
            "torch_packages": TORCH_CUDA_PACKAGES,
        },
    )
    subprocess.check_call(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--upgrade",
            "--force-reinstall",
            "--index-url",
            TORCH_CUDA_INDEX_URL,
            *TORCH_CUDA_PACKAGES,
        ]
    )
    os.environ["CONTOUR_TORCH_CUDA_REINSTALLED"] = "1"
    _status_event(
        status_client,
        "torch_cuda_reinstall_done",
        phase="install",
        status="done",
        progress={"progress_percent": 5, "progress_label": "restart after torch CUDA install"},
    )
    os.execv(sys.executable, [sys.executable, __file__])


def _ensure_sam2_checkpoint(status_client) -> Path:
    explicit = (os.getenv("CONTOUR_SAM2_CHECKPOINT") or "").strip()
    if explicit:
        path = Path(explicit)
        if not path.exists():
            raise RuntimeError(f"CONTOUR_SAM2_CHECKPOINT does not exist: {path}")
        return path
    SAM2_TINY_CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not SAM2_TINY_CHECKPOINT_PATH.exists() or SAM2_TINY_CHECKPOINT_PATH.stat().st_size < 1024:
        _status_event(
            status_client,
            "sam2_checkpoint_download_started",
            phase="install",
            progress={"progress_percent": 6, "progress_label": "download SAM2.1 checkpoint"},
        )
        urllib.request.urlretrieve(SAM2_TINY_CHECKPOINT_URL, SAM2_TINY_CHECKPOINT_PATH)
    if not SAM2_TINY_CHECKPOINT_PATH.exists() or SAM2_TINY_CHECKPOINT_PATH.stat().st_size < 1024:
        raise RuntimeError("SAM2.1 checkpoint download failed")
    _status_event(
        status_client,
        "sam2_checkpoint_ready",
        phase="install",
        status="done",
        progress={
            "progress_percent": 7,
            "progress_label": "SAM2.1 checkpoint ready",
            "sam2_checkpoint": str(SAM2_TINY_CHECKPOINT_PATH),
        },
    )
    return SAM2_TINY_CHECKPOINT_PATH


def _ensure_deeplsd_runtime(status_client) -> Path:
    explicit = (os.getenv("CONTOUR_DEEPLSD_CHECKPOINT") or "").strip()
    if explicit:
        path = Path(explicit)
        if not path.exists():
            raise RuntimeError(f"CONTOUR_DEEPLSD_CHECKPOINT does not exist: {path}")
        return path
    if importlib.util.find_spec("deeplsd") is None:
        _status_event(
            status_client,
            "deeplsd_install_started",
            phase="install",
            progress={"progress_percent": 6, "progress_label": "install DeepLSD"},
        )
        if not DEEPLSD_REPO_DIR.exists():
            subprocess.check_call(
                [
                    "git",
                    "clone",
                    "--recurse-submodules",
                    "--depth",
                    "1",
                    DEEPLSD_REPO_URL,
                    str(DEEPLSD_REPO_DIR),
                ]
            )
        subprocess.check_call([sys.executable, "-m", "pip", "install", *DEEPLSD_RUNTIME_PACKAGES])
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-e", ".", "--no-deps"], cwd=str(DEEPLSD_REPO_DIR))
        _status_event(
            status_client,
            "deeplsd_install_done",
            phase="install",
            status="done",
            progress={"progress_percent": 7, "progress_label": "DeepLSD installed"},
        )
    DEEPLSD_CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not DEEPLSD_CHECKPOINT_PATH.exists() or DEEPLSD_CHECKPOINT_PATH.stat().st_size < 1024:
        _status_event(
            status_client,
            "deeplsd_checkpoint_download_started",
            phase="install",
            progress={"progress_percent": 7, "progress_label": "download DeepLSD checkpoint"},
        )
        urllib.request.urlretrieve(DEEPLSD_CHECKPOINT_URL, DEEPLSD_CHECKPOINT_PATH)
    if not DEEPLSD_CHECKPOINT_PATH.exists() or DEEPLSD_CHECKPOINT_PATH.stat().st_size < 1024:
        raise RuntimeError("DeepLSD checkpoint download failed")
    _status_event(
        status_client,
        "deeplsd_ready",
        phase="install",
        status="done",
        progress={
            "progress_percent": 8,
            "progress_label": "DeepLSD runtime ready",
            "deeplsd_checkpoint": str(DEEPLSD_CHECKPOINT_PATH),
        },
    )
    return DEEPLSD_CHECKPOINT_PATH


def _ensure_hawp_runtime(status_client) -> Path:
    explicit = (os.getenv("CONTOUR_HAWP_CHECKPOINT") or "").strip()
    if explicit:
        path = Path(explicit)
        if not path.exists():
            raise RuntimeError(f"CONTOUR_HAWP_CHECKPOINT does not exist: {path}")
        return path
    if importlib.util.find_spec("hawp") is None:
        _status_event(
            status_client,
            "hawp_install_started",
            phase="install",
            progress={"progress_percent": 7, "progress_label": "install HAWP"},
        )
        if not HAWP_REPO_DIR.exists():
            subprocess.check_call(
                [
                    "git",
                    "clone",
                    "--depth",
                    "1",
                    HAWP_REPO_URL,
                    str(HAWP_REPO_DIR),
                ]
            )
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-e", "."], cwd=str(HAWP_REPO_DIR))
        _status_event(
            status_client,
            "hawp_install_done",
            phase="install",
            status="done",
            progress={"progress_percent": 8, "progress_label": "HAWP installed"},
        )
    HAWP_CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not HAWP_CHECKPOINT_PATH.exists() or HAWP_CHECKPOINT_PATH.stat().st_size < 1024:
        _status_event(
            status_client,
            "hawp_checkpoint_download_started",
            phase="install",
            progress={"progress_percent": 8, "progress_label": "download HAWP checkpoint"},
        )
        urllib.request.urlretrieve(HAWP_CHECKPOINT_URL, HAWP_CHECKPOINT_PATH)
    if not HAWP_CHECKPOINT_PATH.exists() or HAWP_CHECKPOINT_PATH.stat().st_size < 1024:
        raise RuntimeError("HAWP checkpoint download failed")
    _status_event(
        status_client,
        "hawp_ready",
        phase="install",
        status="done",
        progress={
            "progress_percent": 8,
            "progress_label": "HAWP runtime ready",
            "hawp_checkpoint": str(HAWP_CHECKPOINT_PATH),
        },
    )
    return HAWP_CHECKPOINT_PATH


def _ensure_yoloworld_runtime(status_client) -> Path:
    os.environ.setdefault("HF_HOME", str(EXTERNAL_RUNTIME_DIR / "hf_home"))
    os.environ.setdefault("HF_HUB_CACHE", str(EXTERNAL_RUNTIME_DIR / "hf_home/hub"))
    os.environ.setdefault("HUGGINGFACE_HUB_CACHE", str(EXTERNAL_RUNTIME_DIR / "hf_home/hub"))
    os.environ.setdefault("TRANSFORMERS_CACHE", str(EXTERNAL_RUNTIME_DIR / "hf_home/transformers"))
    os.environ.setdefault("YOLO_CONFIG_DIR", str(EXTERNAL_RUNTIME_DIR / "ultralytics"))
    os.environ.setdefault("TORCH_HOME", str(EXTERNAL_RUNTIME_DIR / "torch_home"))
    os.environ.setdefault("XDG_CACHE_HOME", str(EXTERNAL_RUNTIME_DIR / "cache"))
    os.environ.setdefault("CLIP_HOME", str(EXTERNAL_RUNTIME_DIR / "clip"))
    os.environ.setdefault("ULTRALYTICS_HUB_CACHE_DIR", str(EXTERNAL_RUNTIME_DIR / "ultralytics_cache"))
    YOLOWORLD_MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not YOLOWORLD_MODEL_PATH.exists() or YOLOWORLD_MODEL_PATH.stat().st_size < 1024:
        _status_event(
            status_client,
            "yoloworld_model_download_started",
            phase="install",
            progress={"progress_percent": 8, "progress_label": "download YOLO-World model"},
        )
        urllib.request.urlretrieve(YOLOWORLD_MODEL_URL, YOLOWORLD_MODEL_PATH)
    if not YOLOWORLD_MODEL_PATH.exists() or YOLOWORLD_MODEL_PATH.stat().st_size < 1024:
        raise RuntimeError("YOLO-World model download failed")
    _status_event(
        status_client,
        "yoloworld_ready",
        phase="install",
        status="done",
        progress={
            "progress_percent": 8,
            "progress_label": "YOLO-World runtime ready",
            "yoloworld_model": str(YOLOWORLD_MODEL_PATH),
        },
    )
    return YOLOWORLD_MODEL_PATH


def _cleanup_external_runtime_outputs() -> None:
    for path in [
        Path("/kaggle/working/weights"),
        Path("/kaggle/working/yolov8s-worldv2.pt"),
        Path("/kaggle/working/.cache"),
        Path("/kaggle/working/hf_cache"),
    ]:
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        elif path.exists():
            path.unlink()


def main() -> None:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    status_client = _load_status_client()
    contour_status = None
    status_client.start_alive(interval_seconds=60, progress_provider=lambda: _status_progress(contour_status))
    _status_event(
        status_client,
        "kernel_started",
        phase="bootstrap",
        progress={"progress_percent": 0, "progress_label": "kernel started"},
    )
    try:
        _install_requirements(status_client)
    except Exception as exc:
        _status_event(
            status_client,
            "report_written",
            phase="failed",
            status="failed",
            progress={"progress_percent": 5, "progress_label": "dependency install failed"},
            message=f"{exc.__class__.__name__}: {exc}",
        )
        raise

    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from contour_svg import ContourGenerator, load_config
    from contour_svg.status import ContourStatus

    run_config = _load_contour_run_config()
    raw_config_path = (
        os.getenv("CONTOUR_CONFIG")
        or str(run_config.get("config_path") or "").strip()
        or str(DEFAULT_CONFIG)
    )
    config_path = Path(raw_config_path)
    if not config_path.is_absolute():
        config_path = ROOT / config_path
    overrides = {}
    input_override = os.getenv("CONTOUR_INPUT") or str(run_config.get("input_path") or "").strip()
    if input_override:
        overrides.setdefault("input", {})["image_path"] = input_override
    elif config_path.exists():
        with config_path.open("r", encoding="utf-8") as fh:
            import yaml

            selected_config_data = yaml.safe_load(fh) or {}
        input_path = ((selected_config_data.get("input") or {}).get("image_path") or "").strip()
        if input_path:
            overrides.setdefault("input", {})["image_path"] = _repo_relative_path(input_path)
    output_override = os.getenv("CONTOUR_OUTPUT_DIR") or str(run_config.get("output_dir") or "").strip()
    if output_override:
        overrides.setdefault("output", {})["output_dir"] = output_override
    elif config_path == DEFAULT_CONFIG:
        overrides.setdefault("output", {})["output_dir"] = str(DEFAULT_OUT)
    overrides.setdefault("runtime", {})["hf_cache_dir"] = os.getenv("CONTOUR_HF_CACHE_DIR", "/tmp/contour_hf_cache")
    if os.getenv("CONTOUR_SAM2_CHECKPOINT"):
        overrides.setdefault("segmentation", {})["sam2_checkpoint"] = os.environ["CONTOUR_SAM2_CHECKPOINT"]

    try:
        _load_encrypted_env(status_client)
        checkpoint_path = _ensure_sam2_checkpoint(status_client)
        deeplsd_checkpoint_path = _ensure_deeplsd_runtime(status_client)
        hawp_checkpoint_path = _ensure_hawp_runtime(status_client)
        yoloworld_model_path = _ensure_yoloworld_runtime(status_client)
        overrides.setdefault("segmentation", {})["sam2_checkpoint"] = str(checkpoint_path)
        overrides.setdefault("segmentation", {})["yoloworld_model"] = str(yoloworld_model_path)
        overrides.setdefault("geometry", {})["deeplsd_checkpoint"] = str(deeplsd_checkpoint_path)
        overrides.setdefault("geometry", {})["deeplsd_repo_dir"] = str(DEEPLSD_REPO_DIR)
        overrides.setdefault("geometry", {})["hawp_checkpoint"] = str(hawp_checkpoint_path)
        overrides.setdefault("geometry", {})["hawp_repo_dir"] = str(HAWP_REPO_DIR)
        _status_event(
            status_client,
            "preflight_started",
            phase="preflight",
            progress={"progress_percent": 6, "progress_label": "load contour config"},
        )
        config = load_config(config_path, overrides=overrides)
        from contour_svg.llm_gateway import assert_gateway_ready

        gateway_status = assert_gateway_ready(config.gemini.api_key_env)
        _status_event(
            status_client,
            "preflight_ok",
            phase="preflight",
            status="done",
            progress={
                "progress_percent": 8,
                "progress_label": "preflight ok",
                "config_path": str(config_path),
                "input": config.input.image_path,
                "output_dir": config.output.output_dir,
                "llm_gateway": gateway_status,
            },
        )
        contour_status = ContourStatus(status_client)
        result = ContourGenerator(config, status=contour_status).run()
        _cleanup_external_runtime_outputs()
        summary = {
            "status": "ok",
            "output_dir": str(result.output_dir),
            "final_svg": str(result.final_svg) if result.final_svg else None,
            "preview_png": str(result.preview_png) if result.preview_png else None,
            "candidate_count": len(result.candidates),
            "warnings": result.warnings,
        }
        _status_event(
            status_client,
            "report_written",
            phase="report",
            status="done",
            progress={
                "progress_percent": 100,
                "progress_label": "report written",
                "output_dir": str(result.output_dir),
                "candidate_count": len(result.candidates),
                "final_svg": str(result.final_svg) if result.final_svg else None,
            },
        )
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    except Exception as exc:
        _cleanup_external_runtime_outputs()
        _status_event(
            status_client,
            "report_written",
            phase="failed",
            status="failed",
            progress={**_status_progress(contour_status), "progress_label": "contour SVG run failed"},
            message=f"{exc.__class__.__name__}: {exc}\n{traceback.format_exc(limit=4)}",
        )
        raise
    finally:
        status_client.stop_alive()


if __name__ == "__main__":
    main()
