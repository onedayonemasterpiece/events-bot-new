from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Any


class MissingDependencyError(RuntimeError):
    """Raised when a required runtime stage is requested without deps."""


@dataclass(frozen=True)
class DependencyStatus:
    module: str
    available: bool
    version: str | None = None
    error: str | None = None


def optional_import(module: str) -> Any | None:
    try:
        return importlib.import_module(module)
    except Exception:
        return None


def require_module(module: str, package_hint: str | None = None) -> Any:
    try:
        mod = importlib.import_module(module)
    except Exception as exc:
        hint = f" Install `{package_hint}`." if package_hint else ""
        raise MissingDependencyError(
            f"Missing required dependency `{module}` ({type(exc).__name__}: {exc}).{hint}"
        ) from exc
    return mod


def check_module(module: str) -> DependencyStatus:
    try:
        mod = importlib.import_module(module)
    except Exception as exc:
        return DependencyStatus(module=module, available=False, error=f"{type(exc).__name__}: {exc}")
    return DependencyStatus(module=module, available=True, version=getattr(mod, "__version__", None))


def has_cuda() -> bool:
    torch = optional_import("torch")
    if torch is None:
        return False
    try:
        return bool(torch.cuda.is_available())
    except Exception:
        return False


def dependency_report() -> dict[str, dict[str, str | bool | None]]:
    modules = [
        "PIL",
        "cv2",
        "numpy",
        "skimage",
        "svgwrite",
        "cairosvg",
        "torch",
        "transformers",
        "ultralytics",
        "diffusers",
        "controlnet_aux",
        "google_ai",
        "sam2",
    ]
    return {status.module: status.__dict__ for status in (check_module(m) for m in modules)}
