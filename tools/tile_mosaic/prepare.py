"""Input materialisation and continuous texture preparation."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
import shutil
from typing import Any, Mapping
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from PIL import Image, ImageEnhance, ImageFilter, ImageOps

MAX_INPUT_BYTES = 40 * 1024 * 1024
USER_AGENT = "events-bot-tile-mosaic-material-lab/2.0"

try:
    RESAMPLING = Image.Resampling
except AttributeError:  # pragma: no cover
    RESAMPLING = Image


@dataclass(frozen=True)
class PreparedTextures:
    base_path: Path
    input_sha256: str
    source_width: int
    source_height: int
    crop_box: tuple[int, int, int, int]


def sha256_file(path: str | Path) -> str:
    digest = sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_extension(reference: str) -> str:
    suffix = Path(urlparse(reference).path).suffix.lower()
    return suffix if suffix in {".jpg", ".jpeg", ".png", ".webp"} else ".img"


def materialize_input(reference: str, destination_dir: str | Path) -> Path:
    destination = Path(destination_dir)
    destination.mkdir(parents=True, exist_ok=True)
    parsed = urlparse(str(reference))
    output = destination / f"source{_safe_extension(str(reference))}"

    if parsed.scheme in {"http", "https"}:
        request = Request(str(reference), headers={"User-Agent": USER_AGENT})
        total = 0
        with urlopen(request, timeout=45) as response, output.open("wb") as handle:  # noqa: S310
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > MAX_INPUT_BYTES:
                    raise ValueError(f"remote input exceeds {MAX_INPUT_BYTES} bytes")
                handle.write(chunk)
    elif parsed.scheme:
        raise ValueError("input URL must use HTTP(S) or be a local path")
    else:
        source = Path(reference).expanduser().resolve()
        if not source.is_file():
            raise FileNotFoundError(source)
        size = source.stat().st_size
        if size > MAX_INPUT_BYTES:
            raise ValueError(f"local input exceeds {MAX_INPUT_BYTES} bytes")
        shutil.copyfile(source, output)

    if not output.is_file() or output.stat().st_size < 256:
        raise ValueError("input image is missing or empty")
    return output


def load_image(path: str | Path) -> Image.Image:
    with Image.open(path) as opened:
        image = ImageOps.exif_transpose(opened)
        image.load()
        return image.convert("RGB")


def cover_crop_box(
    source_size: tuple[int, int],
    target_size: tuple[int, int],
    *,
    focal_x: float,
    focal_y: float,
) -> tuple[int, int, int, int]:
    source_width, source_height = source_size
    target_width, target_height = target_size
    if min(source_width, source_height, target_width, target_height) <= 0:
        raise ValueError("source and target dimensions must be positive")
    source_aspect = source_width / source_height
    target_aspect = target_width / target_height
    focal_x = min(1.0, max(0.0, float(focal_x)))
    focal_y = min(1.0, max(0.0, float(focal_y)))

    if source_aspect > target_aspect:
        crop_height = source_height
        crop_width = max(1, round(crop_height * target_aspect))
        left = round(focal_x * source_width - crop_width / 2)
        left = min(source_width - crop_width, max(0, left))
        top = 0
    else:
        crop_width = source_width
        crop_height = max(1, round(crop_width / target_aspect))
        left = 0
        top = round(focal_y * source_height - crop_height / 2)
        top = min(source_height - crop_height, max(0, top))
    return (left, top, left + crop_width, top + crop_height)


def _apply_state_texture(base: Image.Image, state: Mapping[str, Any], *, scale: float) -> Image.Image:
    texture = base
    blur = float(state.get("blur_px", 0.0)) * scale
    if blur > 0:
        texture = texture.filter(ImageFilter.GaussianBlur(blur))
    texture = ImageEnhance.Color(texture).enhance(float(state.get("saturation", 1.0)))
    texture = ImageEnhance.Brightness(texture).enhance(float(state.get("brightness", 1.0)))
    texture = ImageEnhance.Contrast(texture).enhance(float(state.get("contrast", 1.0)))
    return texture


def prepare_textures(
    *,
    input_path: str | Path,
    plan: Mapping[str, Any],
    output_dir: str | Path,
) -> PreparedTextures:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    image = load_image(input_path)
    source_width, source_height = image.size
    texture_size = (
        int(plan["grid"]["texture_width"]),
        int(plan["grid"]["texture_height"]),
    )
    focal = plan.get("focal_point") or {"x": 0.5, "y": 0.5}
    crop_box = cover_crop_box(
        image.size,
        texture_size,
        focal_x=float(focal.get("x", 0.5)),
        focal_y=float(focal.get("y", 0.5)),
    )
    base = image.crop(crop_box).resize(texture_size, RESAMPLING.LANCZOS)
    base_path = output / "texture-base.png"
    base.save(base_path, format="PNG", optimize=True)

    scale = texture_size[0] / max(1, int(plan["canvas"]["width"]))
    for state_name, state in plan["states"].items():
        state_texture = _apply_state_texture(base, state, scale=scale)
        state_texture.save(output / f"texture-{state_name}.png", format="PNG", optimize=True)

    return PreparedTextures(
        base_path=base_path,
        input_sha256=sha256_file(input_path),
        source_width=source_width,
        source_height=source_height,
        crop_box=crop_box,
    )
