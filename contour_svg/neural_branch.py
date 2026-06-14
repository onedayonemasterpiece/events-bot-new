from __future__ import annotations

import json
import math
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .dependencies import MissingDependencyError, has_cuda, require_module


PROMPT_P1_SIMPLIFICATION = (
    "Black ink architectural contour drawing on white background. Keep source "
    "building identity, perspective, roofline, corner, windows, arches and base. "
    "Follow the control map. Make bold clean straight postcard lines. "
    "Complete tree-hidden roof, cornice and facade rhythm conservatively."
)

NEGATIVE_IDENTITY_DRIFT = (
    "photo, photorealistic, color, grey paper, shading, texture, filled drawing, "
    "watercolor, pencil sketch, black background, white ink, trees, foliage, "
    "fence, road, cars, people, different building, extra wing, watermark, text"
)


@dataclass(frozen=True)
class NeuralBranchConfig:
    artifact_dir: Path
    out_dir: Path
    source_image: Path | None = None
    style_reference: Path | None = None
    variants: tuple[str, ...] = ("A1", "A3", "C2", "D1")
    seeds: tuple[int, ...] = (42,)
    run_neural: bool = False
    output_size: tuple[int, int] = (768, 768)
    steps: int = 24
    guidance_scale: float = 8.5
    control_scale: float = 1.35
    strength: float = 0.93
    style_rewrite_strength: float = 0.95
    base_model: str = "runwayml/stable-diffusion-v1-5"
    lineart_controlnet: str = "lllyasviel/control_v11p_sd15_lineart"
    style_reference_adapter_model: str = "h94/IP-Adapter"
    style_reference_adapter_subfolder: str = "models"
    style_reference_adapter_weight_name: str = "ip-adapter_sd15.bin"
    style_reference_adapter_scale: float = 0.55


@dataclass
class PreparedNeuralInput:
    variant: str
    name: str
    mode: str
    path: Path
    prompt: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "variant": self.variant,
            "name": self.name,
            "mode": self.mode,
            "path": str(self.path),
            "prompt": self.prompt,
            "metadata": self.metadata,
        }


def run_neural_branch(config: NeuralBranchConfig) -> dict[str, Any]:
    """Prepare structural neural inputs and optionally run real img2img candidates.

    The preparation stage is deterministic and exists for debugging. When
    `run_neural=True`, this function requires CUDA + Diffusers and fails loudly
    if the real neural renderer cannot run.
    """

    branch_dir = config.out_dir / "neural_branch"
    input_dir = branch_dir / "input_maps"
    raw_dir = branch_dir / "raw_candidates"
    normalized_dir = branch_dir / "normalized_candidates"
    input_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    normalized_dir.mkdir(parents=True, exist_ok=True)

    prepared = prepare_neural_inputs(config, input_dir=input_dir)
    _write_contact_sheet(
        [(item.name, item.path) for item in prepared],
        branch_dir / "N0_inputs_contact_sheet.png",
        title="Neural Branch Inputs",
    )

    candidates: list[dict[str, Any]] = []
    if config.run_neural:
        candidates = _run_diffusion_candidates(config, prepared, raw_dir=raw_dir, normalized_dir=normalized_dir)
        if not candidates:
            raise RuntimeError("Neural branch ran but produced no candidates")
        best = _select_best_candidate(candidates)
        if best is None:
            raise RuntimeError("Neural branch produced candidates but no result candidate could be selected")
        shutil.copy2(best["thresholded_path"], branch_dir / "result.png")
        shutil.copy2(best["thresholded_path"], branch_dir / "result_thresholded.png")
        shutil.copy2(best["candidate_path"], branch_dir / "result_raw.png")
        _write_contact_sheet(
            [(Path(item["candidate_path"]).stem, Path(item["candidate_path"])) for item in candidates],
            branch_dir / "contact_sheet.png",
            title="Neural Branch Candidates",
        )
        _write_contact_sheet(
            [(Path(item["thresholded_path"]).stem, Path(item["thresholded_path"])) for item in candidates[:3]],
            branch_dir / "top3_contact_sheet.png",
            title="Neural Branch Top Thresholded",
        )
    else:
        _write_contact_sheet([], branch_dir / "contact_sheet.png", title="Neural Branch Not Executed")

    report = {
        "branch_name": "neural_mask_lineart_branch",
        "status": "executed" if config.run_neural else "prepared_only",
        "neural_executed": config.run_neural,
        "artifact_dir": str(config.artifact_dir),
        "out_dir": str(branch_dir),
        "model": config.base_model if config.run_neural else None,
        "lineart_controlnet": config.lineart_controlnet if config.run_neural else None,
        "variants": list(config.variants),
        "seeds": list(config.seeds),
        "style_rewrite_strength": config.style_rewrite_strength,
        "style_reference_adapter_scale": config.style_reference_adapter_scale,
        "source_image": str(_resolve_source_image(config)) if config.source_image or config.artifact_dir else None,
        "modes": ["neural_raw_mode", "neural_normalized_mode"],
        "prepared_inputs": [item.to_dict() for item in prepared],
        "candidates": candidates,
        "result_png": str(branch_dir / "result.png") if config.run_neural else None,
        "result_thresholded_png": str(branch_dir / "result_thresholded.png") if config.run_neural else None,
        "result_raw_png": str(branch_dir / "result_raw.png") if config.run_neural else None,
        "result_png_policy": "best thresholded black-line-on-white neural style rewrite",
        "accepted_as_final": False,
        "final_policy": "PNG style-rewrite proposal only; final SVG still requires PrimitiveScene/vectorization hard gates",
    }
    (branch_dir / "neural_branch_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def prepare_neural_inputs(config: NeuralBranchConfig, *, input_dir: Path) -> list[PreparedNeuralInput]:
    Image = require_module("PIL.Image", "Pillow")
    ImageFilter = require_module("PIL.ImageFilter", "Pillow")

    artifact_dir = config.artifact_dir
    edge_path = _require_artifact(artifact_dir, "edge_map.png")
    edge = Image.open(edge_path).convert("L")
    edge_control = _black_lines_on_white(edge)
    edge_mask = _line_mask_from_control(edge_control)
    prepared: list[PreparedNeuralInput] = []

    def save(name: str, img, *, variant: str, mode: str = "neural_raw_mode", metadata: dict[str, Any] | None = None) -> None:
        path = input_dir / f"{name}.png"
        img.convert("RGB").save(path)
        prepared.append(
            PreparedNeuralInput(
                variant=variant,
                name=name,
                mode=mode,
                path=path,
                prompt=_prompt_for_variant(variant),
                metadata=metadata or {},
            )
        )

    if _enabled(config, "A1"):
        save("edge_only", edge_control, variant="A1", metadata={"source": str(edge_path)})
    if _enabled(config, "A2"):
        binarized = _control_from_line_mask(edge_mask.point(lambda p: 255 if p >= 42 else 0))
        save("edge_binarized", binarized, variant="A2", mode="neural_normalized_mode")
    if _enabled(config, "A3"):
        thickened = _control_from_line_mask(edge_mask.point(lambda p: 255 if p >= 32 else 0).filter(ImageFilter.MaxFilter(3)))
        save("edge_thickened", thickened, variant="A3", mode="neural_normalized_mode")
    if _enabled(config, "B1"):
        shell = _shell_outline(artifact_dir, edge_control.size)
        save("edge_plus_shell", _control_from_line_mask(_lighter(edge_mask, shell)), variant="B1")
    if _enabled(config, "B2"):
        wall_plane = _wall_plane_outline(artifact_dir, edge_control.size)
        save("edge_plus_wall_plane", _control_from_line_mask(_lighter(edge_mask, wall_plane)), variant="B2")
    if _enabled(config, "C1"):
        occluder = _occluder_outline(artifact_dir, edge_control.size)
        save("edge_plus_occluder_mask", _control_from_line_mask(_lighter(edge_mask, occluder)), variant="C1")
    if _enabled(config, "C2"):
        save("edge_minus_occluders", _control_from_line_mask(_erase_occluders(edge_mask, artifact_dir)), variant="C2", mode="neural_normalized_mode")
    if _enabled(config, "D1"):
        features = _feature_hint_map(artifact_dir, edge_control.size)
        save("edge_plus_features", _control_from_line_mask(_lighter(edge_mask, features)), variant="D1")
    if _enabled(config, "D2"):
        shell = _shell_outline(artifact_dir, edge_control.size)
        features = _feature_hint_map(artifact_dir, edge_control.size)
        wall_plane = _wall_plane_outline(artifact_dir, edge_control.size)
        save(
            "edge_plus_planes_plus_features",
            _control_from_line_mask(_lighter(_lighter(edge_mask, shell), _lighter(features, wall_plane))),
            variant="D2",
        )
    if _enabled(config, "E1"):
        if config.style_reference is None:
            raise RuntimeError("E1 neural style-reference variant requires --style-reference")
        ref_path = config.style_reference
        if not ref_path.exists():
            raise RuntimeError(f"style reference does not exist: {ref_path}")
        shutil.copy2(ref_path, input_dir / "style_reference.png")
        save(
            "photo_plus_edge_style_reference",
            edge_control,
            variant="E1",
            metadata={"style_reference": str(input_dir / "style_reference.png")},
        )

    if not prepared:
        raise RuntimeError(f"No neural branch input variants enabled: {config.variants}")
    return prepared


def _run_diffusion_candidates(
    config: NeuralBranchConfig,
    prepared: list[PreparedNeuralInput],
    *,
    raw_dir: Path,
    normalized_dir: Path,
) -> list[dict[str, Any]]:
    if not has_cuda():
        raise MissingDependencyError("Neural branch img2img requires CUDA/GPU")
    torch = require_module("torch", "torch")
    diffusers = require_module("diffusers", "diffusers")
    Image = require_module("PIL.Image", "Pillow")

    dtype = torch.float16
    controlnet = diffusers.ControlNetModel.from_pretrained(config.lineart_controlnet, torch_dtype=dtype)
    pipe = diffusers.StableDiffusionControlNetImg2ImgPipeline.from_pretrained(
        config.base_model,
        controlnet=controlnet,
        torch_dtype=dtype,
        safety_checker=None,
        requires_safety_checker=False,
    ).to("cuda")
    try:
        pipe.enable_xformers_memory_efficient_attention()
    except Exception:
        pass
    # Do not enable attention slicing before IP-Adapter loading: current
    # Diffusers replaces attention processors with SlicedAttnProcessor, which
    # breaks IP-Adapter processor conversion on Kaggle.
    style_reference_image = None
    style_adapter_loaded = False
    if config.style_reference is not None and config.style_reference.exists():
        style_reference_image = _fit_pad_rgb(Image.open(config.style_reference).convert("RGB"), size=config.output_size, fill=(255, 255, 255))
    source_init = _source_photo_init(config)
    init_debug = raw_dir.parent / "source_photo_init.png"
    source_init.save(init_debug)

    candidates: list[dict[str, Any]] = []
    for item in prepared:
        if item.variant not in set(config.variants):
            continue
        control_source = _fit_pad_rgb(Image.open(item.path).convert("RGB"), size=config.output_size, fill=(255, 255, 255))
        init = source_init
        condition = _controlnet_condition(control_source)
        kwargs = {}
        if item.variant == "E1":
            if style_reference_image is None:
                raise RuntimeError("E1 style rewrite requires style reference image")
            if not hasattr(pipe, "load_ip_adapter") or not hasattr(pipe, "set_ip_adapter_scale"):
                raise MissingDependencyError("Diffusers pipeline must support IP-Adapter for E1 neural style rewrite")
            if not style_adapter_loaded:
                pipe.load_ip_adapter(
                    config.style_reference_adapter_model,
                    subfolder=config.style_reference_adapter_subfolder,
                    weight_name=config.style_reference_adapter_weight_name,
                )
                style_adapter_loaded = True
            pipe.set_ip_adapter_scale(float(config.style_reference_adapter_scale))
            kwargs["ip_adapter_image"] = style_reference_image
        elif style_adapter_loaded:
            pipe.set_ip_adapter_scale(0.0)
        for seed in config.seeds:
            generator = torch.Generator(device="cuda").manual_seed(int(seed))
            result = pipe(
                prompt=item.prompt,
                negative_prompt=NEGATIVE_IDENTITY_DRIFT,
                image=init,
                control_image=condition,
                num_inference_steps=int(config.steps),
                guidance_scale=float(config.guidance_scale),
                controlnet_conditioning_scale=float(config.control_scale),
                strength=float(config.style_rewrite_strength if item.variant == "E1" else config.strength),
                generator=generator,
                **kwargs,
            ).images[0]
            candidate_path = raw_dir / f"N_{item.name}_seed{seed}.png"
            result.save(candidate_path)
            thresholded_path = normalized_dir / f"N_{item.name}_seed{seed}_thresholded.png"
            thresholded = _threshold_line_art(result)
            thresholded.save(thresholded_path)
            overlay_path = normalized_dir / f"N_{item.name}_seed{seed}_overlay_vs_edge.png"
            _overlay_vs_input(init, thresholded).save(overlay_path)
            metrics = _line_art_metrics(thresholded)
            report_path = normalized_dir / f"N_{item.name}_seed{seed}_report.json"
            candidate_report = {
                "branch_name": f"N_{item.name}",
                "variant": item.variant,
                "mode": item.mode,
                "model": config.base_model,
                "controlnet": config.lineart_controlnet,
                "source_image": str(_resolve_source_image(config)),
                "source_init_path": str(init_debug),
                "style_reference_adapter": config.style_reference_adapter_model if item.variant == "E1" else None,
                "style_reference_adapter_scale": config.style_reference_adapter_scale if item.variant == "E1" else None,
                "seed": seed,
                "candidate_path": str(candidate_path),
                "thresholded_path": str(thresholded_path),
                "overlay_vs_edge_path": str(overlay_path),
                "identity_score": None,
                "postcardness_score": metrics["postcardness_proxy"],
                "structure_score": metrics["structure_proxy"],
                "line_simplicity_score": metrics["line_simplicity_proxy"],
                "vectorization_readiness": metrics["vectorization_readiness_proxy"],
                "accepted_for_fusion": metrics["line_density"] >= 0.01 and metrics["line_density"] <= 0.34,
                "accepted_as_final": False,
                "rejection_reason": "final SVG requires PrimitiveScene; neural raster is proposal evidence only",
                "metrics": metrics,
            }
            report_path.write_text(json.dumps(candidate_report, ensure_ascii=False, indent=2), encoding="utf-8")
            candidates.append({**candidate_report, "candidate_report": str(report_path)})
    return candidates


def _prompt_for_variant(variant: str) -> str:
    if variant.startswith("C"):
        return (
            "Black ink architectural contour drawing on white background. Use cleaned guide and source photo. "
            "Keep real shell and perspective. Complete hidden roof, cornice, facade and window rhythm. "
            "Bold clean straight lines."
        )
    if variant.startswith("E"):
        return (
            "Match reference style: strict two-color black architectural line art on white background. "
            "Source photo keeps identity. Control keeps structure. Bold clean roofline, corner, cornices, "
            "windows and arches. Complete hidden rhythm conservatively."
        )
    return PROMPT_P1_SIMPLIFICATION


def _resolve_source_image(config: NeuralBranchConfig) -> Path:
    if config.source_image is not None:
        path = Path(config.source_image)
        if not path.exists():
            raise RuntimeError(f"source image does not exist: {path}")
        return path
    candidates = [
        config.artifact_dir.parent.parent / "input" / "image - 2026-06-14T115705.752.png",
        config.artifact_dir.parent.parent / "input" / "sample.png",
    ]
    for path in candidates:
        if path.exists():
            return path
    feature_root = config.artifact_dir
    for parent in config.artifact_dir.parents:
        if parent.name == "countur_svg_generator":
            feature_root = parent
            break
    input_dir = feature_root / "samples" / "input"
    if input_dir.exists():
        images = sorted([*input_dir.glob("*.png"), *input_dir.glob("*.jpg"), *input_dir.glob("*.jpeg"), *input_dir.glob("*.webp")])
        if images:
            return images[0]
    raise RuntimeError("Neural branch requires the original source photo; pass --source-image or include samples/input")


def _fit_pad_rgb(image, *, size: tuple[int, int], fill: tuple[int, int, int]):
    Image = require_module("PIL.Image", "Pillow")
    resampling = getattr(Image, "Resampling", Image)
    image = image.convert("RGB")
    width, height = image.size
    target_w, target_h = size
    scale = min(target_w / max(1, width), target_h / max(1, height))
    new_size = (max(1, int(round(width * scale))), max(1, int(round(height * scale))))
    resized = image.resize(new_size, resampling.LANCZOS)
    out = Image.new("RGB", size, fill)
    out.paste(resized, ((target_w - new_size[0]) // 2, (target_h - new_size[1]) // 2))
    return out


def _source_photo_init(config: NeuralBranchConfig):
    Image = require_module("PIL.Image", "Pillow")
    source = Image.open(_resolve_source_image(config)).convert("RGB")
    return _fit_pad_rgb(source, size=config.output_size, fill=(255, 255, 255))


def _require_artifact(artifact_dir: Path, name: str) -> Path:
    path = artifact_dir / name
    if not path.exists():
        raise RuntimeError(f"Neural branch requires artifact `{name}` in {artifact_dir}")
    return path


def _enabled(config: NeuralBranchConfig, variant: str) -> bool:
    return variant in set(config.variants)


def _black_lines_on_white(image):
    ImageOps = require_module("PIL.ImageOps", "Pillow")
    gray = image.convert("L")
    hist = gray.histogram()
    bright = sum(hist[160:])
    dark = sum(hist[:96])
    out = gray if bright >= dark else ImageOps.invert(gray)
    return out.point(lambda p: int(max(0, min(255, (p - 24) * 1.35))))


def _line_mask_from_control(image):
    ImageOps = require_module("PIL.ImageOps", "Pillow")
    return ImageOps.invert(image.convert("L")).point(lambda p: 255 if p >= 18 else 0)


def _control_from_line_mask(mask):
    ImageOps = require_module("PIL.ImageOps", "Pillow")
    return ImageOps.invert(mask.convert("L").point(lambda p: 255 if p >= 18 else 0))


def _shell_outline(artifact_dir: Path, size: tuple[int, int]):
    Image = require_module("PIL.Image", "Pillow")
    ImageFilter = require_module("PIL.ImageFilter", "Pillow")
    mask_path = _require_artifact(artifact_dir, "mask_object_visible.png")
    mask = Image.open(mask_path).convert("L").resize(size)
    dilated = mask.filter(ImageFilter.MaxFilter(11))
    eroded = mask.filter(ImageFilter.MinFilter(11))
    return _subtract(dilated, eroded).point(lambda p: 255 if p > 24 else 0)


def _wall_plane_outline(artifact_dir: Path, size: tuple[int, int]):
    Image = require_module("PIL.Image", "Pillow")
    ImageFilter = require_module("PIL.ImageFilter", "Pillow")
    path = artifact_dir / "wall_plane.png"
    if not path.exists():
        return Image.new("L", size, 0)
    mask = Image.open(path).convert("L").resize(size)
    return _subtract(mask.filter(ImageFilter.MaxFilter(7)), mask.filter(ImageFilter.MinFilter(7))).point(lambda p: 255 if p > 20 else 0)


def _occluder_outline(artifact_dir: Path, size: tuple[int, int]):
    Image = require_module("PIL.Image", "Pillow")
    ImageFilter = require_module("PIL.ImageFilter", "Pillow")
    path = artifact_dir / "mask_occluder.png"
    if not path.exists():
        return Image.new("L", size, 0)
    mask = Image.open(path).convert("L").resize(size)
    outline = _subtract(mask.filter(ImageFilter.MaxFilter(11)), mask.filter(ImageFilter.MinFilter(11)))
    return outline.point(lambda p: 220 if p > 20 else 0)


def _erase_occluders(edge_mask, artifact_dir: Path):
    Image = require_module("PIL.Image", "Pillow")
    path = artifact_dir / "mask_occluder.png"
    if not path.exists():
        return edge_mask
    mask = Image.open(path).convert("L").resize(edge_mask.size).point(lambda p: 255 if p > 24 else 0)
    black = Image.new("L", edge_mask.size, 0)
    return Image.composite(black, edge_mask, mask)


def _feature_hint_map(artifact_dir: Path, size: tuple[int, int]):
    Image = require_module("PIL.Image", "Pillow")
    ImageDraw = require_module("PIL.ImageDraw", "Pillow")
    out = Image.new("L", size, 0)
    draw = ImageDraw.Draw(out)
    path = artifact_dir / "facade_elements.json"
    if not path.exists():
        overlay = artifact_dir / "elements_overlay.png"
        if overlay.exists():
            return _line_mask_from_control(_black_lines_on_white(Image.open(overlay).convert("L").resize(size))).point(lambda p: 255 if p > 128 else 0)
        return out
    payload = json.loads(path.read_text(encoding="utf-8"))
    sx = size[0] / max(1.0, _source_width(payload, size[0]))
    sy = size[1] / max(1.0, _source_height(payload, size[1]))
    for item in payload:
        kind = str(item.get("element_type") or "")
        if kind not in {"window", "door", "balcony", "pilaster", "cornice", "molding", "sill"}:
            continue
        box = item.get("bbox_xyxy") or []
        if len(box) != 4:
            continue
        x1, y1, x2, y2 = [float(v) for v in box]
        xy = (x1 * sx, y1 * sy, x2 * sx, y2 * sy)
        width = 3 if kind in {"window", "door", "balcony"} else 2
        draw.rectangle(xy, outline=230, width=width)
        if kind == "window" and (y2 - y1) > (x2 - x1) * 1.25:
            cx = (xy[0] + xy[2]) / 2
            draw.arc((xy[0], xy[1], xy[2], xy[1] + (xy[2] - xy[0])), 180, 360, fill=255, width=2)
            draw.line((xy[0], (xy[1] + xy[3]) / 2, xy[0], xy[3]), fill=230, width=2)
            draw.line((xy[2], (xy[1] + xy[3]) / 2, xy[2], xy[3]), fill=230, width=2)
            draw.line((cx, xy[1], cx, xy[3]), fill=180, width=1)
    return out


def _source_width(payload: list[dict[str, Any]], default: int) -> float:
    xs = [float(v) for item in payload for v in (item.get("bbox_xyxy") or [])[0::2]]
    return max(xs) if xs else float(default)


def _source_height(payload: list[dict[str, Any]], default: int) -> float:
    ys = [float(v) for item in payload for v in (item.get("bbox_xyxy") or [])[1::2]]
    return max(ys) if ys else float(default)


def _lighter(a, b):
    ImageChops = require_module("PIL.ImageChops", "Pillow")
    return ImageChops.lighter(a, b)


def _subtract(a, b):
    ImageChops = require_module("PIL.ImageChops", "Pillow")
    return ImageChops.subtract(a, b)


def _controlnet_condition(image):
    return _black_lines_on_white(image).convert("RGB")


def _dark_postcard_init(image):
    ImageOps = require_module("PIL.ImageOps", "Pillow")
    control = _black_lines_on_white(image).convert("L")
    white_lines = ImageOps.invert(control)
    return white_lines.convert("RGB")


def _threshold_line_art(image):
    ImageOps = require_module("PIL.ImageOps", "Pillow")
    gray = _black_lines_on_white(ImageOps.grayscale(image))
    threshold = _adaptive_dark_threshold(gray)
    binary = gray.point(lambda p: 0 if p <= threshold else 255)
    return binary.convert("RGB")


def _adaptive_dark_threshold(gray):
    hist = gray.histogram()
    total = max(1, sum(hist))
    cumulative = 0
    threshold = 96
    for value in range(256):
        cumulative += hist[value]
        if cumulative / total >= 0.12:
            threshold = min(210, max(32, value))
            break
    return threshold


def _overlay_vs_input(init, thresholded):
    Image = require_module("PIL.Image", "Pillow")
    ImageOps = require_module("PIL.ImageOps", "Pillow")
    base = init.convert("L").resize(thresholded.size)
    generated = thresholded.convert("L").point(lambda p: 255 - p)
    out = Image.new("RGB", generated.size, (0, 0, 0))
    out.putdata(
        [
            (int(g), int(max(b, g * 0.4)), int(b))
            for b, g in zip(base.getdata(), generated.getdata())
        ]
    )
    return ImageOps.autocontrast(out)


def _line_art_metrics(image) -> dict[str, float]:
    gray = image.convert("L")
    pixels = list(gray.getdata())
    total = max(1, len(pixels))
    line_pixels = sum(1 for p in pixels if p < 128)
    density = line_pixels / total
    # A quick proxy: useful line art is neither empty nor a dense texture field.
    line_simplicity = max(0.0, 1.0 - abs(density - 0.08) / 0.24)
    structure = max(0.0, min(1.0, density / 0.08)) * max(0.0, min(1.0, (0.32 - density) / 0.24))
    postcardness = math.sqrt(max(0.0, line_simplicity * structure))
    return {
        "line_density": round(density, 5),
        "line_simplicity_proxy": round(line_simplicity, 4),
        "structure_proxy": round(structure, 4),
        "postcardness_proxy": round(postcardness, 4),
        "vectorization_readiness_proxy": round(min(line_simplicity, structure), 4),
    }


def _select_best_candidate(candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not candidates:
        return None
    gated = [item for item in candidates if item.get("accepted_for_fusion")]
    pool = gated or candidates
    return max(
        pool,
        key=lambda item: (
            float(item.get("postcardness_score") or 0.0),
            float(item.get("structure_score") or 0.0),
            -abs(float((item.get("metrics") or {}).get("line_density") or 0.0) - 0.08),
        ),
    )


def _write_contact_sheet(items: list[tuple[str, Path]], path: Path, *, title: str) -> None:
    Image = require_module("PIL.Image", "Pillow")
    ImageDraw = require_module("PIL.ImageDraw", "Pillow")
    path.parent.mkdir(parents=True, exist_ok=True)
    thumb_w, thumb_h = 320, 220
    label_h = 30
    cols = 2 if len(items) <= 4 else 3
    rows = max(1, math.ceil(max(1, len(items)) / cols))
    sheet = Image.new("RGB", (cols * thumb_w, rows * (thumb_h + label_h) + label_h), (18, 20, 24))
    draw = ImageDraw.Draw(sheet)
    draw.text((12, 8), title, fill=(235, 238, 245))
    if not items:
        draw.text((12, 46), "No neural candidates were executed in this run.", fill=(220, 160, 130))
        sheet.save(path)
        return
    for idx, (label, item_path) in enumerate(items):
        row = idx // cols
        col = idx % cols
        x = col * thumb_w
        y = label_h + row * (thumb_h + label_h)
        img = Image.open(item_path).convert("RGB")
        img.thumbnail((thumb_w - 16, thumb_h - 16))
        px = x + (thumb_w - img.width) // 2
        py = y + 8
        sheet.paste(img, (px, py))
        draw.text((x + 10, y + thumb_h + 4), label[:42], fill=(235, 238, 245))
    sheet.save(path)
