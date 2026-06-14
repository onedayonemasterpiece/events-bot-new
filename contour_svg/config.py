from __future__ import annotations

import copy
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import yaml


def _deep_update(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_update(out[key], value)
        else:
            out[key] = value
    return out


@dataclass
class RuntimeConfig:
    device: str = "auto"
    dtype: str = "float16"
    seed: int = 42
    internet_allowed: bool = True
    hf_cache_dir: str = "/kaggle/working/hf_cache"
    fail_on_neural_unavailable: bool = True


@dataclass
class GeminiConfig:
    enabled: bool = True
    model: str = "gemini-2.5-flash-lite"
    api_key_env: str = "GOOGLE_API_KEY"
    max_retries: int = 2


@dataclass
class InputConfig:
    image_path: str = ""
    object_prompt: str = "main building"
    bbox_hint_xyxy: list[int] | None = None
    style_reference_path: str | None = None
    max_side: int = 1536


@dataclass
class StyleConfig:
    preset: str = "geometric_lineart"
    background: str = "transparent"
    stroke_color: str = "#FFFFFF"
    stroke_width: float = 5.0
    stroke_style: str = "clean"
    viewbox: list[int] = field(default_factory=lambda: [1024, 1024])
    max_paths: int = 240
    min_line_length: float = 16.0


@dataclass
class SegmentationConfig:
    backend: str = "groundingdino_sam2"
    grounding_model: str = "IDEA-Research/grounding-dino-tiny"
    use_florence: bool = True
    florence_model: str = "florence-community/Florence-2-base"
    use_yoloworld: bool = True
    yoloworld_model: str = "yolov8s-worldv2.pt"
    box_threshold: float = 0.28
    text_threshold: float = 0.25
    sam2_checkpoint: str | None = None
    sam2_model_cfg: str = "configs/sam2.1/sam2.1_hiera_t.yaml"
    architecture_priority: bool = True
    primary_prompts: list[str] = field(
        default_factory=lambda: ["building", "architecture", "facade", "monument", "tower", "main object"]
    )
    occluder_prompts: list[str] = field(
        default_factory=lambda: ["tree", "foliage", "leaves", "branch", "fence", "wire", "pole", "sky", "road"]
    )


@dataclass
class GeometryConfig:
    use_canny: bool = True
    use_lsd: bool = True
    use_hough: bool = True
    use_mlsd: bool = True
    use_deeplsd: bool = True
    deeplsd_checkpoint: str | None = None
    deeplsd_repo_dir: str | None = None
    use_hawp: bool = True
    hawp_checkpoint: str | None = None
    hawp_repo_dir: str | None = None
    snap_architecture_angles: bool = True
    merge_angle_deg: float = 6.0
    max_bridge_gap_px: int = 80
    simplify_tolerance: float = 2.0
    max_paths_per_candidate: int = 280


@dataclass
class DiffusionConfig:
    enabled: bool = True
    backend: str = "sd15_controlnet_img2img"
    base_model: str = "runwayml/stable-diffusion-v1-5"
    lineart_controlnet: str = "lllyasviel/control_v11p_sd15_lineart"
    mlsd_controlnet: str = "lllyasviel/control_v11p_sd15_mlsd"
    depth_controlnet: str = "lllyasviel/control_v11f1p_sd15_depth"
    depth_model: str = "depth-anything/Depth-Anything-V2-Small-hf"
    steps: int = 30
    guidance_scale: float = 5.0
    control_scale: float = 1.15
    depth_control_scale: float = 0.80
    strength: float = 0.40
    control_guidance_end: float = 0.85
    style_reference_adapter_enabled: bool = True
    style_reference_adapter_model: str = "h94/IP-Adapter"
    style_reference_adapter_subfolder: str = "models"
    style_reference_adapter_weight_name: str = "ip-adapter_sd15.bin"
    style_reference_adapter_scale: float = 0.35
    style_reference_strength: float = 0.34
    seeds: list[int] = field(default_factory=lambda: [42, 43, 44, 45])
    output_size: list[int] = field(default_factory=lambda: [768, 768])


@dataclass
class CandidateSearchConfig:
    variants: list[str] = field(default_factory=lambda: ["B1", "B2", "B3", "B4", "E1"])
    final_top_k: int = 3
    cv_shortlist_size: int = 8
    allow_gemini_review: bool = True
    allow_gemini_line_review: bool = True
    max_gemini_line_groups: int = 75
    final_min_structure_score: float = 3.0


@dataclass
class OutputConfig:
    output_dir: str = "/kaggle/working/contour_svg_run"
    keep_debug: bool = True


@dataclass
class RunConfig:
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)
    gemini: GeminiConfig = field(default_factory=GeminiConfig)
    input: InputConfig = field(default_factory=InputConfig)
    style: StyleConfig = field(default_factory=StyleConfig)
    segmentation: SegmentationConfig = field(default_factory=SegmentationConfig)
    geometry: GeometryConfig = field(default_factory=GeometryConfig)
    diffusion: DiffusionConfig = field(default_factory=DiffusionConfig)
    candidate_search: CandidateSearchConfig = field(default_factory=CandidateSearchConfig)
    output: OutputConfig = field(default_factory=OutputConfig)

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> "RunConfig":
        defaults = asdict(cls())
        merged = _deep_update(defaults, data or {})
        return cls(
            runtime=RuntimeConfig(**merged.get("runtime", {})),
            gemini=GeminiConfig(**merged.get("gemini", {})),
            input=InputConfig(**merged.get("input", {})),
            style=StyleConfig(**merged.get("style", {})),
            segmentation=SegmentationConfig(**merged.get("segmentation", {})),
            geometry=GeometryConfig(**merged.get("geometry", {})),
            diffusion=DiffusionConfig(**merged.get("diffusion", {})),
            candidate_search=CandidateSearchConfig(**merged.get("candidate_search", {})),
            output=OutputConfig(**merged.get("output", {})),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def resolved_output_dir(self) -> Path:
        return Path(os.path.expandvars(self.output.output_dir)).expanduser()

    def resolved_input_path(self) -> Path:
        return Path(os.path.expandvars(self.input.image_path)).expanduser()


def load_config(path: str | Path | None = None, overrides: dict[str, Any] | None = None) -> RunConfig:
    data: dict[str, Any] = {}
    if path:
        with Path(path).open("r", encoding="utf-8") as fh:
            loaded = yaml.safe_load(fh) or {}
        if not isinstance(loaded, dict):
            raise ValueError(f"Config at {path} must be a mapping")
        data = loaded
    if overrides:
        data = _deep_update(data, overrides)
    return RunConfig.from_mapping(data)
