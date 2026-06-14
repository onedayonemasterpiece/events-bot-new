from __future__ import annotations

import gc
import json
import math
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from .dependencies import MissingDependencyError, has_cuda, require_module
from .guide_bank import GuideBankConfig, GuideBankResult, build_guide_bank
from .neural_branch import (
    NEGATIVE_IDENTITY_DRIFT,
    _burgundy_preview,
    _controlnet_condition,
    _fit_pad_rgb,
    _line_art_metrics,
    _line_only_gate,
    _threshold_line_art,
)

LINE_ART_PROMPT = (
    "You are given a structural architectural line map, not a photo. Transform it "
    "into a cleaner, bolder, simplified architectural contour drawing. Preserve "
    "the same object identity, silhouette, perspective, dome or roofline, gallery, "
    "arches, windows, major horizontal rings, facade planes and base. Use fewer "
    "and larger confident strokes. Merge redundant close parallel lines. Remove "
    "foliage, trees, background, wall texture, brick texture, shadows and random "
    "edge noise. Output must be strictly black line art on a plain white background. "
    "No color, no shading, no realistic rendering, no filled surfaces, no paper "
    "texture, no sketch hatching."
)

BRANCHES = {
    "E1_lineart_control_only": {
        "model": "lllyasviel/control_v11p_sd15_lineart",
        "control_only": True,
        "source_photo_used_as_init": False,
        "guide_used_as_init": False,
    },
    "E2_lineart_line_init": {
        "model": "lllyasviel/control_v11p_sd15_lineart",
        "control_only": False,
        "source_photo_used_as_init": False,
        "guide_used_as_init": True,
    },
    "E3_scribble_control_only": {
        "model": "lllyasviel/control_v11p_sd15_scribble",
        "control_only": True,
        "source_photo_used_as_init": False,
        "guide_used_as_init": False,
    },
    "E4_scribble_line_init": {
        "model": "lllyasviel/control_v11p_sd15_scribble",
        "control_only": False,
        "source_photo_used_as_init": False,
        "guide_used_as_init": True,
    },
}

DEFAULT_GUIDE_IDS = (
    "G3_edge_thickened",
    "G4_edge_cleaned",
    "CG1_silhouette_plus_structure",
    "CG3_fused_balanced",
    "CG4_minimal_clean",
)

ProgressCallback = Callable[[str, dict[str, Any]], None]


@dataclass(frozen=True)
class LineArtExperimentConfig:
    source_images: tuple[Path, ...]
    out_dir: Path
    style_reference: Path | None = None
    output_size: tuple[int, int] = (768, 768)
    guide_ids: tuple[str, ...] = DEFAULT_GUIDE_IDS
    branches: tuple[str, ...] = tuple(BRANCHES)
    seeds: tuple[int, ...] = (42,)
    steps: int = 22
    guidance_scale: float = 8.0
    control_scale: float = 0.85
    strength: float = 0.60
    base_model: str = "runwayml/stable-diffusion-v1-5"
    max_candidates_per_image: int | None = None


@dataclass
class CandidateResult:
    candidate_id: str
    source_image: str
    guide_id: str
    branch: str
    model: str
    seed: int
    candidate_dir: Path
    input_guide: Path
    candidate_raw: Path
    candidate_line_mask: Path
    candidate_thresholded: Path
    candidate_cleaned: Path
    candidate_burgundy_preview: Path
    candidate_report: Path
    line_only_gate_passed: bool
    rejection_reason: str | None
    metrics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "source_image": self.source_image,
            "guide_id": self.guide_id,
            "branch": self.branch,
            "model": self.model,
            "seed": self.seed,
            "candidate_dir": str(self.candidate_dir),
            "input_guide": str(self.input_guide),
            "candidate_raw": str(self.candidate_raw),
            "candidate_line_mask": str(self.candidate_line_mask),
            "candidate_thresholded": str(self.candidate_thresholded),
            "candidate_cleaned": str(self.candidate_cleaned),
            "candidate_burgundy_preview": str(self.candidate_burgundy_preview),
            "candidate_report": str(self.candidate_report),
            "line_only_gate_passed": self.line_only_gate_passed,
            "rejection_reason": self.rejection_reason,
            "metrics": self.metrics,
        }


def run_line_art_experiment_batch(config: LineArtExperimentConfig, *, progress: ProgressCallback | None = None) -> dict[str, Any]:
    if not has_cuda():
        raise MissingDependencyError("Guide-only line-art experiments require CUDA/GPU")
    if not config.source_images:
        raise RuntimeError("Line-art experiment requires at least one source image")
    unknown_branches = [branch for branch in config.branches if branch not in BRANCHES]
    if unknown_branches:
        raise RuntimeError(f"Unknown line-art branches: {unknown_branches}")

    config.out_dir.mkdir(parents=True, exist_ok=True)
    _emit(progress, "preflight", {"source_count": len(config.source_images), "branches": list(config.branches)})
    all_reports: list[dict[str, Any]] = []
    for index, source in enumerate(config.source_images, start=1):
        source = Path(source)
        if not source.exists():
            raise RuntimeError(f"Source image does not exist: {source}")
        image_dir = config.out_dir / _safe_stem(source)
        _emit(progress, "guide_bank", {"source_index": index, "sources_total": len(config.source_images), "source_image": str(source)})
        guide_bank = build_guide_bank(GuideBankConfig(source_image=source, out_dir=image_dir, output_size=config.output_size))
        report = _run_single_image(config, source, image_dir, guide_bank, progress=progress, source_index=index)
        all_reports.append(report)

    gallery = _write_result_gallery(config.out_dir, all_reports)
    batch_report = {
        "status": "ok",
        "workflow": "guide_only_multi_mask_controlnet_line_art_v0_2",
        "source_photo_used_as_init": False,
        "sources": all_reports,
        "result_gallery": gallery,
        "output_dir": str(config.out_dir),
    }
    reports_dir = config.out_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "line_art_batch_report.json").write_text(json.dumps(batch_report, ensure_ascii=False, indent=2), encoding="utf-8")
    (reports_dir / "line_art_batch_report.md").write_text(_batch_markdown(batch_report), encoding="utf-8")
    return batch_report


def _write_result_gallery(out_dir: Path, reports: list[dict[str, Any]]) -> dict[str, str]:
    Image = require_module("PIL.Image", "Pillow")
    ImageDraw = require_module("PIL.ImageDraw", "Pillow")

    gallery_dir = out_dir / "000_result_gallery"
    gallery_dir.mkdir(parents=True, exist_ok=True)
    rows: list[tuple[str, Path, Path, Path | None]] = []
    for report in reports:
        image_dir = Path(str(report["output_dir"]))
        stem = _safe_stem(Path(str(report["source_image"])))
        edge = image_dir / "edge_mask.png"
        result = image_dir / "result.png"
        preview = image_dir / "result_burgundy_preview.png"
        if edge.exists():
            shutil.copy2(edge, gallery_dir / f"{stem}_edge_mask.png")
        if result.exists():
            shutil.copy2(result, gallery_dir / f"{stem}_result.png")
        if preview.exists():
            shutil.copy2(preview, gallery_dir / f"{stem}_burgundy_preview.png")
        rows.append((stem, edge, result, preview if preview.exists() else None))

    thumb = (260, 260)
    label_h = 34
    cols = [("edge_mask", 1), ("result", 2), ("preview", 3)]
    sheet = Image.new("RGB", (len(cols) * thumb[0], len(rows) * (thumb[1] + label_h) + label_h), (245, 247, 250))
    draw = ImageDraw.Draw(sheet)
    for col, (label, _) in enumerate(cols):
        draw.text((col * thumb[0] + 8, 8), label, fill=(20, 24, 32))
    for row, item in enumerate(rows):
        name = item[0]
        y = label_h + row * (thumb[1] + label_h)
        draw.text((8, y + thumb[1] + 6), name[:42], fill=(20, 24, 32))
        for col, (_, idx) in enumerate(cols):
            path = item[idx]
            x = col * thumb[0]
            if path is None or not path.exists():
                draw.text((x + 8, y + 8), "missing", fill=(180, 40, 40))
                continue
            img = Image.open(path).convert("RGB")
            img.thumbnail((thumb[0] - 12, thumb[1] - 12))
            sheet.paste(img, (x + (thumb[0] - img.width) // 2, y + (thumb[1] - img.height) // 2))
    overview_path = gallery_dir / "line_art_results_overview.png"
    sheet.save(overview_path)
    return {"dir": str(gallery_dir), "overview": str(overview_path)}


def _run_single_image(
    config: LineArtExperimentConfig,
    source: Path,
    image_dir: Path,
    guide_bank: GuideBankResult,
    *,
    progress: ProgressCallback | None,
    source_index: int,
) -> dict[str, Any]:
    Image = require_module("PIL.Image", "Pillow")
    torch = require_module("torch", "torch")
    diffusers = require_module("diffusers", "diffusers")

    guide_map = {**guide_bank.guides, **guide_bank.composite_guides}
    selected_guides: list[tuple[str, Path]] = []
    for guide_id in config.guide_ids:
        if guide_id not in guide_map:
            raise RuntimeError(f"Requested guide_id not built for {source}: {guide_id}")
        selected_guides.append((guide_id, guide_map[guide_id]))

    branch_specs = [BRANCHES[branch] | {"branch": branch} for branch in config.branches]
    candidates: list[CandidateResult] = []
    total_planned = len(selected_guides) * len(branch_specs) * len(config.seeds)
    if config.max_candidates_per_image is not None:
        total_planned = min(total_planned, int(config.max_candidates_per_image))
    planned_count = 0

    # Load one ControlNet pipeline at a time. Keeping control-only and img2img
    # SD1.5 pipelines resident together is fragile on a Kaggle T4 and tends to
    # fail with OOM before the experiment can produce useful audit artifacts.
    for model_id in _ordered_models(branch_specs):
        for spec in [item for item in branch_specs if item["model"] == model_id]:
            branch = str(spec["branch"])
            _emit(
                progress,
                "load_model",
                {"source_index": source_index, "source_image": str(source), "model": model_id, "branch": branch},
            )
            dtype = torch.float16
            controlnet = diffusers.ControlNetModel.from_pretrained(model_id, torch_dtype=dtype)
            pipe = None
            try:
                if spec["control_only"]:
                    pipe = diffusers.StableDiffusionControlNetPipeline.from_pretrained(
                        config.base_model,
                        controlnet=controlnet,
                        torch_dtype=dtype,
                        safety_checker=None,
                        requires_safety_checker=False,
                    ).to("cuda")
                else:
                    pipe = diffusers.StableDiffusionControlNetImg2ImgPipeline.from_pretrained(
                        config.base_model,
                        controlnet=controlnet,
                        torch_dtype=dtype,
                        safety_checker=None,
                        requires_safety_checker=False,
                    ).to("cuda")
                _optimize_pipe(pipe)

                for guide_id, guide_path in selected_guides:
                    control_source = _fit_pad_rgb(Image.open(guide_path).convert("RGB"), size=config.output_size, fill=(255, 255, 255))
                    condition = _controlnet_condition(control_source)
                    for seed in config.seeds:
                        if config.max_candidates_per_image is not None and planned_count >= int(config.max_candidates_per_image):
                            break
                        planned_count += 1
                        _emit(
                            progress,
                            "candidate",
                            {
                                "source_index": source_index,
                                "source_image": str(source),
                                "done": planned_count - 1,
                                "total": total_planned,
                                "guide_id": guide_id,
                                "branch": branch,
                                "seed": seed,
                            },
                        )
                        generator = torch.Generator(device="cuda").manual_seed(int(seed))
                        if spec["control_only"]:
                            result = pipe(
                                prompt=LINE_ART_PROMPT,
                                negative_prompt=NEGATIVE_IDENTITY_DRIFT,
                                image=condition,
                                num_inference_steps=int(config.steps),
                                guidance_scale=float(config.guidance_scale),
                                controlnet_conditioning_scale=float(config.control_scale),
                                generator=generator,
                            ).images[0]
                        else:
                            result = pipe(
                                prompt=LINE_ART_PROMPT,
                                negative_prompt=NEGATIVE_IDENTITY_DRIFT,
                                image=condition,
                                control_image=condition,
                                num_inference_steps=int(config.steps),
                                guidance_scale=float(config.guidance_scale),
                                controlnet_conditioning_scale=float(config.control_scale),
                                strength=float(config.strength),
                                generator=generator,
                            ).images[0]
                        candidates.append(
                            _write_candidate(
                                image_dir=image_dir,
                                source=source,
                                guide_id=guide_id,
                                branch=branch,
                                model=model_id,
                                seed=int(seed),
                                input_guide=guide_path,
                                raw=result,
                                source_artifact_dir=image_dir,
                                source_photo_used_as_init=bool(spec["source_photo_used_as_init"]),
                                guide_used_as_init=bool(spec["guide_used_as_init"]),
                                control_only=bool(spec["control_only"]),
                            )
                        )
                    if config.max_candidates_per_image is not None and planned_count >= int(config.max_candidates_per_image):
                        break
            finally:
                del pipe
                del controlnet
                gc.collect()
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
            if config.max_candidates_per_image is not None and planned_count >= int(config.max_candidates_per_image):
                break
        if config.max_candidates_per_image is not None and planned_count >= int(config.max_candidates_per_image):
            break

    if not candidates:
        raise RuntimeError(f"No candidates produced for {source}")
    _write_contact_sheets(image_dir, candidates)
    best = _select_best(candidates)
    if best is not None:
        shutil.copy2(best.candidate_line_mask, image_dir / "result.png")
        shutil.copy2(best.candidate_burgundy_preview, image_dir / "result_burgundy_preview.png")
        shutil.copy2(best.candidate_raw, image_dir / "result_raw.png")

    report = {
        "source_image": str(source),
        "output_dir": str(image_dir),
        "guide_bank": guide_bank.to_dict(),
        "guide_ids_run": [guide_id for guide_id, _ in selected_guides],
        "branches_run": list(config.branches),
        "candidate_count": len(candidates),
        "passed_count": sum(1 for candidate in candidates if candidate.line_only_gate_passed),
        "best_candidate_id": best.candidate_id if best else None,
        "result_png": str(image_dir / "result.png") if best else None,
        "candidates": [candidate.to_dict() for candidate in candidates],
    }
    reports_dir = image_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "line_art_experiment_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    (reports_dir / "line_art_experiment_report.md").write_text(_single_markdown(report), encoding="utf-8")
    _emit(progress, "image_done", {"source_index": source_index, "source_image": str(source), "candidate_count": len(candidates), "best_candidate_id": report["best_candidate_id"]})
    return report


def _write_candidate(
    *,
    image_dir: Path,
    source: Path,
    guide_id: str,
    branch: str,
    model: str,
    seed: int,
    input_guide: Path,
    raw,
    source_artifact_dir: Path,
    source_photo_used_as_init: bool,
    guide_used_as_init: bool,
    control_only: bool,
) -> CandidateResult:
    candidate_id = f"{guide_id}__{branch}__seed{seed}"
    candidate_dir = image_dir / "candidates" / candidate_id
    candidate_dir.mkdir(parents=True, exist_ok=True)
    input_path = candidate_dir / "input_guide.png"
    shutil.copy2(input_guide, input_path)
    raw_path = candidate_dir / "candidate_raw.png"
    raw.save(raw_path)
    line_mask = _threshold_line_art(raw)
    line_mask_path = candidate_dir / "candidate_line_mask.png"
    line_mask.save(line_mask_path)
    thresholded_path = candidate_dir / "candidate_thresholded.png"
    shutil.copy2(line_mask_path, thresholded_path)
    cleaned = _clean_line_mask(line_mask)
    cleaned_path = candidate_dir / "candidate_cleaned.png"
    cleaned.save(cleaned_path)
    burgundy_path = candidate_dir / "candidate_burgundy_preview.png"
    _burgundy_preview(cleaned).save(burgundy_path)

    metrics = _line_art_metrics(cleaned)
    metrics.update(_component_metrics(cleaned))
    gate = _line_only_gate(raw, cleaned, metrics, source_artifact_dir)
    report_payload = {
        "candidate_id": candidate_id,
        "guide_id": guide_id,
        "branch": branch,
        "model": model,
        "seed": seed,
        "source_image": str(source),
        "source_photo_used_as_init": source_photo_used_as_init,
        "guide_used_as_init": guide_used_as_init,
        "control_only": control_only,
        "candidate_raw": str(raw_path),
        "candidate_line_mask": str(line_mask_path),
        "candidate_thresholded": str(thresholded_path),
        "candidate_cleaned": str(cleaned_path),
        "candidate_burgundy_preview": str(burgundy_path),
        **metrics,
        **gate,
    }
    report_path = candidate_dir / "candidate_report.json"
    report_path.write_text(json.dumps(report_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return CandidateResult(
        candidate_id=candidate_id,
        source_image=str(source),
        guide_id=guide_id,
        branch=branch,
        model=model,
        seed=seed,
        candidate_dir=candidate_dir,
        input_guide=input_path,
        candidate_raw=raw_path,
        candidate_line_mask=line_mask_path,
        candidate_thresholded=thresholded_path,
        candidate_cleaned=cleaned_path,
        candidate_burgundy_preview=burgundy_path,
        candidate_report=report_path,
        line_only_gate_passed=bool(gate["line_only_gate_passed"]),
        rejection_reason=gate["rejection_reason"],
        metrics={**metrics, **gate},
    )


def _clean_line_mask(image):
    Image = require_module("PIL.Image", "Pillow")
    np = require_module("numpy", "numpy")
    cv2 = require_module("cv2", "opencv-python-headless")
    gray = image.convert("L")
    arr = np.array(gray)
    line = np.where(arr < 128, 255, 0).astype("uint8")
    labels_count, labels, stats, _ = cv2.connectedComponentsWithStats(line, connectivity=8)
    out = np.zeros_like(line)
    for label in range(1, labels_count):
        x, y, w, h, area = stats[label]
        if area >= 10 and max(w, h) >= 3 and area / max(1, w * h) <= 0.82:
            out[labels == label] = 255
    control = np.where(out > 0, 0, 255).astype("uint8")
    return Image.fromarray(control).convert("RGB")


def _component_metrics(image) -> dict[str, Any]:
    np = require_module("numpy", "numpy")
    cv2 = require_module("cv2", "opencv-python-headless")
    arr = np.array(image.convert("L"))
    line = np.where(arr < 128, 255, 0).astype("uint8")
    labels_count, labels, stats, _ = cv2.connectedComponentsWithStats(line, connectivity=8)
    areas = [int(stats[label, cv2.CC_STAT_AREA]) for label in range(1, labels_count)]
    small = sum(1 for area in areas if area < 16)
    large = max(areas) if areas else 0
    total_line = max(1, sum(areas))
    return {
        "small_component_count": small,
        "small_component_ratio": round(small / max(1, len(areas)), 5),
        "large_filled_region_ratio": round(large / total_line, 5),
    }


def _write_contact_sheets(image_dir: Path, candidates: list[CandidateResult]) -> None:
    contact_dir = image_dir / "contact_sheets"
    contact_dir.mkdir(parents=True, exist_ok=True)
    _write_candidate_sheet(candidates, contact_dir / "contact_sheet_all.png", title="All line-art candidates")
    passed = [candidate for candidate in candidates if candidate.line_only_gate_passed]
    rejected = [candidate for candidate in candidates if not candidate.line_only_gate_passed]
    _write_candidate_sheet(passed, contact_dir / "contact_sheet_passed.png", title="Passed line-art candidates")
    _write_candidate_sheet(rejected, contact_dir / "contact_sheet_rejected.png", title="Rejected line-art candidates")


def _write_candidate_sheet(candidates: list[CandidateResult], path: Path, *, title: str) -> None:
    Image = require_module("PIL.Image", "Pillow")
    ImageDraw = require_module("PIL.ImageDraw", "Pillow")
    thumb_w, thumb_h = 180, 140
    label_h = 38
    columns = [("guide", "input_guide"), ("raw", "candidate_raw"), ("line", "candidate_line_mask"), ("clean", "candidate_cleaned"), ("preview", "candidate_burgundy_preview")]
    rows = max(1, len(candidates))
    width = len(columns) * thumb_w
    height = 34 + rows * (thumb_h + label_h)
    sheet = Image.new("RGB", (width, height), (245, 247, 250))
    draw = ImageDraw.Draw(sheet)
    draw.text((10, 8), title, fill=(20, 24, 32))
    if not candidates:
        draw.text((10, 46), "No candidates", fill=(120, 40, 40))
        sheet.save(path)
        return
    for row, candidate in enumerate(candidates):
        y = 34 + row * (thumb_h + label_h)
        for col, (header, attr) in enumerate(columns):
            x = col * thumb_w
            img = Image.open(getattr(candidate, attr)).convert("RGB")
            img.thumbnail((thumb_w - 12, thumb_h - 12))
            sheet.paste(img, (x + (thumb_w - img.width) // 2, y + 6))
            draw.text((x + 6, y + thumb_h + 2), header, fill=(44, 52, 68))
        label = f"{candidate.guide_id} + {candidate.branch} seed{candidate.seed}"
        if not candidate.line_only_gate_passed:
            label += f" REJECT {candidate.rejection_reason or ''}"
        draw.text((6, y + thumb_h + 18), label[:150], fill=(20, 24, 32))
    sheet.save(path)


def _select_best(candidates: list[CandidateResult]) -> CandidateResult | None:
    pool = [candidate for candidate in candidates if candidate.line_only_gate_passed]
    if not pool:
        pool = candidates
    return max(
        pool,
        key=lambda candidate: (
            float(candidate.metrics.get("postcardness_proxy") or 0.0),
            float(candidate.metrics.get("structure_proxy") or 0.0),
            -abs(float(candidate.metrics.get("line_density") or 0.0) - 0.08),
        ),
    ) if pool else None


def _ordered_models(branch_specs: list[dict[str, Any]]) -> list[str]:
    out: list[str] = []
    for spec in branch_specs:
        model = str(spec["model"])
        if model not in out:
            out.append(model)
    return out


def _optimize_pipe(pipe) -> None:
    try:
        pipe.enable_xformers_memory_efficient_attention()
    except Exception:
        pass
    try:
        pipe.enable_attention_slicing()
    except Exception:
        pass


def _safe_stem(path: Path) -> str:
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in path.stem).strip("_")[:80] or "image"


def _emit(progress: ProgressCallback | None, phase: str, payload: dict[str, Any]) -> None:
    if progress is not None:
        progress(phase, payload)


def _single_markdown(report: dict[str, Any]) -> str:
    lines = [f"# Line-art experiment: `{Path(report['source_image']).name}`", ""]
    lines.append(f"Output: `{report['output_dir']}`")
    lines.append(f"Candidates: {report['candidate_count']} / passed: {report['passed_count']}")
    lines.append(f"Best: `{report.get('best_candidate_id')}`")
    lines.append("")
    lines.append("## Guides run")
    for guide in report.get("guide_ids_run") or []:
        lines.append(f"- {guide}")
    lines.append("")
    lines.append("## Branches run")
    for branch in report.get("branches_run") or []:
        lines.append(f"- {branch}")
    lines.append("")
    lines.append("## Candidates")
    for candidate in report.get("candidates") or []:
        status = "PASS" if candidate.get("line_only_gate_passed") else f"REJECT: {candidate.get('rejection_reason')}"
        lines.append(f"- `{candidate['candidate_id']}` — {status}")
    return "\n".join(lines) + "\n"


def _batch_markdown(report: dict[str, Any]) -> str:
    lines = ["# Line-art batch report", "", f"Output: `{report['output_dir']}`", ""]
    for source in report.get("sources") or []:
        lines.append(f"- `{Path(source['source_image']).name}`: {source['candidate_count']} candidates, best `{source.get('best_candidate_id')}`")
    return "\n".join(lines) + "\n"
