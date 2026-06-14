from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import yaml

from .building_shell import build_building_shell, building_shell_to_candidate
from .config import RunConfig
from .contact_sheet import make_contact_sheet
from .contracts import Candidate
from .detection_grounding import choose_primary_bbox, detect_open_vocabulary_boxes
from .diffusion_controlnet import generate_controlnet_rasters
from .completion import build_completion_proposals
from .evidence_inventory import build_evidence_inventory
from .facade_parser import parse_facade_elements
from .feature_graph import build_feature_graph, feature_graph_to_candidate
from .gemini_judge import judge_candidates_with_gemini
from .gemini_line_editor import review_line_groups_with_gemini
from .guides import build_guides
from .image_io import normalize_image
from .line_graph import apply_line_group_actions, build_line_graph
from .masks import build_mask_bundle, combine_masks
from .plane_graph import build_plane_graph, plane_graph_to_candidate
from .primitive_renderer import primitive_candidates_from_groups
from .ranking import rank_candidates
from .report import export_final, write_candidate_meta, write_leaderboard
from .scoring import score_candidate
from .segment_sam2 import Sam2Segmenter
from .semantic_gemini import analyze_with_gemini
from .status import ContourStatus
from .svg_export import render_preview, write_svg
from .vectorize import guide_lines_to_candidate, raster_to_candidate


STEP_TOTAL = 19


@dataclass
class RunResult:
    output_dir: Path
    final_svg: Path | None
    preview_png: Path | None
    candidates: list[Candidate]
    warnings: list[str]


class ContourGenerator:
    def __init__(self, config: RunConfig, *, status: ContourStatus | None = None):
        self.config = config
        self.status = status or ContourStatus()

    def run(self) -> RunResult:
        config = self.config
        unsupported = sorted(set(config.candidate_search.variants) - {"B1", "B2", "B3", "B4", "E1"})
        if unsupported:
            raise RuntimeError(f"Unsupported contour_svg variants for the neural-only pipeline: {unsupported}")
        if "B1" not in config.candidate_search.variants:
            raise RuntimeError("B1 ControlNet line-art candidate generation is required")
        if "B2" not in config.candidate_search.variants:
            raise RuntimeError("B2 neural mask structural-guide candidate generation is required")
        if "E1" not in config.candidate_search.variants:
            raise RuntimeError("E1 Gemini/CV ranking is required")
        out_dir = config.resolved_output_dir()
        debug_dir = out_dir / "debug"
        candidates_dir = out_dir / "candidates"
        out_dir.mkdir(parents=True, exist_ok=True)
        debug_dir.mkdir(parents=True, exist_ok=True)
        candidates_dir.mkdir(parents=True, exist_ok=True)
        warnings: list[str] = []

        self.status.stage("input", step_index=1, step_total=STEP_TOTAL, label="input normalize")
        image, normalized_path = normalize_image(
            config.resolved_input_path(),
            debug_dir / "input_normalized.png",
            max_side=config.input.max_side,
        )
        (out_dir / "config.resolved.yaml").write_text(
            yaml.safe_dump(config.to_dict(), allow_unicode=True, sort_keys=False),
            encoding="utf-8",
        )
        self.status.stage(
            "input",
            step_index=1,
            step_total=STEP_TOTAL,
            label="input normalized",
            status="done",
            image_width=image.width,
            image_height=image.height,
            output_dir=str(out_dir),
        )

        self.status.stage("semantic_plan", step_index=2, step_total=STEP_TOTAL, label="Gemini semantic plan")
        semantic_plan, semantic_warnings = analyze_with_gemini(normalized_path, config)
        warnings.extend(semantic_warnings)
        (debug_dir / "semantic_plan.json").write_text(
            json.dumps(semantic_plan.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        self.status.stage(
            "semantic_plan",
            step_index=2,
            step_total=STEP_TOTAL,
            label="Gemini semantic plan done",
            status="done",
            occluders=len(semantic_plan.occluders),
            features=len(semantic_plan.style_relevant_features),
        )

        self.status.stage(
            "groundingdino_primary",
            step_index=3,
            step_total=STEP_TOTAL,
            label="GroundingDINO primary object",
        )
        primary_bbox, bbox_warnings = choose_primary_bbox(image, config, semantic_plan, debug_dir=debug_dir)
        warnings.extend(bbox_warnings)
        self.status.stage(
            "groundingdino_primary",
            step_index=3,
            step_total=STEP_TOTAL,
            label="GroundingDINO primary object done",
            status="done",
            primary_label=primary_bbox.label,
            primary_score=round(primary_bbox.score, 4),
        )

        self.status.stage("sam2_primary", step_index=4, step_total=STEP_TOTAL, label="SAM2 primary mask")
        segmenter = Sam2Segmenter(config)
        primary_artifact = segmenter.segment_bbox(image, primary_bbox, debug_dir / "primary_object_mask.png")
        warnings.extend(primary_artifact.warnings)
        self.status.stage(
            "sam2_primary",
            step_index=4,
            step_total=STEP_TOTAL,
            label="SAM2 primary mask done",
            status="done",
            mask_path=str(primary_artifact.path),
        )

        occluder_masks = []
        self.status.stage(
            "groundingdino_occluders",
            step_index=5,
            step_total=STEP_TOTAL,
            label="open-vocabulary occluder detection",
            expected_occluders=len(semantic_plan.occluders),
        )
        occluder_boxes, occ_warnings = detect_open_vocabulary_boxes(
            image,
            config,
            config.segmentation.occluder_prompts,
            debug_dir=debug_dir,
            artifact_prefix="occluder_detector",
        )
        warnings.extend(occ_warnings)
        if semantic_plan.occluders and not occluder_boxes:
            raise RuntimeError(
                "Gemini semantic plan identified occluders, but open-vocabulary detectors returned no occluder boxes"
            )
        self.status.stage(
            "groundingdino_occluders",
            step_index=5,
            step_total=STEP_TOTAL,
            label="open-vocabulary occluder detection done",
            status="done",
            detected_occluders=len(occluder_boxes),
        )
        self.status.stage(
            "sam2_occluders",
            step_index=6,
            step_total=STEP_TOTAL,
            label="SAM2 occluder masks",
            total_occluders=min(len(occluder_boxes), 8),
        )
        for idx, box in enumerate(occluder_boxes[:8]):
            artifact = segmenter.segment_bbox(image, box, debug_dir / f"occluder_mask_{idx:02d}.png")
            warnings.extend(artifact.warnings)
            from PIL import Image

            occluder_masks.append(Image.open(artifact.path).convert("L"))
            self.status.stage(
                "sam2_occluders",
                step_index=6,
                step_total=STEP_TOTAL,
                label=f"SAM2 occluder masks {idx + 1}/{min(len(occluder_boxes), 8)}",
                occluders_done=idx + 1,
                total_occluders=min(len(occluder_boxes), 8),
            )
        if occluder_masks:
            occluder_mask = combine_masks(occluder_masks, image.size)
            occluder_path = debug_dir / "occluder_mask.png"
            occluder_mask.save(occluder_path)
        else:
            occluder_path = None
        self.status.stage(
            "sam2_occluders",
            step_index=6,
            step_total=STEP_TOTAL,
            label="SAM2 occluder masks done",
            status="done",
            occluder_masks=len(occluder_masks),
        )

        self.status.stage("multi_state_masks", step_index=7, step_total=STEP_TOTAL, label="multi-state mask bundle")
        mask_bundle = build_mask_bundle(
            primary_mask_path=primary_artifact.path,
            occluder_mask_path=occluder_path,
            out_dir=out_dir,
        )
        warnings.extend(mask_bundle.warnings)
        (debug_dir / "mask_bundle.json").write_text(
            json.dumps(mask_bundle.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        self.status.stage(
            "multi_state_masks",
            step_index=7,
            step_total=STEP_TOTAL,
            label="multi-state mask bundle done",
            status="done",
            mask_overlay=str(mask_bundle.overlay),
        )

        self.status.stage("facade_parser", step_index=8, step_total=STEP_TOTAL, label="CMP facade semantic parser")
        facade_elements, facade_warnings = parse_facade_elements(image, mask_bundle, out_dir, config)
        warnings.extend(facade_warnings)
        self.status.stage(
            "facade_parser",
            step_index=8,
            step_total=STEP_TOTAL,
            label="CMP facade semantic parser done",
            status="done",
            facade_elements=len(facade_elements),
        )

        self.status.stage("guides", step_index=9, step_total=STEP_TOTAL, label="structural guide extraction")
        guides = build_guides(image, primary_artifact.path, mask_bundle.occluder, out_dir, config)
        warnings.extend(guides.warnings)
        self.status.stage(
            "guides",
            step_index=9,
            step_total=STEP_TOTAL,
            label="structural guide extraction done",
            status="done",
            lines=len(guides.lines),
            edge_map=str(guides.edge_map) if guides.edge_map else None,
        )

        self.status.stage("evidence_inventory", step_index=10, step_total=STEP_TOTAL, label="typed evidence inventory")
        evidence_inventory = build_evidence_inventory(
            image=image,
            masks=mask_bundle,
            guides=guides,
            facade_elements=facade_elements,
            semantic_plan=semantic_plan,
            out_dir=out_dir,
        )
        self.status.stage(
            "evidence_inventory",
            step_index=10,
            step_total=STEP_TOTAL,
            label="typed evidence inventory done",
            status="done",
            evidence_items=len(evidence_inventory.items),
            evidence_contact_sheet=str(evidence_inventory.contact_sheet) if evidence_inventory.contact_sheet else None,
        )

        self.status.stage("building_shell", step_index=11, step_total=STEP_TOTAL, label="BuildingShell coarse object graph")
        building_shell = build_building_shell(
            image=image,
            masks=mask_bundle,
            guides=guides,
            facade_elements=facade_elements,
            evidence=evidence_inventory,
            out_dir=out_dir,
            config=config,
        )
        self.status.stage(
            "building_shell",
            step_index=11,
            step_total=STEP_TOTAL,
            label="BuildingShell coarse object graph done",
            status="done",
            shell_confidence=building_shell.shell_confidence,
            roof_segments=len(building_shell.roof_segments),
            base_segments=len(building_shell.base_segments),
            facade_corner_segments=len(building_shell.facade_corner_segments),
        )

        self.status.stage("plane_graph", step_index=12, step_total=STEP_TOTAL, label="PlaneGraph perspective scaffold")
        plane_graph = build_plane_graph(
            image=image,
            masks=mask_bundle,
            guides=guides,
            facade_elements=facade_elements,
            evidence=evidence_inventory,
            shell=building_shell,
            out_dir=out_dir,
            config=config,
        )
        self.status.stage(
            "plane_graph",
            step_index=12,
            step_total=STEP_TOTAL,
            label="PlaneGraph perspective scaffold done",
            status="done",
            plane_graph_confidence=plane_graph.graph_confidence,
            facade_planes=len(plane_graph.planes),
            plane_bands=len(plane_graph.bands),
            vertical_edges=len(plane_graph.vertical_edges),
        )

        self.status.stage("feature_graph", step_index=13, step_total=STEP_TOTAL, label="FeatureGraph facade elements")
        feature_graph = build_feature_graph(
            image=image,
            masks=mask_bundle,
            facade_elements=facade_elements,
            shell=building_shell,
            plane_graph=plane_graph,
            out_dir=out_dir,
            config=config,
        )
        self.status.stage(
            "feature_graph",
            step_index=13,
            step_total=STEP_TOTAL,
            label="FeatureGraph facade elements done",
            status="done",
            feature_graph_confidence=feature_graph.graph_confidence,
            feature_count=len(feature_graph.features),
            feature_rows=len(feature_graph.rows),
        )

        self.status.stage("line_graph", step_index=14, step_total=STEP_TOTAL, label="line graph and Gemini pruning")
        line_candidates, line_groups, line_overlay = build_line_graph(guides, mask_bundle, out_dir, config)
        actions = review_line_groups_with_gemini(
            line_groups,
            original_image=normalized_path,
            overlay_image=line_overlay,
            semantic_plan=semantic_plan,
            masks=mask_bundle,
            config=config,
            out_path=debug_dir / "gemini_line_group_actions.json",
        )
        line_groups = apply_line_group_actions(line_groups, actions)
        (debug_dir / "line_groups.pruned.json").write_text(
            json.dumps([group.to_dict() for group in line_groups], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        self.status.stage(
            "line_graph",
            step_index=14,
            step_total=STEP_TOTAL,
            label="line graph and Gemini pruning done",
            status="done",
            line_candidates=len(line_candidates),
            line_groups=len(line_groups),
        )

        self.status.stage("completion", step_index=15, step_total=STEP_TOTAL, label="conservative architectural completion")
        completion_proposals, completion_warnings = build_completion_proposals(
            line_groups,
            facade_elements,
            mask_bundle,
            out_dir,
        )
        warnings.extend(completion_warnings)
        self.status.stage(
            "completion",
            step_index=15,
            step_total=STEP_TOTAL,
            label="conservative architectural completion done",
            status="done",
            completion_proposals=len(completion_proposals),
        )

        candidates: list[Candidate] = [
            building_shell_to_candidate(building_shell, config),
            plane_graph_to_candidate(building_shell, plane_graph, config),
            feature_graph_to_candidate(building_shell, plane_graph, feature_graph, config),
        ]
        self.status.stage("controlnet", step_index=16, step_total=STEP_TOTAL, label="ControlNet proposal candidates")
        rasters, diffusion_warnings = generate_controlnet_rasters(image, guides, mask_bundle, out_dir, config)
        warnings.extend(diffusion_warnings)
        self.status.stage(
            "controlnet",
            step_index=16,
            step_total=STEP_TOTAL,
            label="ControlNet proposal candidates done",
            status="done",
            raster_candidates=len(rasters),
        )
        self.status.stage(
            "primitive_renderer",
            step_index=17,
            step_total=STEP_TOTAL,
            label="primitive renderer and proposal vectorization",
            raster_candidates=len(rasters),
        )
        primitive_candidates = primitive_candidates_from_groups(
            line_groups,
            masks=mask_bundle,
            semantic_plan=semantic_plan,
            out_dir=out_dir,
            config=config,
            facade_elements=facade_elements,
            completion_proposals=completion_proposals,
        )
        candidates.extend(primitive_candidates)
        self.status.stage(
            "primitive_renderer",
            step_index=17,
            step_total=STEP_TOTAL,
            label="primitive renderer final candidates",
            primitive_candidates=len(primitive_candidates),
        )
        for idx, (label, raster) in enumerate(rasters):
            variant = _variant_from_raster_label(label)
            candidates.append(
                raster_to_candidate(
                    raster,
                    candidate_id=f"{label}_stroke_{idx:02d}",
                    variant=variant,
                    family=_family_from_raster_variant(variant),
                    config=config,
                )
            )
            self.status.stage(
                "primitive_renderer",
                step_index=17,
                step_total=STEP_TOTAL,
                label=f"proposal vectorization {idx + 1}/{len(rasters)}",
                vectorized_candidates=idx + 1,
                raster_candidates=len(rasters),
            )
        candidates.append(
            guide_lines_to_candidate(
                guides.lines,
                candidate_id=f"B2_structural_primitives_{len(candidates):02d}",
                variant="B2",
                family="NEURAL_MASK_STRUCTURAL_PRIMITIVES",
                config=config,
            )
        )
        self.status.stage(
            "primitive_renderer",
            step_index=17,
            step_total=STEP_TOTAL,
            label="B2 structural guide proposal vectorization",
            vectorized_candidates=len(candidates),
            guide_primitives=len(guides.lines),
        )
        structural_guides = [path for path in [guides.mlsd_guide, guides.edge_map] if path is not None]
        if not structural_guides:
            raise RuntimeError("B2 requires an occluder-subtracted structural guide")
        for guide_idx, guide_path in enumerate(structural_guides):
            candidates.append(
                raster_to_candidate(
                    guide_path,
                    candidate_id=f"B2_guide{guide_idx}_stroke_{len(candidates):02d}",
                    variant="B2",
                    family="NEURAL_MASK_GUIDE_STROKE",
                    config=config,
                )
            )
            self.status.stage(
                "primitive_renderer",
                step_index=17,
                step_total=STEP_TOTAL,
                label=f"B2 structural-guide vectorization {guide_idx + 1}/{len(structural_guides)}",
                vectorized_candidates=len(candidates),
                structural_guides=len(structural_guides),
            )
        if not candidates:
            raise RuntimeError("ControlNet/vectorization produced no SVG candidates")

        source_size = image.size
        for candidate in candidates:
            svg_path = candidates_dir / f"{candidate.candidate_id}.svg"
            write_svg(candidate, config, svg_path, source_size=source_size)
            preview = render_preview(svg_path, candidates_dir / f"{candidate.candidate_id}_preview.png")
            if preview is None:
                raise RuntimeError("CairoSVG preview rendering is required")
            candidate.preview_path = preview
            score_candidate(candidate, expected_stroke=config.style.stroke_color)
            if candidate.final_eligible and not candidate.accepted:
                raise RuntimeError(f"Candidate failed SVG hard gates: {candidate.candidate_id} flags={candidate.failure_flags}")
            write_candidate_meta(candidate)
        self.status.stage(
            "primitive_renderer",
            step_index=17,
            step_total=STEP_TOTAL,
            label="primitive renderer and proposal vectorization done",
            status="done",
            svg_candidates=len(candidates),
            final_eligible_candidates=sum(1 for c in candidates if c.final_eligible and c.accepted),
        )

        self.status.stage("ranking", step_index=18, step_total=STEP_TOTAL, label="CV shortlist and Gemini ranking")
        all_ranked = rank_candidates(candidates)
        final_pool = [candidate for candidate in all_ranked if candidate.accepted and candidate.final_eligible]
        if not final_pool:
            raise RuntimeError("No primitive-rendered final-eligible contour SVG candidates passed hard gates")
        sheet = make_contact_sheet(final_pool[: config.candidate_search.cv_shortlist_size], debug_dir / "contact_sheet.png")
        if sheet is None:
            raise RuntimeError("Contact sheet is required for E1 Gemini ranking")
        judge_candidates_with_gemini(
            final_pool[: config.candidate_search.cv_shortlist_size],
            contact_sheet=sheet,
            original_image=normalized_path,
            semantic_plan=semantic_plan,
            config=config,
            out_path=debug_dir / "gemini_scores.json",
        )
        ranked = rank_candidates(final_pool)
        write_leaderboard(rank_candidates(candidates), out_dir / "leaderboard.csv")
        self.status.stage(
            "ranking",
            step_index=18,
            step_total=STEP_TOTAL,
            label="CV shortlist and Gemini ranking done",
            status="done",
            ranked_candidates=len(ranked),
            proposal_candidates=sum(1 for c in candidates if c.proposal_only),
            top_candidate=ranked[0].candidate_id if ranked else None,
        )

        if not ranked:
            raise RuntimeError("No contour SVG candidates were generated")
        self.status.stage("export", step_index=19, step_total=STEP_TOTAL, label="export final SVG")
        exported = export_final(ranked[0], ranked, config, semantic_plan, warnings)
        self.status.stage(
            "export",
            step_index=19,
            step_total=STEP_TOTAL,
            label="export final SVG done",
            status="done",
            final_svg=exported.get("final_svg"),
            preview_png=exported.get("preview_png"),
            candidate_count=len(ranked),
        )
        return RunResult(
            output_dir=out_dir,
            final_svg=Path(exported["final_svg"]) if exported.get("final_svg") else None,
            preview_png=Path(exported["preview_png"]) if exported.get("preview_png") else None,
            candidates=ranked,
            warnings=warnings,
        )


def _variant_from_raster_label(label: str) -> str:
    prefix = label.split("_", 1)[0].upper()
    return prefix if prefix in {"B1", "B2", "B3", "B4"} else "B1"


def _family_from_raster_variant(variant: str) -> str:
    return {
        "B1": "CONTROLNET_LINEART",
        "B2": "CONTROLNET_MLSD",
        "B3": "CONTROLNET_REFERENCE_LINEART",
        "B4": "CONTROLNET_REFERENCE_MLSD",
    }.get(variant, "CONTROLNET_LINEART")
