import json
from pathlib import Path

from PIL import Image

from contour_svg.building_shell import build_building_shell, building_shell_to_candidate
from contour_svg.config import load_config
from contour_svg.contracts import FacadeElement, GuideSet, LinePrimitive, MaskBundle, SemanticPlan
from contour_svg.evidence_inventory import build_evidence_inventory
from contour_svg.feature_graph import build_feature_graph, feature_graph_to_candidate
from contour_svg.plane_graph import build_plane_graph, plane_graph_to_candidate
from contour_svg.scoring import score_candidate
from contour_svg.svg_export import render_preview, write_svg


FIXTURE = Path("docs/features/countur_svg_generator/samples/generated/audit_1527")


def _mask_bundle() -> MaskBundle:
    return MaskBundle(
        object_visible=FIXTURE / "mask_object_visible.png",
        occluder=FIXTURE / "mask_occluder.png",
        background=FIXTURE / "mask_background.png",
        object_unknown=FIXTURE / "mask_object_unknown.png",
        allowed_line_region=FIXTURE / "mask_allowed_line_region.png",
        overlay=FIXTURE / "masks_multistate_overlay.png",
    )


def _semantic_plan() -> SemanticPlan:
    return SemanticPlan(
        primary_object={"label": "yellow neoclassical corner building"},
        style_relevant_features=[
            {"feature": "roofline"},
            {"feature": "facade corner"},
            {"feature": "base volume"},
        ],
        occluders=[{"label": "tree"}, {"label": "fence"}],
        source="test_fixture",
    )


def _facade_elements() -> list[FacadeElement]:
    return [
        FacadeElement(
            id="fixture_wall_plane",
            element_type="wall_plane",
            bbox_xyxy=(200.0, 39.0, 1100.0, 674.0),
            confidence=0.70,
            source="audit_1527_fixture",
            mask_path=FIXTURE / "wall_plane.png",
        )
    ]


def _facade_elements_from_fixture_json() -> list[FacadeElement]:
    payload = json.loads((FIXTURE / "facade_elements.json").read_text(encoding="utf-8"))
    return [
        FacadeElement(
            id=item["id"],
            element_type=item["element_type"],
            bbox_xyxy=tuple(float(v) for v in item["bbox_xyxy"]),
            confidence=float(item["confidence"]),
            source=item["source"],
            evidence=list(item.get("evidence") or []),
            mask_path=FIXTURE / "wall_plane.png" if item["element_type"] == "wall_plane" else None,
            completion_status=item.get("completion_status") or "visible",
        )
        for item in payload
    ]


def _guides() -> GuideSet:
    lines = [
        LinePrimitive([(350, 180), (690, 40)], role="roofline", source="mlsd", confidence=0.86),
        LinePrimitive([(690, 40), (1060, 262)], role="roofline", source="deeplsd", confidence=0.82),
        LinePrimitive([(365, 260), (680, 125)], role="roofline", source="deeplsd", confidence=0.78),
        LinePrimitive([(690, 115), (1015, 285)], role="roofline", source="mlsd", confidence=0.76),
        LinePrimitive([(350, 510), (710, 525)], role="structure", source="deeplsd", confidence=0.80),
        LinePrimitive([(708, 525), (1100, 505)], role="structure", source="mlsd", confidence=0.82),
        LinePrimitive([(380, 635), (700, 675)], role="structure", source="hough", confidence=0.72),
        LinePrimitive([(700, 675), (1090, 640)], role="structure", source="hough", confidence=0.72),
        LinePrimitive([(685, 66), (700, 675)], role="structure", source="deeplsd", confidence=0.84),
        LinePrimitive([(1085, 294), (1092, 650)], role="structure", source="mlsd", confidence=0.78),
        LinePrimitive([(365, 185), (360, 630)], role="structure", source="hawp", confidence=0.75),
        LinePrimitive([(900, 235), (905, 650)], role="structure", source="hawp", confidence=0.72),
    ]
    return GuideSet(
        primary_mask=_mask_bundle().object_visible,
        occluder_mask=_mask_bundle().occluder,
        edge_map=FIXTURE / "edge_map.png",
        mlsd_guide=FIXTURE / "mlsd_guide.png",
        line_overlay=FIXTURE / "deeplsd_lines_overlay.png",
        lines=lines,
    )


def test_building_shell_stage_creates_coarse_scene_artifacts(tmp_path: Path) -> None:
    image = Image.open("docs/features/countur_svg_generator/samples/input/image - 2026-06-14T115705.752.png").convert("RGB")
    config = load_config("docs/features/countur_svg_generator/examples/sample_building.yaml")
    masks = _mask_bundle()
    guides = _guides()
    elements = _facade_elements()

    inventory = build_evidence_inventory(
        image=image,
        masks=masks,
        guides=guides,
        facade_elements=elements,
        semantic_plan=_semantic_plan(),
        out_dir=tmp_path,
    )
    shell = build_building_shell(
        image=image,
        masks=masks,
        guides=guides,
        facade_elements=elements,
        evidence=inventory,
        out_dir=tmp_path,
        config=config,
    )

    assert shell.passed is True
    assert shell.shell_confidence >= 0.52
    assert shell.roof_segments
    assert shell.base_segments
    assert shell.facade_corner_segments
    assert (tmp_path / "debug" / "evidence_inventory.json").exists()
    assert (tmp_path / "debug" / "evidence_contact_sheet.png").exists()
    assert (tmp_path / "debug" / "building_shell.json").exists()
    assert (tmp_path / "debug" / "building_shell_overlay.png").exists()
    assert (tmp_path / "debug" / "building_shell_score.json").exists()


def test_shell_only_candidate_is_valid_primitive_svg(tmp_path: Path) -> None:
    image = Image.open("docs/features/countur_svg_generator/samples/input/image - 2026-06-14T115705.752.png").convert("RGB")
    config = load_config("docs/features/countur_svg_generator/examples/sample_building.yaml")
    masks = _mask_bundle()
    guides = _guides()
    elements = _facade_elements()
    inventory = build_evidence_inventory(
        image=image,
        masks=masks,
        guides=guides,
        facade_elements=elements,
        semantic_plan=_semantic_plan(),
        out_dir=tmp_path,
    )
    shell = build_building_shell(
        image=image,
        masks=masks,
        guides=guides,
        facade_elements=elements,
        evidence=inventory,
        out_dir=tmp_path,
        config=config,
    )
    candidate = building_shell_to_candidate(shell, config)
    svg = tmp_path / "shell_only.svg"
    write_svg(candidate, config, svg, source_size=image.size)
    candidate.preview_path = render_preview(svg, tmp_path / "shell_only_preview.png")
    score_candidate(candidate, expected_stroke=config.style.stroke_color)

    assert candidate.accepted is True
    assert candidate.final_eligible is True
    assert candidate.primitive_rendered is True
    assert candidate.family == "SHELL_ONLY"
    assert 8 <= len(candidate.lines) <= 35
    assert candidate.parameters["shell_score"] >= 0.52


def test_plane_graph_stage_creates_scaffold_candidate(tmp_path: Path) -> None:
    image = Image.open("docs/features/countur_svg_generator/samples/input/image - 2026-06-14T115705.752.png").convert("RGB")
    config = load_config("docs/features/countur_svg_generator/examples/sample_building.yaml")
    masks = _mask_bundle()
    guides = _guides()
    elements = _facade_elements()
    inventory = build_evidence_inventory(
        image=image,
        masks=masks,
        guides=guides,
        facade_elements=elements,
        semantic_plan=_semantic_plan(),
        out_dir=tmp_path,
    )
    shell = build_building_shell(
        image=image,
        masks=masks,
        guides=guides,
        facade_elements=elements,
        evidence=inventory,
        out_dir=tmp_path,
        config=config,
    )
    graph = build_plane_graph(
        image=image,
        masks=masks,
        guides=guides,
        facade_elements=elements,
        evidence=inventory,
        shell=shell,
        out_dir=tmp_path,
        config=config,
    )
    candidate = plane_graph_to_candidate(shell, graph, config)
    svg = tmp_path / "plane_scaffold.svg"
    write_svg(candidate, config, svg, source_size=image.size)
    candidate.preview_path = render_preview(svg, tmp_path / "plane_scaffold_preview.png")
    score_candidate(candidate, expected_stroke=config.style.stroke_color)

    assert graph.passed is True
    assert graph.planes
    assert graph.bands
    assert graph.vertical_edges
    assert (tmp_path / "debug" / "plane_graph.json").exists()
    assert (tmp_path / "debug" / "plane_graph_overlay.png").exists()
    assert (tmp_path / "debug" / "plane_graph_score.json").exists()
    assert candidate.accepted is True
    assert candidate.final_eligible is True
    assert candidate.family == "PLANE_SCAFFOLD"
    assert 12 <= len(candidate.lines) <= 70


def test_feature_graph_stage_creates_element_scaffold_candidate(tmp_path: Path) -> None:
    image = Image.open("docs/features/countur_svg_generator/samples/input/image - 2026-06-14T115705.752.png").convert("RGB")
    config = load_config("docs/features/countur_svg_generator/examples/sample_building.yaml")
    masks = _mask_bundle()
    guides = _guides()
    elements = _facade_elements_from_fixture_json()
    inventory = build_evidence_inventory(
        image=image,
        masks=masks,
        guides=guides,
        facade_elements=elements,
        semantic_plan=_semantic_plan(),
        out_dir=tmp_path,
    )
    shell = build_building_shell(
        image=image,
        masks=masks,
        guides=guides,
        facade_elements=elements,
        evidence=inventory,
        out_dir=tmp_path,
        config=config,
    )
    plane_graph = build_plane_graph(
        image=image,
        masks=masks,
        guides=guides,
        facade_elements=elements,
        evidence=inventory,
        shell=shell,
        out_dir=tmp_path,
        config=config,
    )
    feature_graph = build_feature_graph(
        image=image,
        masks=masks,
        facade_elements=elements,
        shell=shell,
        plane_graph=plane_graph,
        out_dir=tmp_path,
        config=config,
    )
    candidate = feature_graph_to_candidate(shell, plane_graph, feature_graph, config)
    svg = tmp_path / "feature_scaffold.svg"
    write_svg(candidate, config, svg, source_size=image.size)
    candidate.preview_path = render_preview(svg, tmp_path / "feature_scaffold_preview.png")
    score_candidate(candidate, expected_stroke=config.style.stroke_color)

    assert feature_graph.passed is True
    assert len(feature_graph.features) >= 8
    assert feature_graph.rows
    assert (tmp_path / "debug" / "feature_graph.json").exists()
    assert (tmp_path / "debug" / "feature_graph_overlay.png").exists()
    assert (tmp_path / "debug" / "feature_graph_score.json").exists()
    assert candidate.accepted is True
    assert candidate.final_eligible is True
    assert candidate.family == "FEATURE_SCAFFOLD"
    assert 18 <= len(candidate.lines) <= 110
