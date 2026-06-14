from pathlib import Path

from contour_svg.config import RunConfig
from contour_svg.completion import build_completion_proposals
from contour_svg.contracts import CompletionProposal, FacadeElement, LineGroup, LinePrimitive, MaskBundle, SemanticPlan
from contour_svg.gemini_line_editor import _line_group_schema
from contour_svg.primitive_renderer import primitive_candidates_from_groups


def test_primitive_renderer_creates_final_eligible_candidates(tmp_path: Path):
    masks = MaskBundle(
        object_visible=tmp_path / "object.png",
        occluder=tmp_path / "occluder.png",
        background=tmp_path / "background.png",
        object_unknown=tmp_path / "unknown.png",
        allowed_line_region=tmp_path / "allowed.png",
        overlay=tmp_path / "overlay.png",
    )
    scene = SemanticPlan(primary_object={"label": "building", "confidence": 0.9})
    groups = [
        LineGroup(
            id="G_silhouette_001",
            members=["L_1"],
            merged_geometry=LinePrimitive([(0, 0), (100, 0), (100, 80), (0, 80), (0, 0)], role="silhouette"),
            semantic_label="silhouette",
            importance="primary",
            decision="keep",
            confidence=0.9,
        ),
        LineGroup(
            id="G_roof_edge_001",
            members=["L_2"],
            merged_geometry=LinePrimitive([(10, 10), (90, 10)], role="roofline"),
            semantic_label="roof_edge",
            importance="primary",
            decision="keep",
            confidence=0.8,
        ),
    ]

    candidates = primitive_candidates_from_groups(groups, masks=masks, semantic_plan=scene, out_dir=tmp_path, config=RunConfig())

    assert candidates
    assert all(candidate.final_eligible for candidate in candidates)
    assert all(candidate.primitive_rendered for candidate in candidates)
    assert all(not candidate.proposal_only for candidate in candidates)
    assert (tmp_path / "debug" / "arch_primitives.json").exists()


def test_primitive_renderer_uses_facade_elements_and_completion(tmp_path: Path):
    masks = MaskBundle(
        object_visible=tmp_path / "object.png",
        occluder=tmp_path / "occluder.png",
        background=tmp_path / "background.png",
        object_unknown=tmp_path / "unknown.png",
        allowed_line_region=tmp_path / "allowed.png",
        overlay=tmp_path / "overlay.png",
    )
    scene = SemanticPlan(primary_object={"label": "building", "confidence": 0.9})
    groups = [
        LineGroup(
            id="G_silhouette_001",
            members=["L_1"],
            merged_geometry=LinePrimitive([(0, 0), (120, 0), (120, 90), (0, 90), (0, 0)], role="silhouette"),
            semantic_label="silhouette",
            importance="primary",
            decision="keep",
            confidence=0.9,
        ),
        LineGroup(
            id="G_roof_edge_001",
            members=["L_2"],
            merged_geometry=LinePrimitive([(10, 12), (110, 12)], role="roofline"),
            semantic_label="roof_edge",
            importance="primary",
            decision="keep",
            confidence=0.8,
        ),
    ]
    elements = [
        FacadeElement(
            id="E_window_001",
            element_type="window",
            bbox_xyxy=(35, 30, 55, 65),
            confidence=0.82,
            source="unit_facade_parser",
            evidence=["semantic_mask_evidence"],
        )
    ]
    completions = [
        CompletionProposal(
            id="C_gap_001",
            completion_type="cornice_gap",
            geometry=LinePrimitive([(15, 70), (100, 70)], role="structure", source="completion"),
            confidence=0.42,
            evidence=["completion_evidence"],
        )
    ]

    candidates = primitive_candidates_from_groups(
        groups,
        masks=masks,
        semantic_plan=scene,
        out_dir=tmp_path,
        config=RunConfig(),
        facade_elements=elements,
        completion_proposals=completions,
    )

    primitive_ids = {pid for candidate in candidates for pid in (candidate.candidate_svg.primitive_ids if candidate.candidate_svg else [])}
    assert any("window" in pid for pid in primitive_ids)
    assert any("completion" in pid for pid in primitive_ids)


def test_completion_proposals_write_debug_artifacts(tmp_path: Path):
    from PIL import Image

    for name in ["object.png", "occluder.png", "background.png", "unknown.png", "allowed.png", "overlay.png"]:
        Image.new("L", (140, 100), 255 if name in {"unknown.png", "allowed.png"} else 0).save(tmp_path / name)
    masks = MaskBundle(
        object_visible=tmp_path / "object.png",
        occluder=tmp_path / "occluder.png",
        background=tmp_path / "background.png",
        object_unknown=tmp_path / "unknown.png",
        allowed_line_region=tmp_path / "allowed.png",
        overlay=tmp_path / "overlay.png",
    )
    groups = [
        LineGroup(
            id="G_cornice_left",
            members=["L1"],
            merged_geometry=LinePrimitive([(10, 40), (45, 40)], role="structure"),
            semantic_label="cornice_band",
            importance="secondary",
            decision="keep",
            confidence=0.8,
        ),
        LineGroup(
            id="G_cornice_right",
            members=["L2"],
            merged_geometry=LinePrimitive([(80, 42), (125, 42)], role="structure"),
            semantic_label="cornice_band",
            importance="secondary",
            decision="keep",
            confidence=0.8,
        ),
    ]

    proposals, warnings = build_completion_proposals(groups, [], masks, tmp_path)

    assert warnings == []
    assert proposals
    assert (tmp_path / "debug" / "completion_proposals.json").exists()
    assert (tmp_path / "debug" / "completion_overlay.png").exists()


def test_line_group_schema_does_not_use_empty_enum_values():
    schema = _line_group_schema(["G_roof_001", "G_wall_001"])
    properties = schema["properties"]["group_actions"]["items"]["properties"]

    assert properties["group_id"]["enum"] == ["G_roof_001", "G_wall_001"]
    assert "enum" not in properties["target_group_id"]
