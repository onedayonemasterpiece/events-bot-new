from pathlib import Path

from contour_svg.neural_branch import NeuralBranchConfig, run_neural_branch


FIXTURE = Path("docs/features/countur_svg_generator/samples/generated/audit_1527")
STYLE_REF = Path("docs/features/countur_svg_generator/samples/output/IMG_20260614_115550.webp")
SOURCE_IMAGE = Path("docs/features/countur_svg_generator/samples/input/image - 2026-06-14T115705.752.png")


def test_neural_branch_prepares_mask_edge_inputs_without_running_diffusion(tmp_path: Path) -> None:
    report = run_neural_branch(
        NeuralBranchConfig(
            artifact_dir=FIXTURE,
            out_dir=tmp_path,
            source_image=SOURCE_IMAGE,
            style_reference=STYLE_REF,
            variants=("A1", "A3", "B1", "C1", "D1", "E1"),
            run_neural=False,
        )
    )

    branch_dir = tmp_path / "neural_branch"
    input_dir = branch_dir / "input_maps"

    assert report["status"] == "prepared_only"
    assert report["neural_executed"] is False
    assert report["accepted_as_final"] is False
    assert report["source_image"] == str(SOURCE_IMAGE)
    assert (branch_dir / "N0_inputs_contact_sheet.png").exists()
    assert (branch_dir / "contact_sheet.png").exists()
    assert (branch_dir / "neural_branch_report.json").exists()
    for name in [
        "edge_only.png",
        "edge_thickened.png",
        "edge_plus_shell.png",
        "edge_plus_occluder_mask.png",
        "edge_plus_features.png",
        "photo_plus_edge_style_reference.png",
        "style_reference.png",
    ]:
        assert (input_dir / name).exists()
    assert len(report["prepared_inputs"]) == 6
