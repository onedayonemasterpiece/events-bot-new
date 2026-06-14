from pathlib import Path


def test_contour_svg_kaggle_runner_uses_status_framework() -> None:
    source = Path("kaggle/ContourSvgGenerator/script.py").read_text(encoding="utf-8")

    assert "from kaggle_status_client import load_status_client" in source
    assert "start_alive" in source
    assert "kernel_started" in source
    assert "preflight_ok" in source
    assert "report_written" in source
    assert "progress_percent" in source
    assert "progress_label" in source


def test_contour_svg_neural_branch_kaggle_runner_uses_stage_statuses() -> None:
    source = Path("kaggle/ContourSvgNeuralBranch/script.py").read_text(encoding="utf-8")

    assert "from kaggle_status_client import load_status_client" in source
    assert "start_alive" in source
    for event in [
        "kernel_started",
        "preflight_ok",
        "neural_inputs_started",
        "neural_img2img_started",
        "neural_report_written",
        "report_written",
    ]:
        assert event in source
    assert "progress_percent" in source
    assert "progress_label" in source


def test_contour_svg_pipeline_emits_domain_stage_progress() -> None:
    source = Path("contour_svg/pipeline.py").read_text(encoding="utf-8")

    for phase in [
        "semantic_plan",
        "groundingdino_primary",
        "sam2_primary",
        "groundingdino_occluders",
        "sam2_occluders",
        "multi_state_masks",
        "guides",
        "evidence_inventory",
        "building_shell",
        "plane_graph",
        "feature_graph",
        "rhythm_graph",
        "feature_graph_refined",
        "line_graph",
        "primitive_scene",
        "controlnet",
        "primitive_renderer",
        "ranking",
        "export",
    ]:
        assert phase in source


def test_contour_svg_status_updates_progress_and_calls_client() -> None:
    from contour_svg.status import ContourStatus

    calls = []

    class FakeClient:
        def event(self, *args, **kwargs):
            calls.append((args, kwargs))

    status = ContourStatus(FakeClient())
    status.stage("controlnet", step_index=8, step_total=11, label="ControlNet line-art candidates")

    assert status.progress["phase"] == "controlnet"
    assert status.progress["progress_percent"] == 73
    assert status.progress["progress_label"] == "ControlNet line-art candidates"
    assert calls[0][0] == ("controlnet_running",)
    assert calls[0][1]["phase"] == "controlnet"
    assert calls[0][1]["progress"]["step_index"] == 8
