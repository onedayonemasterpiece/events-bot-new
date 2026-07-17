from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

path = Path(__file__).parents[1] / "scripts/inspect/probe_briefing_crop_interval.py"
spec = spec_from_file_location("briefing_crop_probe", path)
probe = module_from_spec(spec); spec.loader.exec_module(probe)


def test_minimal_prompt_has_no_css_focal_request():
    prompt = probe.build_prompt(2560, 1541)
    assert len(prompt) < 260
    assert "crop-critical vertical interval" in prompt
    assert "focus" not in prompt.lower()


def test_solver_accepts_known_6611_interval():
    result = probe.solve_vertical_crop(height=1541, crop_height=656, top_px=64, bottom_px=548, margin_px=40)
    assert result["usable"] is True
    assert result["crop_top_px"] <= 24
    assert result["crop_bottom_px"] >= 588
    assert 0 <= result["focus_y"] <= 100


def test_solver_rejects_interval_taller_than_crop():
    result = probe.solve_vertical_crop(height=1080, crop_height=492, top_px=197, bottom_px=875, margin_px=40)
    assert result == {"usable": False, "reason": "critical_interval_does_not_fit"}
