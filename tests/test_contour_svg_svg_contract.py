from pathlib import Path

from contour_svg.config import RunConfig
from contour_svg.contracts import Candidate, LinePrimitive
from contour_svg.scoring import score_candidate
from contour_svg.svg_export import write_svg
from contour_svg.validators import validate_svg_file, validate_svg_text


def test_validate_rejects_embedded_raster():
    svg = '<svg viewBox="0 0 10 10"><image href="data:image/png;base64,abc" /></svg>'

    validation = validate_svg_text(svg, expected_stroke="#FFFFFF")

    assert validation.valid is False
    assert "embedded_image_tag" in validation.flags
    assert "embedded_raster_data" in validation.flags


def test_write_svg_outputs_true_stroke_svg(tmp_path: Path):
    config = RunConfig()
    candidate = Candidate(
        candidate_id="unit",
        variant="B1",
        family="P1_POSTCARD_BALANCED",
        lines=[
            LinePrimitive([(0, 0), (100, 0)], role="roofline", priority=9),
            LinePrimitive([(20, 20), (20, 120), (80, 120)], role="arc", priority=7),
        ],
    )
    svg_path = write_svg(candidate, config, tmp_path / "candidate.svg", source_size=(100, 120))

    text = svg_path.read_text(encoding="utf-8")
    validation = validate_svg_file(svg_path, expected_stroke="#FFFFFF")

    assert validation.valid is True
    assert validation.path_count == 2
    assert "<image" not in text
    assert "base64" not in text
    assert 'fill="none"' in text
    assert 'stroke-linecap="round"' in text
    assert 'stroke-linejoin="round"' in text


def test_write_svg_sketch_style_keeps_true_stroke_svg(tmp_path: Path):
    config = RunConfig()
    config.style.stroke_style = "sketch"
    candidate = Candidate(
        candidate_id="sketch",
        variant="B2",
        family="POSTCARD_MINIMAL",
        lines=[
            LinePrimitive([(0, 0), (100, 0), (100, 80), (0, 80), (0, 0)], role="silhouette", priority=9),
            LinePrimitive([(10, 20), (90, 20)], role="roofline", priority=8),
        ],
    )
    svg_path = write_svg(candidate, config, tmp_path / "sketch.svg", source_size=(100, 100))

    text = svg_path.read_text(encoding="utf-8")
    validation = validate_svg_file(svg_path, expected_stroke="#FFFFFF")

    assert validation.valid is True
    assert validation.path_count > len(candidate.lines)
    assert "<image" not in text
    assert "base64" not in text
    assert 'stroke-opacity="' in text


def test_final_svg_candidate_requires_primitive_renderer_and_silhouette(tmp_path: Path):
    config = RunConfig()
    candidate = Candidate(
        candidate_id="raw_trace",
        variant="B1",
        family="CONTROLNET_LINEART",
        lines=[LinePrimitive([(0, 0), (100, 0)], role="diffusion_lineart", priority=5)],
        final_eligible=True,
        primitive_rendered=False,
        raster_path=tmp_path / "lineart.png",
    )
    write_svg(candidate, config, tmp_path / "raw_trace.svg", source_size=(100, 100))

    score_candidate(candidate, expected_stroke="#FFFFFF")

    assert candidate.accepted is False
    assert "final_requires_primitive_renderer" in candidate.failure_flags
    assert "final_rejects_raster_derived_candidate" in candidate.failure_flags
    assert "missing_global_silhouette" in candidate.failure_flags


def test_primitive_rendered_candidate_with_silhouette_passes_final_gates(tmp_path: Path):
    config = RunConfig()
    candidate = Candidate(
        candidate_id="primitive",
        variant="B2",
        family="PRIMITIVE_ARCHITECTURAL_BALANCED",
        lines=[
            LinePrimitive([(0, 0), (100, 0), (100, 80), (0, 80), (0, 0)], role="silhouette", priority=9),
            LinePrimitive([(10, 20), (90, 20)], role="roofline", priority=8),
            LinePrimitive([(10, 50), (90, 50)], role="structure", priority=6),
        ],
        final_eligible=True,
        primitive_rendered=True,
    )
    write_svg(candidate, config, tmp_path / "primitive.svg", source_size=(100, 100))

    score_candidate(candidate, expected_stroke="#FFFFFF")

    assert candidate.accepted is True
    assert candidate.failure_flags == []
