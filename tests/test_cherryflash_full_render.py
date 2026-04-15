from __future__ import annotations

from pathlib import Path

import numpy as np
from PIL import Image

from scripts import render_cherryflash_full as full


def _write_frame(path: Path, color: tuple[int, int, int, int]) -> None:
    Image.new("RGBA", (4, 4), color).save(path)


def test_dedupe_exact_frames_only_changes_intro_segment(
    monkeypatch,
    tmp_path: Path,
) -> None:
    raw_dir = tmp_path / "raw"
    frames_dir = tmp_path / "frames"
    raw_dir.mkdir()
    frames_dir.mkdir()

    _write_frame(raw_dir / "frame_0001.png", (255, 0, 0, 255))
    _write_frame(raw_dir / "frame_0002.png", (255, 0, 0, 255))
    _write_frame(raw_dir / "frame_0003.png", (0, 0, 255, 255))
    _write_frame(raw_dir / "frame_0004.png", (0, 0, 255, 255))

    monkeypatch.setattr(full, "RAW_FRAMES_DIR", raw_dir)
    monkeypatch.setattr(full, "FRAMES_DIR", frames_dir)

    final_frame, removed_total, removed_before_anchor = full._dedupe_exact_frames(
        audio_anchor_frame=3,
        dedupe_end_frame=2,
    )

    assert final_frame == 3
    assert removed_total == 1
    assert removed_before_anchor == 1
    assert sorted(path.name for path in frames_dir.glob("*.png")) == [
        "frame_0001.png",
        "frame_0002.png",
        "frame_0003.png",
    ]
    assert Image.open(frames_dir / "frame_0001.png").getpixel((0, 0)) == (255, 0, 0, 255)
    assert Image.open(frames_dir / "frame_0002.png").getpixel((0, 0)) == (0, 0, 255, 255)
    assert Image.open(frames_dir / "frame_0003.png").getpixel((0, 0)) == (0, 0, 255, 255)


def test_build_render_scenes_appends_final_card(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _write_frame(tmp_path / "scene1.png", (255, 255, 255, 255))
    _write_frame(tmp_path / "Final.png", (255, 255, 0, 255))

    monkeypatch.setattr(full, "ROOT", tmp_path)

    scenes = full._build_render_scenes(
        {
            "scenes": [
                {
                    "title": "Scene 1",
                    "about": "Scene 1",
                    "date": "12 апреля",
                    "location": "Калининград",
                    "images": ["scene1.png"],
                }
            ]
        }
    )

    assert [scene.variant for scene in scenes] == ["primary", "brand_outro"]
    assert scenes[0].start_local == 0.0
    assert scenes[-1].image_path == tmp_path / "Final.png"


def test_build_render_scenes_keeps_full_timing_for_all_primary_scenes(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _write_frame(tmp_path / "scene1.png", (255, 255, 255, 255))
    _write_frame(tmp_path / "scene2.png", (240, 240, 240, 255))
    _write_frame(tmp_path / "Final.png", (255, 255, 0, 255))

    monkeypatch.setattr(full, "ROOT", tmp_path)

    scenes = full._build_render_scenes(
        {
            "scenes": [
                {
                    "title": "Scene 1",
                    "about": "Scene 1",
                    "date": "12 апреля",
                    "location": "Калининград",
                    "images": ["scene1.png"],
                },
                {
                    "title": "Scene 2",
                    "about": "Scene 2",
                    "date": "13 апреля",
                    "location": "Светлогорск",
                    "images": ["scene2.png"],
                },
            ]
        }
    )

    primary_scenes = [scene for scene in scenes if scene.variant == "primary"]
    assert len(primary_scenes) == 2
    assert {scene.start_local for scene in primary_scenes} == {0.0}


def test_primary_geometry_advances_primary_drift_every_30fps_frame(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _write_frame(tmp_path / "scene1.png", (255, 255, 255, 255))
    _write_frame(tmp_path / "Final.png", (255, 255, 0, 255))
    monkeypatch.setattr(full, "ROOT", tmp_path)

    scene = full._build_render_scenes(
        {
            "scenes": [
                {
                    "title": "Scene 1",
                    "about": "Scene 1",
                    "date": "12 апреля",
                    "location": "Калининград",
                    "images": ["scene1.png"],
                }
            ]
        }
    )[0]

    late_phase_start = (
        full.approval.T_ENTRY + full.approval.T_HOLD + full.approval.T_MOVE
    )
    _, y_a, _, _ = full._primary_geometry(late_phase_start, (1000, 1000))
    _, y_b, _, _ = full._primary_geometry(late_phase_start + (1.0 / full.FPS), (1000, 1000))

    assert y_b < y_a
    assert 0.2 <= (y_a - y_b) <= 0.5


def test_render_scene_frame_preserves_subpixel_primary_drift(tmp_path: Path) -> None:
    poster_path = tmp_path / "scene1.png"
    gradient = Image.new("RGBA", (32, 32))
    for x in range(32):
        for y in range(32):
            gradient.putpixel((x, y), (x * 7, y * 7, 128, 255))
    gradient.save(poster_path)

    scene = full.RenderScene(
        index=1,
        variant="primary",
        title="Scene 1",
        date_line="12 апреля",
        location_line="Калининград",
        description="",
        image_path=poster_path,
        start_local=0.0,
    )

    frame_a = full._render_scene_frame(scene, 2.767, [])
    frame_b = full._render_scene_frame(scene, 2.800, [])

    assert frame_a.tobytes() != frame_b.tobytes()


def test_render_scene_frame_keeps_encode_safe_primary_drift(tmp_path: Path) -> None:
    poster_path = tmp_path / "scene1.png"
    gradient = Image.new("RGBA", (64, 64))
    for x in range(64):
        for y in range(64):
            gradient.putpixel((x, y), ((x * 4) % 256, (y * 4) % 256, 128, 255))
    gradient.save(poster_path)

    scene = full.RenderScene(
        index=1,
        variant="primary",
        title="Scene 1",
        date_line="12 апреля",
        location_line="Калининград",
        description="",
        image_path=poster_path,
        start_local=0.0,
    )

    late_phase_start = (
        full.approval.T_ENTRY + full.approval.T_HOLD + full.approval.T_MOVE + 0.4
    )
    frame_a = full._render_scene_frame(scene, late_phase_start, [])
    frame_b = full._render_scene_frame(scene, late_phase_start + (1.0 / full.FPS), [])

    arr_a = np.asarray(
        frame_a.resize((270, 480), Image.Resampling.BILINEAR),
        dtype=np.int16,
    )
    arr_b = np.asarray(
        frame_b.resize((270, 480), Image.Resampling.BILINEAR),
        dtype=np.int16,
    )

    assert float(np.abs(arr_a - arr_b).mean()) > 0.15


def test_brand_outro_keeps_black_background(monkeypatch) -> None:
    frame = full._render_brand_outro_frame(1.6)

    assert frame.getpixel((0, 0)) == (*full.BG_BLACK, 255)


def test_render_scene_frames_uses_short_brand_outro_duration(
    monkeypatch,
    tmp_path: Path,
) -> None:
    raw_dir = tmp_path / "raw"
    preview_dir = tmp_path / "preview"
    raw_dir.mkdir()
    preview_dir.mkdir()

    monkeypatch.setattr(full, "RAW_FRAMES_DIR", raw_dir)
    monkeypatch.setattr(full, "PREVIEW_FRAMES_DIR", preview_dir)
    monkeypatch.setattr(full, "INTRO_END_FRAME", 0)
    monkeypatch.setattr(full, "FPS", 2)
    monkeypatch.setattr(full, "FINAL_CARD_DURATION", 1.0)
    monkeypatch.setattr(full, "SCENE_TOTAL_LOCAL", 5.0)
    monkeypatch.setattr(
        full,
        "_render_scene_frame",
        lambda scene, local_t, blocks: Image.new("RGBA", (4, 4), (255, 255, 0, 255)),
    )

    scene = full.RenderScene(
        index=1,
        variant="brand_outro",
        title="Outro",
        date_line="",
        location_line="",
        description="",
        image_path=tmp_path / "Final.png",
        start_local=0.0,
    )

    final_frame = full._render_scene_frames([scene])

    assert final_frame == 3
    assert sorted(path.name for path in raw_dir.glob("*.png")) == [
        "frame_0001.png",
        "frame_0002.png",
        "frame_0003.png",
    ]
