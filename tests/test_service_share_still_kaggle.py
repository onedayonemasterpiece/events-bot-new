from __future__ import annotations

import importlib.util
import json
import tarfile
from pathlib import Path

from PIL import Image

from scripts.research.prepare_service_share_faces import date_label
from scripts.research.select_service_share_events import select
from scripts.research.service_share_poster_cubes.layout_contract import FAMILY_ORDER, resolve_layout
from scripts.run_service_share_still_kaggle import (
    PROFILES,
    build_bundle,
    stage_kernel,
    validate_output,
)


def _event(event_id: int, *, score: int, festival: str | None = None) -> dict:
    return {
        "id": event_id,
        "title": f"Event {event_id}",
        "start_date": "2026-07-20",
        "end_date": None,
        "lifecycle_status": "active",
        "image_url": f"https://example.test/{event_id}.jpg",
        "safe_crop": True,
        "festival": festival,
        "source_likes_count": score,
        "source_views_count": score * 100,
        "shares_count": score // 2,
        "source_engagement_sources_count": 1,
    }


def test_selection_preserves_popular_promoted_random_mix() -> None:
    events = [_event(index, score=20-index, festival="Promo") for index in range(1, 5)]
    events += [_event(index, score=20-index) for index in range(5, 14)]
    result = select(
        events,
        local_date="2026-07-15",
        promo_festivals={"Promo"},
        popular_count=3,
        promoted_count=1,
        random_count=3,
    )
    groups = [row["selection_group"] for row in result["events"]]
    assert groups.count("popular") == 3
    assert groups.count("promoted") == 1
    assert groups.count("random") == 3
    assert len({row["event_id"] for row in result["events"]}) == 7
    assert all(row["stable_daily_key"] for row in result["events"])


def test_kernel_profiles_keep_gpu_debug_and_cpu_final_separate(tmp_path: Path) -> None:
    debug = tmp_path / "debug"
    final = tmp_path / "final"
    debug.mkdir()
    final.mkdir()
    stage_kernel(debug, username="tester", profile="debug-gpu")
    stage_kernel(final, username="tester", profile="final-cpu")
    debug_meta = json.loads((debug / "kernel-metadata.json").read_text())
    final_meta = json.loads((final / "kernel-metadata.json").read_text())
    assert debug_meta["id"] == "tester/service-share-still-debug"
    assert debug_meta["title"] == "Service Share Still Debug"
    assert debug_meta["enable_gpu"] is True
    assert debug_meta["machine_shape"] == "NvidiaTeslaT4"
    assert final_meta["id"] == "tester/service-share-still-final"
    assert final_meta["title"] == "Service Share Still Final"
    assert final_meta["enable_gpu"] is False
    assert "machine_shape" not in final_meta
    assert PROFILES["debug-gpu"]["samples"] == 24
    assert PROFILES["final-cpu"]["samples"] == 256


def test_bundle_is_relative_minimal_and_secret_free(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "source"
    (bundle_dir / "brand").mkdir(parents=True)
    (bundle_dir / "fonts").mkdir()
    (bundle_dir / "kaggle_faces").mkdir()
    (bundle_dir / "brand" / "tag.png").write_bytes(b"brand")
    (bundle_dir / "fonts" / "font.ttf").write_bytes(b"font")
    (bundle_dir / "kaggle_faces" / "face.png").write_bytes(b"face")
    (bundle_dir / "kaggle_faces" / "face_manifest.json").write_text(json.dumps({
        "selection":{"requested_mix":{"popular":1,"promoted":0,"random":0}},
        "faces":[{"event_id":1,"face_path":"face.png"}],
    }))
    stage = tmp_path / "stage"
    stage.mkdir()
    tar_path, digest, _ = build_bundle(bundle_dir, stage)
    assert len(digest) == 64
    assert tar_path.name == "service_share_bundle.tarball"
    with tarfile.open(tar_path, "r:gz") as archive:
        names = archive.getnames()
    assert "bundle/tools/render_scene.py" in names
    assert "bundle/tools/composite_product.py" in names
    assert "bundle/tools/layout_contract.py" in names
    assert "bundle/faces/face.png" in names
    assert not any(".env" in name or "secret" in name.lower() for name in names)
    assert not any(name.startswith("/") or ".." in Path(name).parts for name in names)


def test_bundle_uses_tracked_brand_and_font_fallbacks(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "source"
    (bundle_dir / "kaggle_faces").mkdir(parents=True)
    (bundle_dir / "kaggle_faces" / "face.png").write_bytes(b"face")
    (bundle_dir / "kaggle_faces" / "face_manifest.json").write_text(json.dumps({
        "selection":{"requested_mix":{"popular":1,"promoted":0,"random":0}},
        "faces":[{"event_id":1,"face_path":"face.png"}],
    }))
    stage = tmp_path / "stage"
    stage.mkdir()
    tar_path, _, _ = build_bundle(bundle_dir, stage)
    with tarfile.open(tar_path, "r:gz") as archive:
        names = set(archive.getnames())
    assert "bundle/brand/desktop_tag_exact_240x88.png" in names
    assert "bundle/brand/desktop_tag_exact_960x352.png" in names
    assert "bundle/brand/announcements-wordmark-ui.svg" in names
    assert "bundle/fonts/Cygre-Regular.ttf" in names
    assert "bundle/fonts/Cygre-Bold.ttf" in names


def test_daily_composition_rotation_is_deterministic_and_changes_family() -> None:
    dates = ["2026-07-15", "2026-07-16", "2026-07-17"]
    resolved = [resolve_layout({"composition_date": day, "composition_family": "auto"}) for day in dates]
    assert resolved[0][0] == "soft_s_curve"
    assert {family for family, _, _ in resolved} == set(FAMILY_ORDER)
    assert resolve_layout({"composition_date": dates[0], "composition_family": "auto"}) == resolved[0]
    for family, _, layout in resolved:
        hero = layout[0]
        assert family in FAMILY_ORDER
        assert hero[0] == "HERO"
        assert hero[2] > 4.0
        assert len(layout) == 5


def test_event_date_labels_are_face_copy_not_snapshot_copy() -> None:
    assert date_label({"start_date": "2026-07-24", "end_date": "2026-07-24"}) == "24 ИЮЛЯ"
    assert date_label({"start_date": "2026-06-30", "end_date": "2026-07-30"}) == "ДО 30 ИЮЛЯ"
    composite = Path("scripts/research/service_share_poster_cubes/composite_product.py").read_text()
    assert "ДАННЫЕ НА" not in composite


def test_typography_pipeline_uses_high_resolution_brand_and_no_baked_face_shadow() -> None:
    composite = Path("scripts/research/service_share_poster_cubes/composite_product.py").read_text()
    faces = Path("scripts/research/prepare_service_share_faces.py").read_text()
    assert "desktop_tag_exact_960x352.png" in composite
    assert "ImageFont.Layout.RAQM" in composite
    assert "ImageFont.Layout.RAQM" in faces
    assert "shadow_draw" not in faces
    assert '"baked_text_shadow": False' in faces


def test_render_scene_enforces_hero_safe_zone_and_chain_overlap_gates() -> None:
    source = Path("scripts/research/service_share_poster_cubes/render_scene.py").read_text()
    for token in (
        "product safe-zone intrusion", "must exit both right and bottom",
        "_bbox_overlap_ratio", "insufficient screen-space overlap",
        "excessive 3D distance ratio", "seamless_cyclorama",
    ):
        assert token in source


def test_output_validator_rejects_gpu_fallback(tmp_path: Path) -> None:
    output = tmp_path / "card.png"
    Image.new("RGB", (512, 512)).save(output)
    import hashlib
    output_hash = hashlib.sha256(output.read_bytes()).hexdigest()
    (tmp_path / "service_share_render_result.json").write_text(json.dumps({
        "ok":True,"research_only":True,"profile":"debug-gpu","bundle_sha256":"abc",
        "resolution":[512,512],"actual_device":{"actual":"CPU"},
        "global_snapshot_date_present":False,"event_dates_on_faces":True,"selection_mix":{"popular":3},
        "composition_date":"2026-07-15","composition_family_requested":"auto","composition_seed_input":"",
        "composition":{"gates_passed":True,"family":"soft_s_curve","seed":resolve_layout({"composition_date":"2026-07-15","composition_family":"auto","composition_seed":""})[1]},
        "output_filename":"card.png","output_sha256":output_hash,
    }))
    config = {"profile":"debug-gpu","resolution":512,"selection_mix":{"popular":3},"composition_date":"2026-07-15","composition_family":"auto","composition_seed":""}
    import pytest
    with pytest.raises(RuntimeError, match="silently fell back"):
        validate_output(tmp_path, config, require_bundle_sha="abc")


def test_kernel_runtime_has_business_status_contract() -> None:
    source = Path("kaggle/ServiceShareStill/service_share_still.py").read_text()
    for token in (
        "load_status_client", "kernel_started", "preflight_ok", "render_started",
        "samples_done", "samples_total", "render_done", "report_written",
        "progress_label", "[service_share_status]",
    ):
        assert token in source
