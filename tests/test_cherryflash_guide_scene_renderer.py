from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageChops, ImageStat

from scripts import render_cherryflash_full as renderer


def _rms_diff(a: Image.Image, b: Image.Image) -> float:
    stat = ImageStat.Stat(ImageChops.difference(a.convert("RGB"), b.convert("RGB")))
    return sum(value * value for value in stat.rms) ** 0.5


def test_guide_excursion_scene_starts_with_reveal_not_static_card(tmp_path) -> None:
    avatar = tmp_path / "avatar.jpg"
    Image.new("RGB", (320, 320), (180, 130, 90)).save(avatar)
    scene = renderer.RenderScene(
        index=1,
        variant="guide_excursion_promo",
        title="История в переплётах: экскурсия по библиотеке БФУ",
        date_line="10 июля 16:00",
        location_line="",
        description="",
        image_path=avatar,
        image_paths=(avatar,),
        start_local=0.0,
        guide_excursion={
            "palette": "prussian_cream",
            "icon_kind": "building",
            "contact": "@amber_fringilla",
            "contact_label": "запись",
        },
    )

    first = renderer._render_guide_excursion_frame(scene, 0.0)
    second = renderer._render_guide_excursion_frame(scene, 1.0 / renderer.FPS)
    third = renderer._render_guide_excursion_frame(scene, 2.0 / renderer.FPS)
    composed = renderer._render_guide_excursion_frame(scene, 4.20)

    assert _rms_diff(first, composed) > 40.0
    assert _rms_diff(first, second) > 1.0
    assert _rms_diff(second, third) > 1.0


def test_guide_excursion_uses_svg_repo_icon_masks() -> None:
    for kind in ("walk", "route", "water", "building"):
        mask_path = renderer._resolve_guide_icon_mask(kind)
        assert mask_path is not None, kind
        assert mask_path.name.endswith(".mask.png")
        icon = renderer._guide_icon(kind, 52, (1, 2, 3), (250, 250, 250))
        assert icon.size == (52, 52)
        assert icon.getchannel("A").getbbox() is not None


def test_guide_true3d_approved_renderer_is_tracked_version() -> None:
    source = Path("scripts/render_cherryflash_guide_true3d_v4.py").read_text(encoding="utf-8")

    assert 'APPROVED_VERSION = "true3d-v4-approved-2026-07-11"' in source
    assert '"bg":"#0E5B7B"' in source
    assert "med_radius = 0.38" in source
    assert "med_x_sep = 0.76" in source
    assert "date_line\": \"10 июля 16:00\"" in source
    assert "artifact run `run_video_20260711_130410`" in source


def test_guide_true3d_content_preserves_date_contact_icon_and_palette(tmp_path) -> None:
    avatar = tmp_path / "avatar.jpg"
    Image.new("RGB", (320, 320), (180, 130, 90)).save(avatar)
    scene = renderer.RenderScene(
        index=1,
        variant="guide_excursion_promo",
        title="История в переплётах: экскурсия по библиотеке БФУ",
        date_line="10 июля 16:00",
        location_line="",
        description="",
        image_path=avatar,
        image_paths=(avatar,),
        start_local=0.0,
        guide_excursion={
            "palette": "museum_green_ivory",
            "icon_kind": "building",
            "contact": "@amber_fringilla",
            "contact_label": "ЗАПИСЬ В TELEGRAM",
        },
    )

    content = renderer._guide_true3d_scene_content(scene)

    assert content["title"] == "История в переплётах: экскурсия по библиотеке БФУ"
    assert content["date_line"] == "10 июля 16:00"
    assert content["contact"] == "@amber_fringilla"
    assert content["contact_label"] == "ЗАПИСЬ В TELEGRAM"
    assert content["icon_kind"] == "building"
    assert content["palette"] == "museum_green_ivory"
    assert content["avatars"] == [str(avatar)]
