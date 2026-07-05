from __future__ import annotations

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
        date_line="10 ИЮЛЯ • 16:00",
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
