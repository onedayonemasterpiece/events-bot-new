from __future__ import annotations

from video_announce.scenario import VideoAnnounceScenario


def test_copy_assets_includes_cygre_fonts_for_crumple(tmp_path):
    scenario = VideoAnnounceScenario(None, None, chat_id=0, user_id=0)

    scenario._copy_assets(tmp_path, audio_name="The_xx_-_Intro.mp3")

    assert (tmp_path / "BebasNeue-Bold.ttf").exists()
    assert (tmp_path / "Benzin-Bold.ttf").exists()
    assert (tmp_path / "Oswald-VariableFont_wght.ttf").exists()
    assert (tmp_path / "DrukCyr-Bold.ttf").exists()
    assert (tmp_path / "Cygre-Medium.ttf").exists()
    assert (tmp_path / "Cygre-Regular.ttf").exists()
    assert (tmp_path / "Final.png").exists()
    assert (tmp_path / "The_xx_-_Intro.mp3").exists()
