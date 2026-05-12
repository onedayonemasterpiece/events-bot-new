from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from handlers import admin_assist_cmd, kenigsberg_stories_cmd
from kenigsberg_stories.state import (
    SecondRange,
    format_bans_report,
    map_generated_range_to_source,
    parse_second_ranges,
    recent_source_exclusions,
)
from scripts import render_kenigsberg_story as renderer
from PIL import Image, ImageDraw


def test_parse_second_ranges_accepts_single_seconds_and_ranges() -> None:
    ranges = parse_second_ranges("1-3, 7, 16-17")

    assert ranges == [
        SecondRange(1.0, 3.0),
        SecondRange(7.0, 8.0),
        SecondRange(16.0, 17.0),
    ]


def test_map_generated_ban_range_back_to_dominant_source_segment() -> None:
    issue = {
        "issue_number": 15,
        "dataset": "zigomaro/koenigsberg19191940",
        "segments": [
            {
                "timeline_start": 0.0,
                "timeline_end": 5.0,
                "dataset": "zigomaro/koenigsberg19191940",
                "source_file": "devau.mp4",
                "source_start": 12.0,
                "source_end": 17.0,
            },
            {
                "timeline_start": 5.0,
                "timeline_end": 10.0,
                "dataset": "zigomaro/koenigsberg19191940",
                "source_file": "kneiphof.mp4",
                "source_start": 40.0,
                "source_end": 45.0,
            },
        ],
    }

    mapped = map_generated_range_to_source(issue, SecondRange(3.0, 6.5))

    assert [
        (item["source_file"], item["source_start"], item["source_end"])
        for item in mapped
    ] == [
        ("devau.mp4", 15.0, 17.0),
    ]


def test_map_integer_ban_range_ignores_small_edge_overlap() -> None:
    issue = {
        "issue_number": 4,
        "dataset": "zigomaro/koenigsberg19191940",
        "segments": [
            {
                "timeline_start": 0.0,
                "timeline_end": 5.7,
                "dataset": "zigomaro/koenigsberg19191940",
                "source_file": "main.mp4",
                "source_start": 10.0,
            },
            {
                "timeline_start": 5.7,
                "timeline_end": 9.0,
                "dataset": "zigomaro/koenigsberg19191940",
                "source_file": "edge.mp4",
                "source_start": 40.0,
            },
        ],
    }

    mapped = map_generated_range_to_source(issue, SecondRange(4.0, 6.0))

    assert len(mapped) == 1
    assert mapped[0]["source_file"] == "main.mp4"
    assert mapped[0]["source_start"] == 14.0
    assert mapped[0]["source_end"] == 15.7


def test_format_bans_report_lists_recent_source_bans() -> None:
    report = format_bans_report(
        {
            "source_bans": [
                {
                    "issue_number": 15,
                    "dataset": "zigomaro/koenigsberg-winter",
                    "source_file": "winter.mp4",
                    "source_start": 1.25,
                    "source_end": 3.5,
                }
            ]
        }
    )

    assert "Kenigsberg bans: 1" in report
    assert "issue #15 zigomaro/koenigsberg-winter winter.mp4 1.25-3.50s" in report


def test_admin_assist_routes_kenigsberg_ban_request(monkeypatch) -> None:
    monkeypatch.setattr(
        admin_assist_cmd,
        "require_main_attr",
        lambda name: "/vk_misses" if name == "VK_MISS_REVIEW_COMMAND" else None,
    )

    proposals = admin_assist_cmd._heuristic_proposals(
        "в выпуске kenigsberg #15 бан 1-3, 7, 16-17"
    )

    assert proposals is not None
    assert proposals[0].action_id == "kenigsberg"
    assert admin_assist_cmd._build_command_text(
        proposals[0].action_id,
        proposals[0].args,
    ) == "/kenigsberg ban #15 1-3, 7, 16-17"


def test_admin_assist_canonicalizes_direct_kenigsberg_ban_request(monkeypatch) -> None:
    monkeypatch.setattr(
        admin_assist_cmd,
        "require_main_attr",
        lambda name: "/vk_misses" if name == "VK_MISS_REVIEW_COMMAND" else None,
    )

    proposals = admin_assist_cmd._heuristic_proposals("Kenigsberg #4 бан 4-6")

    assert proposals is not None
    assert proposals[0].action_id == "kenigsberg"
    assert admin_assist_cmd._build_command_text(
        proposals[0].action_id,
        proposals[0].args,
    ) == "/kenigsberg ban #4 4-6"


def test_kenigsberg_command_canonicalizes_reordered_ban_args() -> None:
    assert kenigsberg_stories_cmd._canonicalize_ban_args("#4 бан 4-6") == "ban #4 4-6"


def test_admin_assist_routes_kenigsberg_bans_list_request(monkeypatch) -> None:
    monkeypatch.setattr(
        admin_assist_cmd,
        "require_main_attr",
        lambda name: "/vk_misses" if name == "VK_MISS_REVIEW_COMMAND" else None,
    )

    proposals = admin_assist_cmd._heuristic_proposals("kenigsberg покажи список банов")

    assert proposals is not None
    assert proposals[0].action_id == "kenigsberg"
    assert admin_assist_cmd._build_command_text(
        proposals[0].action_id,
        proposals[0].args,
    ) == "/kenigsberg bans"


def test_kenigsberg_command_canonicalizes_bans_list_args() -> None:
    assert kenigsberg_stories_cmd._canonicalize_ban_args("покажи список банов") == "bans"


def test_renderer_avoids_source_bans(monkeypatch, tmp_path) -> None:
    video = tmp_path / "devau.mp4"
    video.write_bytes(b"stub")

    monkeypatch.setattr(renderer, "ffprobe_duration", lambda path: 12.0)

    segments = renderer.pick_video_segments(
        [video],
        [(0.0, 2.0)],
        rng=renderer.random.Random(1),
        dataset_slug="zigomaro/koenigsberg19191940",
        crop_px=96,
        source_bans=[{"source_file": "devau.mp4", "source_start": 0.0, "source_end": 9.0}],
    )

    assert segments[0]["source_start"] >= 9.0


def test_recent_source_exclusions_use_previous_issue_segments() -> None:
    exclusions = recent_source_exclusions(
        {
            "issues": {
                "15": {
                    "issue_number": 15,
                    "registered_at": "2026-05-12T10:00:00+00:00",
                    "dataset": "zigomaro/koenigsberg19191940",
                    "segments": [
                        {
                            "dataset": "zigomaro/koenigsberg19191940",
                            "source_file": "devau.mp4",
                            "source_start": 12.0,
                            "source_end": 14.5,
                        }
                    ],
                }
            }
        },
        max_age_days=3650,
    )

    assert exclusions == [
        {
            "dataset": "zigomaro/koenigsberg19191940",
            "source_file": "devau.mp4",
            "source_start": 12.0,
            "source_end": 14.5,
            "issue_number": 15,
            "created_at": "2026-05-12T10:00:00+00:00",
            "reason": "recent_generation",
        }
    ]


def test_renderer_selects_video_dataset_from_nested_kaggle_datasets_dir(tmp_path) -> None:
    dataset_dir = tmp_path / "datasets" / "zigomaro" / "koenigsberg19191940"
    dataset_dir.mkdir(parents=True)
    video = dataset_dir / "devau.mp4"
    video.write_bytes(b"stub")

    selected, period_key, dataset_slug = renderer.choose_video_dataset(
        tmp_path,
        renderer.random.Random(1),
    )

    assert selected == dataset_dir
    assert period_key == "1919-1940"
    assert dataset_slug == "zigomaro/koenigsberg19191940"


def test_renderer_randomly_selects_from_mounted_video_datasets(tmp_path) -> None:
    first = tmp_path / "datasets" / "koenigsberg19191940"
    second = tmp_path / "datasets" / "koenigsberg-winter"
    first.mkdir(parents=True)
    second.mkdir(parents=True)
    (first / "a.mp4").write_bytes(b"stub")
    (second / "b.mp4").write_bytes(b"stub")

    seen = {
        renderer.choose_video_dataset(tmp_path, renderer.random.Random(seed))[1]
        for seed in range(1, 12)
    }

    assert seen == {"1919-1940", "winter"}


def test_renderer_beat_slots_vary_by_seed() -> None:
    a = renderer.beat_slots(18.0, renderer.random.Random(1))
    b = renderer.beat_slots(18.0, renderer.random.Random(2))

    assert a != b


def test_renderer_music_selection_stays_inside_allowed_full_story_range(monkeypatch, tmp_path) -> None:
    music_dir = tmp_path / "music"
    music_dir.mkdir()
    track = music_dir / "The Promise.flac"
    track.write_bytes(b"stub")

    monkeypatch.setattr(renderer, "ffprobe_duration", lambda path: 266.0)
    selected, start, duration, meta = renderer.choose_music(
        music_dir,
        renderer.random.Random(1),
    )

    assert selected == track
    assert duration == renderer.MAIN_DURATION + 2 * renderer.OUTRO_SCREEN_DURATION
    assert start + duration <= meta["allowed_end"]
    assert meta["allowed_start"] == 224.0


def test_renderer_music_selection_rejects_unlisted_tracks(monkeypatch, tmp_path) -> None:
    music_dir = tmp_path / "music"
    music_dir.mkdir()
    (music_dir / "unknown-vocal.flac").write_bytes(b"stub")

    monkeypatch.setattr(renderer, "ffprobe_duration", lambda path: 120.0)

    with pytest.raises(RuntimeError, match="allowed instrumental range"):
        renderer.choose_music(music_dir, renderer.random.Random(1))


def test_renderer_music_selection_rejects_ranges_shorter_than_full_story(monkeypatch, tmp_path) -> None:
    music_dir = tmp_path / "music"
    music_dir.mkdir()
    (music_dir / "One Truth.flac").write_bytes(b"stub")

    monkeypatch.setattr(renderer, "ffprobe_duration", lambda path: 120.0)

    with pytest.raises(RuntimeError, match="long enough for the full story"):
        renderer.choose_music(music_dir, renderer.random.Random(1))


def test_renderer_splits_thought_without_repeating_last_line() -> None:
    lines = renderer.split_scene_lines(
        "Альбертина была устроена по классической университетской модели. "
        "В ней действовали богословский, юридический, медицинский и философский факультеты.",
        7,
    )

    assert len(lines) >= 3
    assert len(lines) == len(set(line.casefold() for line in lines))
    assert any("Альбертина" in line for line in lines)
    assert any("философский" in line for line in lines)
    draw = ImageDraw.Draw(Image.new("RGBA", (renderer.W, renderer.H)))
    fnt = renderer.font(44)
    assert all(
        len(renderer.wrap_text(line, draw, fnt, renderer.W - 120)) <= 4
        for line in lines
    )


def test_renderer_distributes_short_story_lines_across_all_segments() -> None:
    assigned = [
        renderer.scene_text_for_segment(["first", "second"], idx, 6)
        for idx in range(6)
    ]

    assert assigned == ["first", "first", "first", "second", "second", "second"]


def test_renderer_uses_payload_scene_lines_for_independent_text_cues() -> None:
    lines = renderer.payload_scene_lines(
        {"scene_lines": ["Первый смысловой экран", "Второй смысловой экран"]},
        "fallback thought",
        6,
    )
    cues = renderer.build_text_cues(lines, 18.0)

    assert lines == ["Первый смысловой экран", "Второй смысловой экран"]
    assert len(cues) == 2
    assert cues[0]["start"] < cues[0]["end"] < cues[1]["start"] < cues[1]["end"]
    assert cues[-1]["end"] > 14.0


def test_renderer_requires_payload_scene_lines() -> None:
    with pytest.raises(RuntimeError, match="scene_lines are required"):
        renderer.payload_scene_lines({}, "fallback thought", 6)


def test_kenigsberg_story_text_uses_thoughts_md_without_rewrite() -> None:
    thought = (
        "1724 год подарил Кёнигсбергу два символа сразу. "
        "В этот год три города объединились в единый Кёнигсберг, "
        "и в тот же год родился Иммануил Кант."
    )

    payload = kenigsberg_stories_cmd._prepare_story_text_from_thought(thought)

    assert payload["source"] == "thoughts_md"
    assert payload["scene_lines"] == [
        "1724 год подарил Кёнигсбергу два символа сразу.",
        "В этот год три города объединились в единый Кёнигсберг, и в тот же год родился Иммануил Кант.",
    ]
    assert payload["hook"] == "1724 год подарил Кёнигсбергу два символа сразу."


def test_kenigsberg_detects_stale_local_handoff_session() -> None:
    now = datetime(2026, 5, 12, 18, 42, tzinfo=timezone.utc)
    stale = SimpleNamespace(
        kaggle_kernel_ref="local:KoenigsbergStories",
        kaggle_dataset=None,
        started_at=now - timedelta(
            minutes=kenigsberg_stories_cmd.VIDEO_KAGGLE_HANDOFF_GRACE_MINUTES,
            seconds=1,
        ),
        created_at=None,
    )
    fresh = SimpleNamespace(
        kaggle_kernel_ref="local:KoenigsbergStories",
        kaggle_dataset=None,
        started_at=now - timedelta(minutes=1),
        created_at=None,
    )
    handed_off = SimpleNamespace(
        kaggle_kernel_ref="zigomaro/koenigsberg-stories",
        kaggle_dataset="zigomaro/kenigsberg-session-266-1778610442",
        started_at=now - timedelta(hours=1),
        created_at=None,
    )

    assert kenigsberg_stories_cmd._is_stale_local_handoff(stale, now=now) is True
    assert kenigsberg_stories_cmd._is_stale_local_handoff(fresh, now=now) is False
    assert kenigsberg_stories_cmd._is_stale_local_handoff(handed_off, now=now) is False


def test_renderer_cherryflash_style_outro_keeps_black_background() -> None:
    frame = renderer.draw_cherryflash_outro_screen(
        1.4,
        ["МОСТ", "В КЁНИГСБЕРГ"],
        sides=["left", "right"],
    )

    assert frame.getpixel((0, 0)) == (*renderer.OUTRO_BG, 255)
    assert frame.size == (renderer.W, renderer.H)


def test_renderer_blends_transition_frames() -> None:
    previous = Image.new("RGBA", (2, 2), (255, 0, 0, 255))
    current = Image.new("RGBA", (2, 2), (0, 0, 255, 255))

    blended = renderer.blend_transition_frame(current, [previous, previous], 0)

    assert blended.getpixel((0, 0))[0] > 0
    assert blended.getpixel((0, 0))[2] > 0


def test_renderer_extracts_cfr_segment_frames_and_pads_short_decode(monkeypatch, tmp_path) -> None:
    calls: list[list[str]] = []
    source = tmp_path / "source.mp4"
    source.write_bytes(b"stub")

    def fake_run(cmd: list[str]):
        calls.append(cmd)
        out_pattern = cmd[-1]
        out_dir = renderer.Path(out_pattern).parent
        out_dir.mkdir(parents=True, exist_ok=True)
        Image.new("RGB", (renderer.W, renderer.H), (10, 20, 30)).save(out_dir / "source_00001.jpg")
        Image.new("RGB", (renderer.W, renderer.H), (20, 30, 40)).save(out_dir / "source_00002.jpg")
        return object()

    monkeypatch.setattr(renderer, "run", fake_run)

    frames, meta = renderer.extract_segment_frames(
        {
            "source_path": str(source),
            "source_file": source.name,
            "source_start": 1.25,
            "timeline_start": 0.0,
            "timeline_end": 0.133,
        },
        tmp_path / "segment",
        frame_count=4,
        crop_px=96,
    )

    assert len(frames) == 4
    assert meta["decode_strategy"] == "ffmpeg_cfr_30fps"
    assert meta["decoded_frame_count"] == 2
    assert meta["pad_frame_count"] == 2
    assert "-vf" in calls[0]
    vf = calls[0][calls[0].index("-vf") + 1]
    assert "fps=30" in vf
    assert "crop=iw:if(gt(ih\\,96)\\,ih-96\\,ih):0:0" in vf
