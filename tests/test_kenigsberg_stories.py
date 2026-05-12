from __future__ import annotations

from handlers import admin_assist_cmd
from kenigsberg_stories.state import (
    SecondRange,
    format_bans_report,
    map_generated_range_to_source,
    parse_second_ranges,
)
from scripts import render_kenigsberg_story as renderer


def test_parse_second_ranges_accepts_single_seconds_and_ranges() -> None:
    ranges = parse_second_ranges("1-3, 7, 16-17")

    assert ranges == [
        SecondRange(1.0, 3.0),
        SecondRange(7.0, 8.0),
        SecondRange(16.0, 17.0),
    ]


def test_map_generated_ban_ranges_back_to_source_segments() -> None:
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

    mapped = map_generated_range_to_source(issue, SecondRange(3.0, 7.0))

    assert [
        (item["source_file"], item["source_start"], item["source_end"])
        for item in mapped
    ] == [
        ("devau.mp4", 15.0, 17.0),
        ("kneiphof.mp4", 40.0, 42.0),
    ]


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
