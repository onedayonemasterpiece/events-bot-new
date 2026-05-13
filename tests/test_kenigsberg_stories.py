from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from PIL import Image
from handlers import admin_assist_cmd, kenigsberg_stories_cmd
from kenigsberg_stories.state import (
    SecondRange,
    choose_next_thought,
    format_bans_report,
    map_generated_range_to_source,
    parse_second_ranges,
    recent_music_exclusions,
    recent_source_exclusions,
    register_issue_manifest,
)
from scripts import render_kenigsberg_story as renderer


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


def test_kenigsberg_text_split_retry_delays_are_bounded(monkeypatch) -> None:
    monkeypatch.setenv("KENIGSBERG_STORIES_TEXT_SPLIT_RETRY_DELAYS_SEC", "1, 4, bad, 99")

    assert kenigsberg_stories_cmd._text_split_retry_delays(4) == [1.0, 4.0, 30.0]


@pytest.mark.asyncio
async def test_kenigsberg_launch_command_acknowledges_before_background(monkeypatch) -> None:
    answers: list[str] = []
    background_started = asyncio.Event()

    class FakeMessage:
        from_user = SimpleNamespace(id=1)
        chat = SimpleNamespace(id=2)

        async def answer(self, text: str):  # noqa: ANN001
            answers.append(text)
            return SimpleNamespace(chat=SimpleNamespace(id=2), message_id=len(answers))

    async def fake_require_superadmin(message):  # noqa: ANN001
        return True

    async def fake_background(message):  # noqa: ANN001
        background_started.set()

    monkeypatch.setattr(kenigsberg_stories_cmd, "_require_superadmin", fake_require_superadmin)
    monkeypatch.setattr(kenigsberg_stories_cmd, "_run_launch_in_background", fake_background)

    await kenigsberg_stories_cmd.cmd_kenigsberg(FakeMessage(), SimpleNamespace(args=None))

    assert answers == [
        "Kenigsberg: команду получил. Проверяю доступ и состояние запуска; "
        "следующие статусы придут отдельными сообщениями."
    ]
    await asyncio.wait_for(background_started.wait(), timeout=1)


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


def test_renderer_treats_recent_source_exclusions_as_soft(monkeypatch, tmp_path) -> None:
    video = tmp_path / "winter.mp4"
    video.write_bytes(b"stub")

    monkeypatch.setattr(renderer, "ffprobe_duration", lambda path: 5.0)

    segments = renderer.pick_video_segments(
        [video],
        [(0.0, 2.0), (2.0, 4.0)],
        rng=renderer.random.Random(1),
        dataset_slug="zigomaro/koenigsberg-winter",
        crop_px=96,
        source_bans=[
            {
                "dataset": "zigomaro/koenigsberg-winter",
                "source_file": "winter.mp4",
                "source_start": 0.0,
                "source_end": 5.0,
                "reason": "recent_generation",
            }
        ],
    )

    assert len(segments) == 2
    assert any(segment["source_soft_repeat_fallback"] for segment in segments)
    assert any(segment["source_overlaps_recent_generation"] for segment in segments)


def test_renderer_prefers_unused_video_files_within_one_story(monkeypatch, tmp_path) -> None:
    videos = []
    for name in ("a.mp4", "b.mp4", "c.mp4"):
        path = tmp_path / name
        path.write_bytes(b"stub")
        videos.append(path)

    monkeypatch.setattr(renderer, "ffprobe_duration", lambda path: 12.0)

    segments = renderer.pick_video_segments(
        videos,
        [(0.0, 2.0), (2.0, 4.0), (4.0, 6.0)],
        rng=renderer.random.Random(7),
        dataset_slug="zigomaro/koenigsberg19191940",
        crop_px=96,
        source_bans=[],
    )

    assert len({segment["source_file"] for segment in segments}) == 3


def test_renderer_avoids_overlapping_source_ranges_within_one_story(monkeypatch, tmp_path) -> None:
    video = tmp_path / "devau.mp4"
    video.write_bytes(b"stub")
    monkeypatch.setattr(renderer, "ffprobe_duration", lambda path: 20.0)

    segments = renderer.pick_video_segments(
        [video],
        [(0.0, 2.0), (2.0, 4.0), (4.0, 6.0)],
        rng=renderer.random.Random(1),
        dataset_slug="zigomaro/koenigsberg19191940",
        crop_px=96,
        source_bans=[],
    )

    ranges = sorted((segment["source_start"], segment["source_end"]) for segment in segments)
    assert all(left_end + 0.74 <= right_start for (_, left_end), (right_start, _) in zip(ranges, ranges[1:]))


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


@pytest.mark.asyncio
async def test_thought_is_marked_used_only_after_successful_manifest(monkeypatch, tmp_path) -> None:
    thoughts_path = tmp_path / "thoughts.md"
    thoughts_path.write_text("1. First\n2. Second\n", encoding="utf-8")
    state = {"used_thought_ids": [], "issues": {}}
    saved: list[dict] = []

    async def fake_load_state(db):  # noqa: ANN001
        return dict(state)

    async def fake_save_state(db, value):  # noqa: ANN001
        state.clear()
        state.update(value)
        saved.append(dict(value))

    monkeypatch.setattr("kenigsberg_stories.state.load_state", fake_load_state)
    monkeypatch.setattr("kenigsberg_stories.state.save_state", fake_save_state)
    monkeypatch.setattr("kenigsberg_stories.state.secrets.randbelow", lambda n: 0)

    chosen = await choose_next_thought(object(), thoughts_path=thoughts_path)

    assert chosen["id"] == "1"
    assert state["used_thought_ids"] == []
    assert saved == []

    await register_issue_manifest(
        object(),
        {
            "issue_number": 12,
            "thought_id": "1",
            "segments": [],
        },
    )

    assert state["used_thought_ids"] == ["1"]


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


def test_renderer_rhythm_slots_land_on_strong_beats_and_vary_by_seed() -> None:
    strong_beats = [0.72, 2.72, 4.72, 6.72, 8.72, 10.72, 12.72, 14.72, 16.72]
    a = renderer.rhythm_slots_from_strong_beats(strong_beats, 18.0, renderer.random.Random(1))
    b = renderer.rhythm_slots_from_strong_beats(strong_beats, 18.0, renderer.random.Random(2))

    assert a != b
    for slots in (a, b):
        assert slots[0][0] == 0.0
        assert slots[0][1] == 0.72
        assert slots[-1][1] in strong_beats
        assert slots[-1][1] < 18.0
        for start, end in slots[1:]:
            assert start in strong_beats
            assert end in strong_beats
            assert round(end - start, 2) in {2.0, 4.0}


def test_renderer_rhythm_slots_keep_target_duration_when_strong_beats_end_too_early() -> None:
    strong_beats = [0.72, 2.72, 4.72, 6.72, 8.72, 10.72]

    slots = renderer.rhythm_slots_from_strong_beats(strong_beats, 18.0, renderer.random.Random(1))

    assert slots[-2][1] == 10.72
    assert slots[-1] == (10.72, 18.0)


def test_renderer_approximate_rhythm_slots_cover_target_duration() -> None:
    slots = renderer.approximate_rhythm_slots(18.0, renderer.random.Random(1))

    assert slots[0][0] == 0.0
    assert slots[-1][1] == 18.0
    assert all(end > start for start, end in slots)


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


def test_renderer_music_selection_avoids_recent_overlapping_track(monkeypatch, tmp_path) -> None:
    music_dir = tmp_path / "music"
    music_dir.mkdir()
    recent_track = music_dir / "The Promise.flac"
    fresh_track = music_dir / "Wyatt Earth.flac"
    recent_track.write_bytes(b"stub")
    fresh_track.write_bytes(b"stub")

    monkeypatch.setattr(renderer, "ffprobe_duration", lambda path: 500.0)
    monkeypatch.setattr(renderer, "estimate_voice_risk", lambda path, start, duration: 0.0)

    selected, _start, _duration, meta = renderer.choose_music(
        music_dir,
        renderer.random.Random(1),
        recent_music=[
            {
                "file": "01 - The Promise.flac",
                "start": 224.0,
                "end": 260.0,
                "issue_number": 22,
            }
        ],
    )

    assert selected == fresh_track
    assert meta["recent_same_track"] is False
    assert meta["overlaps_recent"] is False


def test_renderer_music_selection_prefers_lower_voice_risk(monkeypatch, tmp_path) -> None:
    music_dir = tmp_path / "music"
    music_dir.mkdir()
    high_voice = music_dir / "The Promise.flac"
    low_voice = music_dir / "Wyatt Earth.flac"
    high_voice.write_bytes(b"stub")
    low_voice.write_bytes(b"stub")

    monkeypatch.setattr(renderer, "ffprobe_duration", lambda path: 500.0)
    monkeypatch.setattr(
        renderer,
        "estimate_voice_risk",
        lambda path, start, duration: 0.92 if "Promise" in path.name else 0.05,
    )

    selected, _start, _duration, meta = renderer.choose_music(music_dir, renderer.random.Random(2))

    assert selected == low_voice
    assert meta["voice_risk"] == 0.05


def test_recent_music_exclusions_uses_issue_manifest_history() -> None:
    state = {
        "recent_music": [],
        "issues": {
            "22": {
                "issue_number": 22,
                "registered_at": "2026-05-13T12:00:00+00:00",
                "selected_music": {
                    "file": "01 - The Promise.flac",
                    "start": 233.41,
                    "end": 257.738,
                },
            }
        },
    }

    recent = recent_music_exclusions(state, max_age_days=30)

    assert recent == [
        {
            "file": "01 - The Promise.flac",
            "start": 233.41,
            "end": 257.738,
            "issue_number": 22,
            "created_at": "2026-05-13T12:00:00+00:00",
        }
    ]


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


def test_renderer_rejects_overlong_payload_line_instead_of_splitting_on_kaggle() -> None:
    thought = (
        "Говорят, летом 1831 года в Кёнигсберге вспыхнула холера, "
        "и на волне страха люди верили даже самым невероятным слухам "
        "и даже подозревали Фридриха Вильгельма Бесселя с его "
        "«серебряными шарами» и обсерваторией в каких-то тёмных опытах."
    )

    with pytest.raises(RuntimeError, match="LLM split is required"):
        renderer.payload_scene_lines({"scene_lines": [thought]}, "fallback thought", 6)


def test_renderer_requires_payload_scene_lines() -> None:
    with pytest.raises(RuntimeError, match="scene_lines are required"):
        renderer.payload_scene_lines({}, "fallback thought", 6)


def test_kenigsberg_story_text_uses_llm_split_without_rewrite(monkeypatch) -> None:
    thought = (
        "1724 год подарил Кёнигсбергу два символа сразу. "
        "В этот год три города объединились в единый Кёнигсберг, "
        "и в тот же год родился Иммануил Кант."
    )

    async def fake_split(text: str) -> dict:
        assert text == thought
        return {
            "hook": "1724 год подарил Кёнигсбергу два символа сразу.",
            "scene_lines": [
                "1724 год подарил Кёнигсбергу два символа сразу.",
                "В этот год три города объединились в единый Кёнигсберг, и в тот же год родился Иммануил Кант.",
            ],
        }

    monkeypatch.setattr(kenigsberg_stories_cmd, "_ask_story_text_split_llm", fake_split)
    payload = asyncio.run(kenigsberg_stories_cmd._prepare_story_text_from_thought(thought))

    assert payload["source"] == "thoughts_md_llm_split"
    assert payload["scene_lines"] == [
        "1724 год подарил Кёнигсбергу два символа сразу.",
        "В этот год три города объединились в единый Кёнигсберг, и в тот же год родился Иммануил Кант.",
    ]
    assert payload["hook"] == "1724 год подарил Кёнигсбергу два символа сразу."


@pytest.mark.asyncio
async def test_kenigsberg_text_split_disables_google_fallback(monkeypatch) -> None:
    captured = {}

    class FakeClient:
        def __init__(self, **kwargs):  # noqa: ANN001
            self.fallback_models = ["gemma-4-31b-it"]
            self.incident_notifications_enabled = True
            self.max_retries = 99
            self.provider_timeout_seconds = 99
            captured["kwargs"] = kwargs
            captured["client"] = self

        async def generate_content_async(self, **kwargs):  # noqa: ANN001
            captured["generate"] = kwargs
            return (
                '{"hook":"Один экран.","scene_lines":["Один экран."]}',
                {},
            )

    class FakeSecretsProvider:
        pass

    monkeypatch.setattr(
        kenigsberg_stories_cmd,
        "require_main_attr",
        lambda name: (lambda: object()) if name == "get_supabase_client" else None,
    )
    import types
    import sys

    fake_module = types.SimpleNamespace(
        GoogleAIClient=lambda **kwargs: FakeClient(**kwargs),
        SecretsProvider=FakeSecretsProvider,
    )
    monkeypatch.setitem(sys.modules, "google_ai", fake_module)

    result = await kenigsberg_stories_cmd._ask_story_text_split_llm("Один экран.")

    assert result["scene_lines"] == ["Один экран."]
    assert captured["kwargs"]["incident_notifier"] is None
    assert captured["client"].fallback_models == []
    assert captured["client"].incident_notifications_enabled is False
    assert captured["generate"]["model"] == "gemini-3.1-flash-lite"


def test_kenigsberg_text_split_falls_back_to_4o_when_gemini_split_is_invalid(monkeypatch) -> None:
    thought = "Кнайпхоф был островом, но не окраиной. Он стал ключевой частью Кёнигсберга."

    async def fake_primary(_text: str) -> dict:
        return {
            "hook": "Кнайпхоф был островом, но не окраиной.",
            "scene_lines": ["Кнайпхоф был островом, но не окраиной."],
        }

    async def fake_ask_4o(*args, **kwargs):  # noqa: ANN002, ANN003
        assert kwargs["model"] == "gpt-4o"
        assert kwargs["meta"]["consumer"] == "kenigsberg_stories"
        assert kwargs["meta"]["stage"] == "text_split_4o_fallback"
        return (
            '{"hook":"Кнайпхоф был островом, но не окраиной.",'
            '"scene_lines":["Кнайпхоф был островом, но не окраиной.",'
            '"Он стал ключевой частью Кёнигсберга."]}'
        )

    monkeypatch.setattr(kenigsberg_stories_cmd, "_ask_story_text_split_llm", fake_primary)
    monkeypatch.setattr(
        kenigsberg_stories_cmd,
        "require_main_attr",
        lambda name: fake_ask_4o if name == "ask_4o" else None,
    )

    payload = asyncio.run(kenigsberg_stories_cmd._prepare_story_text_from_thought(thought))

    assert payload["source"] == "thoughts_md_llm_split"
    assert payload["text_model"] == "gpt-4o"
    assert payload["text_fallback_from"] == "gemini-3.1-flash-lite"
    assert " ".join(payload["scene_lines"]) == thought


def test_kenigsberg_long_single_sentence_thought_requires_llm_semantic_split(monkeypatch) -> None:
    thought = (
        "Говорят, летом 1831 года в Кёнигсберге вспыхнула холера, "
        "и на волне страха люди верили даже самым невероятным слухам "
        "и даже подозревали Фридриха Вильгельма Бесселя с его "
        "«серебряными шарами» и обсерваторией в каких-то тёмных опытах."
    )

    async def fake_split(text: str) -> dict:
        assert text == thought
        return {
            "hook": "Говорят, летом 1831 года в Кёнигсберге вспыхнула холера,",
            "scene_lines": [
                "Говорят, летом 1831 года в Кёнигсберге вспыхнула холера,",
                "и на волне страха люди верили даже самым невероятным слухам",
                "и даже подозревали Фридриха Вильгельма Бесселя с его «серебряными шарами»",
                "и обсерваторией в каких-то тёмных опытах.",
            ],
        }

    monkeypatch.setattr(kenigsberg_stories_cmd, "_ask_story_text_split_llm", fake_split)
    payload = asyncio.run(kenigsberg_stories_cmd._prepare_story_text_from_thought(thought))

    assert payload["source"] == "thoughts_md_llm_split"
    assert payload["scene_lines"] == [
        "Говорят, летом 1831 года в Кёнигсберге вспыхнула холера,",
        "и на волне страха люди верили даже самым невероятным слухам",
        "и даже подозревали Фридриха Вильгельма Бесселя с его «серебряными шарами»",
        "и обсерваторией в каких-то тёмных опытах.",
    ]
    assert " ".join(payload["scene_lines"]) == thought
    assert payload["hook"] == "Говорят, летом 1831 года в Кёнигсберге вспыхнула холера,"


def test_kenigsberg_story_text_fails_when_llm_drops_tail(monkeypatch) -> None:
    monkeypatch.setenv("KENIGSBERG_STORIES_TEXT_SPLIT_FALLBACK_4O_MODEL", "")
    thought = (
        "Говорят, летом 1831 года в Кёнигсберге вспыхнула холера, "
        "и на волне страха люди верили даже самым невероятным слухам."
    )

    async def fake_split(_text: str) -> dict:
        return {
            "hook": "Говорят, летом 1831 года в Кёнигсберге вспыхнула холера,",
            "scene_lines": ["Говорят, летом 1831 года в Кёнигсберге вспыхнула холера,"],
        }

    monkeypatch.setattr(kenigsberg_stories_cmd, "_ask_story_text_split_llm", fake_split)
    with pytest.raises(kenigsberg_stories_cmd.StoryTextPreparationError, match="changed"):
        asyncio.run(kenigsberg_stories_cmd._prepare_story_text_from_thought(thought))


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


def test_renderer_masks_bottom_source_strip() -> None:
    image = Image.new("RGBA", (renderer.W, renderer.H), (180, 180, 180, 255))
    draw = renderer.ImageDraw.Draw(image, "RGBA")
    draw.rectangle((0, renderer.H - 34, renderer.W, renderer.H), fill=(90, 90, 90, 255))
    draw.text((20, renderer.H - 30), "VEO watermark", fill=(245, 245, 245, 255))

    renderer.mask_bottom_source_strip(image, 34)

    assert image.getpixel((20, renderer.H - 20))[:3] != (245, 245, 245)
    assert image.getpixel((20, renderer.H - 20))[:3] == image.getpixel((renderer.W - 20, renderer.H - 20))[:3]


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


@pytest.mark.asyncio
async def test_kenigsberg_production_story_config_uses_mostvkenig_and_native_profile(monkeypatch):
    captured = {}

    async def fake_build_story_publish_config(db, *, main_chat_id, selection_params, selected_event_dates):
        captured["db"] = db
        captured["main_chat_id"] = main_chat_id
        captured["selection_params"] = selection_params
        captured["selected_event_dates"] = selected_event_dates
        return {"targets": selection_params["story_targets_override"]}

    monkeypatch.setattr(kenigsberg_stories_cmd, "build_story_publish_config", fake_build_story_publish_config)

    config = await kenigsberg_stories_cmd._build_production_story_config(db=object())

    assert config == {"targets": kenigsberg_stories_cmd._kenigsberg_story_targets_override()}
    params = captured["selection_params"]
    assert params["mode"] == "kenigsberg_story"
    assert params["story_publish_mode"] == "video"
    assert params["story_upload_profile"] == "telegram_story_native_hevc_720p_v1"
    assert params["story_targets_override"][0]["peer"] == "@mostvkenig"
