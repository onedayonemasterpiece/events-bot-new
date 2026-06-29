from __future__ import annotations

from types import SimpleNamespace

import main as daily


def _events(count: int) -> list[SimpleNamespace]:
    return [SimpleNamespace(id=idx) for idx in range(1, count + 1)]


def _metric(event_id: int, *, likes: int = 0, reposts: int = 0, views: int = 0) -> daily.DailyAudienceMetric:
    return daily.DailyAudienceMetric(
        event_id=event_id,
        likes=likes,
        reposts=reposts,
        views=views,
        sources=1,
    )


def test_daily_audience_labels_relax_score_when_strict_share_is_low(monkeypatch) -> None:
    monkeypatch.setenv("DAILY_AUDIENCE_MIN_SCORE", "20")
    monkeypatch.setenv("DAILY_AUDIENCE_RELAXED_MIN_SCORE", "8")
    monkeypatch.setenv("DAILY_AUDIENCE_MIN_SHARE", "0.15")
    monkeypatch.setenv("DAILY_AUDIENCE_MAX_SHARE", "0.20")

    events = _events(10)
    metrics = {
        1: _metric(1, likes=25),
        2: _metric(2, likes=10),
        3: _metric(3, likes=7),
    }

    labels = daily._select_daily_audience_labels(events, metrics)

    assert list(labels) == [1, 2]


def test_daily_audience_labels_do_not_relax_when_strict_inventory_is_enough(monkeypatch) -> None:
    monkeypatch.setenv("DAILY_AUDIENCE_MIN_SCORE", "20")
    monkeypatch.setenv("DAILY_AUDIENCE_RELAXED_MIN_SCORE", "8")
    monkeypatch.setenv("DAILY_AUDIENCE_MIN_SHARE", "0.15")
    monkeypatch.setenv("DAILY_AUDIENCE_MAX_SHARE", "0.20")

    events = _events(10)
    metrics = {
        1: _metric(1, likes=30),
        2: _metric(2, likes=25),
        3: _metric(3, likes=10),
    }

    labels = daily._select_daily_audience_labels(events, metrics)

    assert list(labels) == [1, 2]


def test_daily_audience_labels_respect_relaxed_floor(monkeypatch) -> None:
    monkeypatch.setenv("DAILY_AUDIENCE_MIN_SCORE", "20")
    monkeypatch.setenv("DAILY_AUDIENCE_RELAXED_MIN_SCORE", "8")
    monkeypatch.setenv("DAILY_AUDIENCE_MIN_SHARE", "0.15")
    monkeypatch.setenv("DAILY_AUDIENCE_MAX_SHARE", "0.20")

    events = _events(10)
    metrics = {
        1: _metric(1, likes=25),
        2: _metric(2, likes=7),
    }

    labels = daily._select_daily_audience_labels(events, metrics)

    assert list(labels) == [1]
