from __future__ import annotations

from volunteer_monitor.festival_source_search import (
    SearchCandidate,
    _dedupe,
    festival_source_query,
)


def test_query_is_bounded_and_contains_resolution_anchors() -> None:
    query = festival_source_query(
        name_hint="Море внутри",
        city="Светлогорск",
        date_hint="8–9 августа 2026",
        organizer="Арт-группа",
    )
    assert '"Море внутри"' in query
    assert "Светлогорск" in query
    assert "официальный сайт" in query


def test_candidates_are_canonicalized_and_deduplicated() -> None:
    rows = [
        SearchCandidate("test", "https://festival.ru/#top", "A", "one"),
        SearchCandidate("test", "https://festival.ru/", "B", "two"),
        SearchCandidate("test", "javascript:alert(1)", "bad", "bad"),
    ]
    result = _dedupe(rows, 8)
    assert result == [SearchCandidate("test", "https://festival.ru/", "A", "one")]


def test_tavily_uses_bearer_auth_and_disables_generated_answer(monkeypatch) -> None:
    from volunteer_monitor.festival_source_search import TavilySearchProvider

    captured = {}

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "results": [
                    {
                        "url": "https://festival.example/",
                        "title": "Фестиваль",
                        "content": "Официальная страница",
                    }
                ],
                "usage": {"credits": 1},
            }

    def fake_post(url, **kwargs):
        captured["url"] = url
        captured.update(kwargs)
        return Response()

    monkeypatch.setattr("volunteer_monitor.festival_source_search.requests.post", fake_post)
    rows = TavilySearchProvider("tvly-test").search("test", limit=3)
    assert captured["headers"]["Authorization"] == "Bearer tvly-test"
    assert captured["json"]["include_answer"] is False
    assert captured["json"]["include_raw_content"] is False
    assert rows[0].url == "https://festival.example/"


def test_gemini_keeps_only_grounded_tool_urls() -> None:
    from types import SimpleNamespace
    from volunteer_monitor.festival_source_search import _gemini_grounding_rows

    response = SimpleNamespace(
        text="A model may mention https://invented.example but prose is ignored",
        candidates=[
            SimpleNamespace(
                grounding_metadata=SimpleNamespace(
                    grounding_chunks=[
                        SimpleNamespace(
                            web=SimpleNamespace(
                                uri="https://official.example/festival",
                                title="Official festival",
                            )
                        ),
                        SimpleNamespace(web=SimpleNamespace(uri="", title="empty")),
                    ]
                )
            )
        ],
    )
    rows = _gemini_grounding_rows(response)
    assert [row.url for row in rows] == ["https://official.example/festival"]
