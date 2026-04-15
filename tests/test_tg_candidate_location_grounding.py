from types import SimpleNamespace

import pytest


@pytest.mark.asyncio
async def test_tg_build_candidate_replaces_unsupported_extracted_location_from_poster_ocr():
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(default_location=None, default_ticket_link=None, trust_level=None)

    message = {
        "source_username": "signalkld",
        "message_id": 10431,
        "source_link": "https://t.me/signalkld/10431",
        "text": (
            "ЭкоКёниг приглашает на Весенний Экодвор.\n"
            "Вход в Железнодорожные ворота — со стороны Парка Победы."
        ),
        "events": [
            {
                "title": "Весенний Экодвор",
                "date": "2026-04-19",
                "time": "12:00-15:00",
                "location_name": "Фридландские ворота",
            }
        ],
        "posters": [
            {
                "sha256": "poster1",
                "ocr_text": (
                    "ВЕСЕННИЙ ЭКОДВОР\n"
                    "19 АПРЕЛЯ, 12:00-15:00\n"
                    "КАЛИНИНГРАД, ЖЕЛЕЗНОДОРОЖНЫЕ ВОРОТА"
                ),
            }
        ],
    }

    cand = _build_candidate(src, message, message["events"][0])

    assert (cand.location_name or "").casefold() != "фридландские ворота"
    assert "железнодорож" in (cand.location_name or "").casefold()
