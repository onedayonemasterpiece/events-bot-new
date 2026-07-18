from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "site" / "scripts" / "export-production-preview-data.py"
SPEC = importlib.util.spec_from_file_location("export_production_preview_data_content", SCRIPT)
assert SPEC and SPEC.loader
EXPORTER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(EXPORTER)


def test_false_terminal_summary_is_replaced_by_complete_description_sentence() -> None:
    short = (
        "Спектакль «Гараж» — это сатирическая история о собрании гаражно-строительного "
        "кооператива «Фауна», где обычное обсуждение планов застройки."
    )
    description = (
        "Спектакль «Гараж» — это сатирическая история о собрании гаражно-строительного "
        "кооператива «Фауна», где обычное обсуждение планов застройки превращается в "
        "остросюжетный конфликт. В центре сюжета оказывается решение правления."
    )

    assert EXPORTER.event_summary(short, description) == (
        "Спектакль «Гараж» — это сатирическая история о собрании гаражно-строительного "
        "кооператива «Фауна», где обычное обсуждение планов застройки превращается в "
        "остросюжетный конфликт."
    )


def test_long_unsentenced_lead_uses_disclosed_word_boundary_ellipsis() -> None:
    description = " ".join(["длинное описание события"] * 40)
    lead = EXPORTER.event_summary(None, description)

    assert lead.endswith("…")
    assert len(lead) <= 321
    assert not lead.endswith(".\u2026")


def _structured_row(*, event_type: str = "экскурсия", time: str = "18:00") -> sqlite3.Row:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute(
        "create table event(source_text text, date text, time text, ticket_link text, event_type text)"
    )
    con.execute(
        "insert into event values(?,?,?,?,?)",
        (
            "Название: Женитьба\n"
            "Дата: 2026-08-09\n"
            "Время: 18:00\n"
            "Площадка: Драматический театр\n"
            "Ссылка: https://dramteatr39.ru/spektakli/jenitba\n\n"
            "Описание:\n"
            "О спектакле\n"
            "Пьесы Николая Гоголя традиционно вызывают зрительский интерес.\n\n"
            "Сцена: Основная",
            "2026-08-09",
            time,
            "https://dramteatr39.ru/spektakli/jenitba",
            event_type,
        ),
    )
    return con.execute("select * from event").fetchone()


def test_exact_structured_source_exposes_occurrence_type_and_description() -> None:
    row = _structured_row()
    projection = EXPORTER.structured_occurrence_projection(row)

    assert projection == {
        "title": "Женитьба",
        "date": "2026-08-09",
        "time": "18:00",
        "link": "https://dramteatr39.ru/spektakli/jenitba",
        "event_type": "спектакль",
        "description": "Пьесы Николая Гоголя традиционно вызывают зрительский интерес.",
    }
    assert EXPORTER.source_event_type_conflicts(row["event_type"], projection["event_type"])


def test_structured_source_guard_fails_closed_when_occurrence_time_differs() -> None:
    assert EXPORTER.structured_occurrence_projection(_structured_row(time="14:30")) is None


def test_classified_nonidentity_document_cannot_beat_strong_event_photo() -> None:
    document = {
        "src": "document.webp",
        "width": 757,
        "height": 800,
        "image_text_mode": "ocr_text",
        "media_semantic_status": "classified",
        "media_role": "unknown_document",
        "safe_crop": False,
    }
    photo = {
        "src": "photo.webp",
        "width": 1280,
        "height": 853,
        "image_text_mode": "visual_only",
        "media_semantic_status": "classified",
        "media_role": "event_photo",
        "safe_crop": True,
    }

    assert EXPORTER.choose_primary_image_asset([document, photo]) is photo


def test_shared_catalog_boilerplate_yields_to_strong_event_exclusive_visual() -> None:
    shared = {
        "src": "monthly-ad.webp",
        "width": 1200,
        "height": 800,
        "image_text_mode": "ocr_text",
        "media_semantic_status": "classified",
        "media_role": "unknown_document",
        "safe_crop": False,
        "event_reuse_count": 6,
    }
    exclusive = {
        "src": "event-scene.webp",
        "width": 1200,
        "height": 800,
        "image_text_mode": "visual_only",
        "media_semantic_status": "pending",
        "media_role": "unknown_document",
        "safe_crop": False,
        "event_reuse_count": 1,
    }

    assert EXPORTER.choose_primary_image_asset([shared, exclusive]) is exclusive


def test_shared_media_is_preserved_when_there_is_no_strong_exclusive_alternative() -> None:
    shared = {
        "src": "recurring-poster.webp",
        "width": 1200,
        "height": 800,
        "image_text_mode": "visual_only",
        "media_semantic_status": "pending",
        "media_role": "unknown_document",
        "safe_crop": False,
        "event_reuse_count": 3,
    }
    tiny_exclusive = {
        "src": "tiny.webp",
        "width": 320,
        "height": 240,
        "image_text_mode": "visual_only",
        "media_semantic_status": "pending",
        "media_role": "unknown_document",
        "safe_crop": False,
        "event_reuse_count": 1,
    }

    assert EXPORTER.choose_primary_image_asset([shared, tiny_exclusive]) is shared


def test_collect_images_honours_stored_ocr_mode_without_ocr_text() -> None:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute(
        """
        create table eventposter(
          id integer primary key,
          event_id integer,
          supabase_url text,
          catbox_url text,
          ocr_text text,
          image_text_mode text,
          review_status text,
          display_order integer,
          width integer,
          height integer,
          media_role text,
          media_role_confidence real,
          media_semantic_status text,
          safe_crop integer
        )
        """
    )
    con.execute(
        "insert into eventposter values(1,7,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "https://static.kenigevents.ru/p/document.webp",
            "https://source.invalid/document.webp",
            None,
            "ocr_text",
            "approved",
            0,
            757,
            800,
            "unknown_document",
            0.95,
            "classified",
            0,
        ),
    )

    _primary, mode, _role, assets = EXPORTER.collect_images(con, 7, "[]", "Событие")

    assert mode == "ocr_text"
    assert assets[0]["image_text_mode"] == "ocr_text"
    assert assets[0]["recommended_hero_fit"] == "contain"
    assert assets[0]["safe_crop"] is False
