import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.build_event_detail_related_probe import build_probe, write_outputs


class EventDetailRelatedProbeTest(unittest.TestCase):
    def test_build_probe_filters_and_evaluates_related_candidates(self) -> None:
        sample = {
            "sample": [
                {"id": 100, "title": "Камерный джаз у моря", "event_type": "концерт", "city": "Калининград", "location_name": "Джаз-клуб", "time": "19:00", "digest": "джазовый концерт у моря вечером", "status": "active"},
                {"id": 101, "title": "Балтийский джаз", "event_type": "концерт", "city": "Калининград", "location_name": "Филармония", "time": "20:00", "digest": "джазовый концерт", "status": "active"},
                {"id": 102, "title": "Органный вечер", "event_type": "концерт", "city": "Калининград", "location_name": "Собор", "time": "19:00", "digest": "музыкальный вечер", "status": "active"},
                {"id": 103, "title": "Анна Каренина", "event_type": "спектакль", "city": "Калининград", "location_name": "Драмтеатр", "time": "19:00", "digest": "театральная постановка", "status": "active"},
                {"id": 104, "title": "Детский мастер-класс", "event_type": "мастер-класс", "city": "Калининград", "location_name": "Дом творчества", "time": "12:00", "digest": "создание игрушки для детей", "status": "active", "is_free": True},
                {"id": 106, "title": "Лекция об истории джаза", "event_type": "лекция", "city": "Калининград", "location_name": "Библиотека", "time": "18:30", "digest": "вечерняя лекция о джазе и музыке", "status": "active", "is_free": True},
                {"id": 107, "title": "Свинг на набережной", "event_type": "концерт", "city": "Калининград", "location_name": "Набережная", "time": "19:30", "digest": "джазовый концерт и живая музыка вечером", "status": "active"},
                {"id": 108, "title": "Музыкальная встреча", "event_type": "встреча", "city": "Калининград", "location_name": "Культурный центр", "time": "18:00", "digest": "обсуждение музыки и вечерняя встреча", "status": "active", "is_free": True},
                {"id": 105, "title": "Отменённый джаз", "event_type": "концерт", "city": "Калининград", "time": "19:00", "digest": "джаз", "status": "cancelled"},
            ]
        }
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_path = tmp_path / "sample.json"
            input_path.write_text(json.dumps(sample, ensure_ascii=False), encoding="utf-8")
            report = build_probe(input_path, current_event_id=100, top_k=5)
            write_outputs(report, tmp_path / "probe", sample)

            related_ids = [item["event_id"] for item in report["related_static_candidates"]["related_static"]]
            self.assertNotIn(100, related_ids)
            self.assertNotIn(105, related_ids)
            self.assertIn(101, related_ids)
            self.assertTrue(report["deterministic_checks"]["current_event_not_in_related"])
            self.assertTrue(report["deterministic_checks"]["cancelled_not_in_related"])
            self.assertTrue(report["ok"])
            self.assertEqual(report["cost_latency_report"]["provider_calls"], 0)
            self.assertTrue((tmp_path / "probe" / "persona_eval_report.md").exists())
            self.assertTrue((tmp_path / "probe" / "taxonomy_mapping_report.md").exists())
            payload = json.loads((tmp_path / "probe" / "event_detail_related_payload.json").read_text(encoding="utf-8"))
            html = (tmp_path / "probe" / "event_detail_related_static.html").read_text(encoding="utf-8")
            self.assertEqual(payload["surface"], "event_detail_related")
            self.assertEqual(payload["algorithm_id"], "static_related_v1")
            self.assertIn("Балтийский джаз", html)
            self.assertNotIn('data-event-id="100"', html)

    def test_cli_can_probe_sqlite_catalog_and_write_static_fallback_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "events.sqlite"
            con = sqlite3.connect(db_path)
            con.execute(
                "CREATE TABLE event (id INTEGER PRIMARY KEY, title TEXT, description TEXT, date TEXT, time TEXT, "
                "location_name TEXT, city TEXT, event_type TEXT, is_free INTEGER, search_digest TEXT, ticket_status TEXT)"
            )
            rows = [
                (100, "Камерный джаз у моря", "джазовый концерт у моря вечером", "2026-07-01", "19:00", "Джаз-клуб", "Калининград", "концерт", 0, "джазовый концерт", ""),
                (101, "Балтийский джаз", "джазовый концерт", "2026-07-02", "20:00", "Филармония", "Калининград", "концерт", 0, "джазовая музыка вечером", ""),
                (102, "Органный вечер", "музыкальный вечер", "2026-07-03", "19:00", "Собор", "Калининград", "концерт", 0, "классическая музыка", ""),
                (103, "Лекция об истории джаза", "лекция о джазе", "2026-07-04", "18:30", "Библиотека", "Калининград", "лекция", 1, "бесплатная лекция о музыке", ""),
            ]
            con.executemany("INSERT INTO event VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
            con.commit()
            con.close()

            output_dir = tmp_path / "probe"
            result = subprocess.run(
                [
                    sys.executable,
                    "scripts/build_event_detail_related_probe.py",
                    "--sqlite-db",
                    str(db_path),
                    "--output",
                    str(output_dir),
                    "--current-event-id",
                    "100",
                    "--top-k",
                    "3",
                ],
                check=True,
                cwd=Path(__file__).resolve().parents[1],
                text=True,
                capture_output=True,
            )
            self.assertIn('"ok": true', result.stdout)
            self.assertTrue((output_dir / "event_sample.json").exists())
            self.assertTrue((output_dir / "event_detail_related_payload.json").exists())
            html = (output_dir / "event_detail_related_static.html").read_text(encoding="utf-8")
            self.assertIn('data-surface="event_detail_related"', html)
            self.assertIn("Балтийский джаз", html)
            self.assertNotIn('data-event-id="100"', html)

    def test_static_fallback_html_handles_no_related_candidates(self) -> None:
        sample = {"sample": [{"id": 100, "title": "Одинокое событие", "event_type": "лекция", "city": "Калининград", "time": "19:00", "digest": "лекция без похожих событий", "status": "active"}]}
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_path = tmp_path / "sample.json"
            input_path.write_text(json.dumps(sample, ensure_ascii=False), encoding="utf-8")
            report = build_probe(input_path, current_event_id=100, top_k=5)
            write_outputs(report, tmp_path / "probe", sample)

            self.assertFalse(report["deterministic_checks"]["has_related_candidates"])
            html = (tmp_path / "probe" / "event_detail_related_static.html").read_text(encoding="utf-8")
            self.assertIn("Похожих событий пока нет.", html)
            self.assertNotIn('data-event-id="100"', html)


if __name__ == "__main__":
    unittest.main()
