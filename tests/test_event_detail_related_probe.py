import json
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
            write_outputs(report, tmp_path / "probe", input_path)

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


if __name__ == "__main__":
    unittest.main()
