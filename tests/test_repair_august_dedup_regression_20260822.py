from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "ops" / "repair_august_dedup_regression_20260822.py"
SPEC = importlib.util.spec_from_file_location("repair_august_dedup", SCRIPT)
assert SPEC and SPEC.loader
repair = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(repair)


def _make_db(path: Path) -> None:
    con = sqlite3.connect(path)
    con.execute("PRAGMA foreign_keys=ON")
    con.executescript(
        """
        CREATE TABLE event(
          id INTEGER PRIMARY KEY, title TEXT NOT NULL, date TEXT NOT NULL, time TEXT NOT NULL,
          lifecycle_status TEXT NOT NULL DEFAULT 'active', silent INTEGER NOT NULL DEFAULT 0,
          identity_status TEXT NOT NULL DEFAULT 'canonical', merged_into_event_id INTEGER,
          linked_event_ids TEXT NOT NULL DEFAULT '[]', telegraph_url TEXT, ics_url TEXT,
          ics_post_url TEXT, tg_event_post_url TEXT, vk_repost_url TEXT,
          source_post_url TEXT, source_vk_post_url TEXT,
          FOREIGN KEY(merged_into_event_id) REFERENCES event(id)
        );
        CREATE TABLE smart_update_candidate_state(
          id INTEGER PRIMARY KEY, occurrence_key TEXT NOT NULL, current_outcome TEXT NOT NULL,
          accepted_event_id INTEGER, diagnostic_event_id INTEGER, reason TEXT,
          FOREIGN KEY(accepted_event_id) REFERENCES event(id),
          FOREIGN KEY(diagnostic_event_id) REFERENCES event(id)
        );
        CREATE TABLE smart_update_attempt(
          id INTEGER PRIMARY KEY, candidate_state_id INTEGER NOT NULL, terminal_outcome TEXT NOT NULL,
          accepted_event_id INTEGER, diagnostic_event_id INTEGER,
          FOREIGN KEY(candidate_state_id) REFERENCES smart_update_candidate_state(id),
          FOREIGN KEY(accepted_event_id) REFERENCES event(id),
          FOREIGN KEY(diagnostic_event_id) REFERENCES event(id)
        );
        CREATE TABLE event_source(
          id INTEGER PRIMARY KEY, event_id INTEGER NOT NULL, source_type TEXT NOT NULL,
          source_url TEXT NOT NULL, canonical_source_url TEXT, candidate_key TEXT,
          occurrence_key TEXT, smart_update_candidate_id INTEGER,
          FOREIGN KEY(event_id) REFERENCES event(id),
          FOREIGN KEY(smart_update_candidate_id) REFERENCES smart_update_candidate_state(id),
          UNIQUE(event_id,source_url)
        );
        CREATE TABLE event_source_fact(
          id INTEGER PRIMARY KEY, event_id INTEGER NOT NULL, source_id INTEGER NOT NULL, fact TEXT NOT NULL,
          FOREIGN KEY(event_id) REFERENCES event(id), FOREIGN KEY(source_id) REFERENCES event_source(id)
        );
        CREATE TABLE eventposter(
          id INTEGER PRIMARY KEY, event_id INTEGER NOT NULL, poster_hash TEXT NOT NULL,
          raw_sha256 TEXT, review_status TEXT, duplicate_of_id INTEGER, review_reason TEXT,
          FOREIGN KEY(event_id) REFERENCES event(id), FOREIGN KEY(duplicate_of_id) REFERENCES eventposter(id),
          UNIQUE(event_id,poster_hash)
        );
        CREATE UNIQUE INDEX ux_eventposter_event_raw_sha256
          ON eventposter(event_id,raw_sha256)
          WHERE raw_sha256 IS NOT NULL AND TRIM(raw_sha256) != '';
        CREATE TABLE joboutbox(
          id INTEGER PRIMARY KEY, event_id INTEGER NOT NULL, task TEXT NOT NULL,
          status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, payload TEXT,
          last_error TEXT, last_result TEXT, coalesce_key TEXT, depends_on INTEGER,
          updated_at TEXT, next_run_at TEXT,
          FOREIGN KEY(event_id) REFERENCES event(id)
        );
        CREATE TABLE event_publication(
          id INTEGER PRIMARY KEY, event_id INTEGER NOT NULL, platform TEXT NOT NULL,
          target TEXT NOT NULL, stored_url TEXT, live_url TEXT, stored_post_id INTEGER,
          live_post_id INTEGER, match_method TEXT, match_confidence REAL,
          status TEXT NOT NULL, resolved_at TEXT,
          FOREIGN KEY(event_id) REFERENCES event(id)
        );
        CREATE TABLE event_identity_decision_log(
          id INTEGER PRIMARY KEY AUTOINCREMENT, event_id INTEGER, candidate_event_id INTEGER,
          source_id INTEGER,
          decision TEXT NOT NULL, decision_reason TEXT, confidence REAL, decided_by TEXT,
          decision_payload TEXT,
          FOREIGN KEY(event_id) REFERENCES event(id), FOREIGN KEY(candidate_event_id) REFERENCES event(id),
          FOREIGN KEY(source_id) REFERENCES event_source(id)
        );
        """
    )
    con.executemany(
        "INSERT INTO event(id,title,date,time,linked_event_ids,telegraph_url,ics_url,tg_event_post_url,vk_repost_url) VALUES(?,?,?,?,?,?,?,?,?)",
        [
            (10, "Canonical", "2026-08-24", "18:00", "[11]", "https://telegra.ph/canonical", "https://ics/canonical", "https://t.me/kldevents/100", "https://vk.com/wall-1_100"),
            (11, "Obsolete replay", "2026-08-24", "18:00", "[30]", "https://telegra.ph/obsolete", "https://ics/obsolete", "https://t.me/kldevents/101", "https://vk.com/wall-1_101"),
            (20, "Exhibition", "2026-08-25", "10:00", "[]", None, None, None, None),
            (21, "Excursion", "2026-08-25", "12:00", "[]", None, None, None, None),
            (30, "External linked event", "2026-08-26", "19:00", "[11]", None, None, None, None),
        ],
    )
    con.executemany(
        "INSERT INTO smart_update_candidate_state(id,occurrence_key,current_outcome,accepted_event_id,diagnostic_event_id,reason) VALUES(?,?,?,?,?,?)",
        [
            (300, "occ-replay", "CREATED", 11, None, "historical create"),
            (301, "occ-diagnostic", "RETRY_SCHEDULED", None, 11, "diagnostic only"),
        ],
    )
    con.execute("INSERT INTO smart_update_attempt VALUES(400,300,'CREATED',11,NULL)")
    con.executemany(
        "INSERT INTO event_source VALUES(?,?,?,?,?,?,?,?)",
        [
            (100, 10, "telegram", "https://t.me/source/1", "https://t.me/source/1", "candidate-owner", "occ-owner", None),
            (101, 11, "telegram", "https://t.me/source/2", "https://t.me/source/2", "candidate-new", "occ-new", 300),
            (102, 11, "telegram", "https://t.me/source/1", "https://t.me/source/1", "candidate-replay", "occ-replay", 300),
            (120, 20, "telegram", "https://t.me/source/20", "https://t.me/source/20", None, None, None),
            (121, 21, "telegram", "https://t.me/source/21", "https://t.me/source/21", None, None, None),
        ],
    )
    con.execute(
        "INSERT INTO event_identity_decision_log(id,event_id,candidate_event_id,source_id,decision,decision_reason,confidence,decided_by,decision_payload) VALUES(50,11,10,102,'historical_create','legacy',0.8,'runtime','{}')"
    )
    con.executemany(
        "INSERT INTO event_source_fact VALUES(?,?,?,?)",
        [(500, 10, 100, "owner fact"), (501, 11, 101, "unique obsolete fact"), (502, 11, 102, "duplicate binding fact")],
    )
    con.executemany(
        "INSERT INTO eventposter(id,event_id,poster_hash,raw_sha256,review_status,duplicate_of_id,review_reason) VALUES(?,?,?,?,?,?,?)",
        [
            (200, 10, "shared", "raw-shared", "approved", None, None),
            (201, 11, "unique", "raw-unique", "approved", None, None),
            (202, 11, "shared", "raw-shared", "approved", None, None),
            # Production also enforces raw-byte identity.  Different derived
            # poster hashes may still represent the same uploaded bytes.
            (203, 10, "canonical-derived", "raw-same-bytes", "approved", None, None),
            (204, 11, "obsolete-derived", "raw-same-bytes", "approved", None, None),
        ],
    )
    con.executemany(
        "INSERT INTO joboutbox VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (600, 11, "event_media_review", "pending", 0, '{"event_id":11}', None, None, "media:11", None, "2026-08-22T10:00:00Z", "2026-08-22T10:05:00Z"),
            (601, 11, "telegraph_build", "done", 1, "null", None, "historic result", "telegraph:11", 600, "2026-08-22T09:00:00Z", "2026-08-22T09:00:00Z"),
            (602, 10, "event_media_review", "paused", 2, '{"event_id":10}', "paused", None, "media:10", None, "2026-08-22T08:00:00Z", "2026-08-23T08:00:00Z"),
        ],
    )
    con.executemany(
        "INSERT INTO event_publication VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (700, 10, "vk", "klgdevents", "https://vk.com/wall-1_100", "https://vk.com/wall-1_100", 100, 100, "exact", 1.0, "published", "2026-08-22T10:00:00Z"),
            (701, 11, "vk", "klgdevents", "https://vk.com/wall-1_101", "https://vk.com/wall-1_101", 101, 101, "exact", 1.0, "published", "2026-08-22T10:00:00Z"),
        ],
    )
    con.commit()
    con.close()


def _manifest(db: Path, path: Path) -> dict:
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row

    def event_hash(event_id: int) -> str:
        return repair.row_hash(con.execute("SELECT * FROM event WHERE id=?", (event_id,)).fetchone())

    merge_ids = (10, 11)
    candidates = [
        repair._candidate_projection(row)
        for row in con.execute(
            "SELECT * FROM smart_update_candidate_state WHERE accepted_event_id IN (?,?) OR diagnostic_event_id IN (?,?) ORDER BY id",
            (*merge_ids, *merge_ids),
        )
    ]
    sources = [
        repair._source_projection(row)
        for row in con.execute("SELECT * FROM event_source WHERE event_id IN (?,?) ORDER BY id", merge_ids)
    ]
    clusters = [
        {
            "cluster_id": "sos-replay",
            "relation": "SAME_EVENT",
            "canonical_id": 10,
            "obsolete_ids": [11],
            "reason": "veto_fallthrough",
            "confidence": 1.0,
            "evidence": ["manual replay adjudication", "same occurrence"],
            "conflicts": [],
            "expected_row_hashes": {"10": event_hash(10), "11": event_hash(11)},
            "anchors": {
                "10": {"title": "Canonical", "date": "2026-08-24", "time": "18:00"},
                "11": {"title": "Obsolete replay", "date": "2026-08-24", "time": "18:00"},
            },
            "source_policy": "move_unique_collapse_exact",
            "poster_policy": "move_preserve_graph",
            "expected_candidate_states": candidates,
            "expected_source_bindings": sources,
            "public_mapping": {"canonical_id": 10, "obsolete_ids": [11]},
        },
        {
            "cluster_id": "exhibition-vs-excursion",
            "relation": "KEEP_DISTINCT",
            "distinct_relation": "distinct_event",
            "canonical_id": 20,
            "obsolete_ids": [21],
            "reason": "different_occurrence_type",
            "confidence": 1.0,
            "evidence": ["separate admission and time"],
            "conflicts": ["event_type"],
            "expected_row_hashes": {"20": event_hash(20), "21": event_hash(21)},
            "anchors": {
                "20": {"title": "Exhibition", "date": "2026-08-25", "time": "10:00"},
                "21": {"title": "Excursion", "date": "2026-08-25", "time": "12:00"},
            },
            "public_mapping": {"canonical_id": 20, "obsolete_ids": [21]},
        },
    ]
    for cluster in clusters:
        cluster["expected_graph_sha256"] = repair.cluster_graph_hash(
            con, [cluster["canonical_id"], *cluster["obsolete_ids"]]
        )
        ids = [cluster["canonical_id"], *cluster["obsolete_ids"]]
        placeholders = ",".join("?" for _ in ids)
        cluster["expected_job_rows"] = [
            dict(row)
            for row in con.execute(
                f"SELECT * FROM joboutbox WHERE event_id IN ({placeholders}) ORDER BY id", ids
            )
        ]
        cluster["expected_event_publications"] = [
            dict(row)
            for row in con.execute(
                f"SELECT * FROM event_publication WHERE event_id IN ({placeholders}) ORDER BY id", ids
            )
        ]
    con.close()
    manifest = {
        "schema_version": 1,
        "incident": repair.INCIDENT,
        "prevention_sha": "a" * 40,
        "census": {"cutoff": "2026-08-22T15:00:00Z", "sha256": repair.census_hash("2026-08-22T15:00:00Z", clusters)},
        "clusters": clusters,
    }
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest


def _row_hash(db: Path, event_id: int) -> str:
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    value = repair.row_hash(con.execute("SELECT * FROM event WHERE id=?", (event_id,)).fetchone())
    con.close()
    return value


def _mixed_component_manifest(db: Path, path: Path) -> dict:
    """Build an admissions-style component with duplicate and family edges."""

    con = sqlite3.connect(db)
    con.execute(
        "INSERT INTO event(id,title,date,time) VALUES(22,'Admissions umbrella','2026-08-25','09:00')"
    )
    con.execute(
        "INSERT INTO event(id,title,date,time) VALUES(23,'Exhibition duplicate','2026-08-25','10:00')"
    )
    con.execute(
        "INSERT INTO event(id,title,date,time) VALUES(24,'Excursion duplicate','2026-08-25','12:00')"
    )
    con.executemany(
        "INSERT INTO event_source VALUES(?,?,?,?,?,?,?,?)",
        [
            (122, 22, "telegram", "https://t.me/source/22", "https://t.me/source/22", None, None, None),
            (123, 23, "telegram", "https://t.me/source/23", "https://t.me/source/23", None, None, None),
            (124, 24, "telegram", "https://t.me/source/24", "https://t.me/source/24", None, None, None),
        ],
    )
    con.commit()
    con.close()
    manifest = _manifest(db, path)
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    ids = [20, 21, 22, 23, 24]
    p = ",".join("?" for _ in ids)
    pairs = [
        {"left_id": 20, "right_id": 23, "relation": "MERGE", "canonical_id": 20},
        {"left_id": 21, "right_id": 24, "relation": "MERGE", "canonical_id": 21},
        {"left_id": 20, "right_id": 21, "relation": "KEEP_DISTINCT_RELATED"},
        {"left_id": 20, "right_id": 24, "relation": "KEEP_DISTINCT_RELATED"},
        {"left_id": 21, "right_id": 23, "relation": "KEEP_DISTINCT_RELATED"},
        {"left_id": 23, "right_id": 24, "relation": "KEEP_DISTINCT_RELATED"},
        {"left_id": 22, "right_id": 20, "relation": "PARENT_CHILD"},
        {"left_id": 22, "right_id": 21, "relation": "PARENT_CHILD"},
        {"left_id": 22, "right_id": 23, "relation": "PARENT_CHILD"},
        {"left_id": 22, "right_id": 24, "relation": "PARENT_CHILD"},
    ]
    component = {
        "component_id": "admissions_departments",
        "event_ids": ids,
        "pair_verdicts": pairs,
        "reason": "manual_component_adjudication",
        "confidence": 1.0,
        "evidence": ["department titles and source posts"],
        "conflicts": ["department scope or parent campaign role"],
        "expected_row_hashes": {
            str(event_id): repair.row_hash(
                con.execute("SELECT * FROM event WHERE id=?", (event_id,)).fetchone()
            )
            for event_id in ids
        },
        "anchors": {
            str(event_id): {
                "title": con.execute(
                    "SELECT title FROM event WHERE id=?", (event_id,)
                ).fetchone()[0]
            }
            for event_id in ids
        },
        "expected_graph_sha256": repair.cluster_graph_hash(con, ids),
        "source_policy": "move_unique_collapse_exact",
        "poster_policy": "move_preserve_graph",
        "expected_candidate_states": [],
        "expected_source_bindings": [
            repair._source_projection(row)
            for row in con.execute(
                f"SELECT * FROM event_source WHERE event_id IN ({p}) ORDER BY id", ids
            )
        ],
        "expected_job_rows": [
            dict(row)
            for row in con.execute(
                f"SELECT * FROM joboutbox WHERE event_id IN ({p}) ORDER BY id", ids
            )
        ],
        "expected_event_publications": [
            dict(row)
            for row in con.execute(
                f"SELECT * FROM event_publication WHERE event_id IN ({p}) ORDER BY id", ids
            )
        ],
    }
    con.close()
    manifest["clusters"][1] = component
    manifest["census"]["sha256"] = repair.census_hash(
        manifest["census"]["cutoff"], manifest["clusters"]
    )
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest


def test_dry_run_apply_preserves_graph_cancels_jobs_and_second_apply_is_exact_noop(tmp_path: Path) -> None:
    db = tmp_path / "fixture.sqlite"
    manifest_path = tmp_path / "manifest.json"
    _make_db(db)
    manifest = _manifest(db, manifest_path)
    before_bytes = db.read_bytes()
    distinct_before = {event_id: _row_hash(db, event_id) for event_id in (20, 21)}

    dry = repair.run(db, manifest_path)
    assert dry["status"] == "ready"
    assert dry["changed"] is False
    assert db.read_bytes() == before_bytes

    applied = repair.run(db, manifest_path, "apply")
    assert applied["status"] == "applied"
    assert applied["changed"] is True
    assert applied["verification"]["quick_check"] == "ok"
    assert applied["social_actions_performed"] is False
    assert applied["cleanup_mapping"][0]["obsolete"][0]["urls"]["tg_event_post_url"] == "https://t.me/kldevents/101"

    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    obsolete = con.execute("SELECT * FROM event WHERE id=11").fetchone()
    assert (obsolete["lifecycle_status"], obsolete["silent"], obsolete["identity_status"], obsolete["merged_into_event_id"]) == ("cancelled", 1, "merged", 10)
    assert obsolete["telegraph_url"] == "https://telegra.ph/obsolete"
    assert obsolete["ics_url"] == "https://ics/obsolete"
    assert con.execute("SELECT COUNT(*) FROM event").fetchone()[0] == 5
    assert con.execute("SELECT event_id FROM event_source WHERE id=101").fetchone()[0] == 10
    assert con.execute("SELECT COUNT(*) FROM event_source WHERE id=102").fetchone()[0] == 0
    assert tuple(con.execute("SELECT event_id,source_id FROM event_source_fact WHERE id=502").fetchone()) == (10, 100)
    assert tuple(con.execute("SELECT event_id,source_id FROM event_source_fact WHERE id=501").fetchone()) == (10, 101)
    assert con.execute("SELECT COUNT(*) FROM event_source_fact").fetchone()[0] == 3
    assert con.execute("SELECT source_id FROM event_identity_decision_log WHERE id=50").fetchone()[0] == 100
    assert con.execute("SELECT event_id FROM eventposter WHERE id=201").fetchone()[0] == 10
    assert tuple(con.execute("SELECT event_id,review_status,duplicate_of_id FROM eventposter WHERE id=202").fetchone()) == (11, "duplicate", 200)
    assert tuple(con.execute("SELECT event_id,review_status,duplicate_of_id FROM eventposter WHERE id=204").fetchone()) == (11, "duplicate", 203)
    assert tuple(con.execute("SELECT status,last_error FROM joboutbox WHERE id=600").fetchone())[0] == "error"
    assert con.execute("SELECT status FROM joboutbox WHERE id=601").fetchone()[0] == "done"
    assert tuple(con.execute("SELECT accepted_event_id,current_outcome FROM smart_update_candidate_state WHERE id=300").fetchone()) == (10, "MERGED")
    distinct_review = con.execute(
        "SELECT event_id,candidate_event_id,decision,decision_payload "
        "FROM event_identity_decision_log WHERE decision='FINAL_DISTINCT'"
    ).fetchone()
    assert distinct_review is not None
    assert tuple(distinct_review[:3]) == (20, 21, "FINAL_DISTINCT")
    distinct_payload = json.loads(str(distinct_review[3]))
    assert distinct_payload["stage"] == "manual_pair_review_v1"
    assert distinct_payload["evidence"] == ["separate admission and time"]
    assert distinct_payload["blocking_conflicts"] == ["event_type"]
    # Append-only attempt history and diagnostic evidence are not repointed.
    assert tuple(con.execute("SELECT accepted_event_id,terminal_outcome FROM smart_update_attempt WHERE id=400").fetchone()) == (11, "CREATED")
    assert con.execute("SELECT diagnostic_event_id FROM smart_update_candidate_state WHERE id=301").fetchone()[0] == 11
    assert con.execute("SELECT linked_event_ids FROM event WHERE id=30").fetchone()[0] == "[10]"
    assert con.execute("SELECT COUNT(*) FROM event_identity_decision_log WHERE decision='repair_merge'").fetchone()[0] == 1
    con.close()

    # KEEP_DISTINCT is census evidence only and remains byte-stable at row level.
    assert {event_id: _row_hash(db, event_id) for event_id in (20, 21)} == distinct_before
    second = repair.run(db, manifest_path, "apply")
    assert second["status"] == "noop"
    assert second["changed"] is False
    assert second["diff"] == []
    verified = repair.run(db, manifest_path, "verify")
    assert verified["status"] == "verified"
    assert verified["diff"] == []
    assert manifest["prevention_sha"] == applied["prevention_sha"]


def test_stale_row_hash_refuses_before_any_write(tmp_path: Path) -> None:
    db = tmp_path / "fixture.sqlite"
    manifest_path = tmp_path / "manifest.json"
    _make_db(db)
    _manifest(db, manifest_path)
    con = sqlite3.connect(db)
    con.execute("UPDATE event SET title='Operator edit' WHERE id=11")
    con.commit()
    con.close()
    before = db.read_bytes()

    with pytest.raises(repair.RepairBlocked, match="event_row_hash_mismatch:sos-replay:11"):
        repair.run(db, manifest_path, "apply")
    assert db.read_bytes() == before


def test_running_job_blocks_all_writes(tmp_path: Path) -> None:
    db = tmp_path / "fixture.sqlite"
    manifest_path = tmp_path / "manifest.json"
    _make_db(db)
    con = sqlite3.connect(db)
    con.execute(
        "INSERT INTO joboutbox(id,event_id,task,status,attempts,payload,coalesce_key,updated_at,next_run_at) "
        "VALUES(699,11,'telegraph_build','running',0,'null','telegraph:running','2026-08-22T10:00:00Z','2026-08-22T10:00:00Z')"
    )
    con.commit()
    con.close()
    _manifest(db, manifest_path)
    before = db.read_bytes()

    with pytest.raises(repair.RepairBlocked, match="affected_job_running:699"):
        repair.run(db, manifest_path, "apply")
    assert db.read_bytes() == before


def test_scheduler_timestamp_only_drift_is_observed_and_apply_is_allowed(tmp_path: Path) -> None:
    db = tmp_path / "fixture.sqlite"
    manifest_path = tmp_path / "manifest.json"
    _make_db(db)
    _manifest(db, manifest_path)
    con = sqlite3.connect(db)
    con.execute(
        "UPDATE joboutbox SET updated_at=?,next_run_at=? WHERE id=600",
        ("2026-08-22T11:00:00Z", "2026-08-22T11:05:00Z"),
    )
    con.commit()
    con.close()

    applied = repair.run(db, manifest_path, "apply")
    assert applied["status"] == "applied"
    assert applied["observed_job_timestamp_drift"] == [
        {
            "id": 600,
            "expected": {
                "updated_at": "2026-08-22T10:00:00Z",
                "next_run_at": "2026-08-22T10:05:00Z",
            },
            "observed": {
                "updated_at": "2026-08-22T11:00:00Z",
                "next_run_at": "2026-08-22T11:05:00Z",
            },
        }
    ]
    con = sqlite3.connect(db)
    stored = json.loads(
        con.execute(
            f"SELECT observed_job_timestamp_drift_json FROM {repair.RECEIPT_TABLE}"
        ).fetchone()[0]
    )
    con.close()
    assert stored == applied["observed_job_timestamp_drift"]
    con = sqlite3.connect(db)
    con.execute(
        "UPDATE joboutbox SET updated_at='2026-08-22T12:00:00Z',"
        "next_run_at='2026-08-22T12:05:00Z' WHERE id=600"
    )
    con.commit()
    con.close()
    second = repair.run(db, manifest_path, "apply")
    assert second["status"] == "noop"
    assert second["changed"] is False
    assert second["diff"] == []


@pytest.mark.parametrize(
    ("assignment", "expected"),
    [
        ("status='error'", "job_semantic_constraint_mismatch"),
        ("attempts=attempts+1", "job_semantic_constraint_mismatch"),
        ("last_result='new result'", "job_semantic_constraint_mismatch"),
        ("last_error='new error'", "job_semantic_constraint_mismatch"),
        ("payload='{\"event_id\":999}'", "job_semantic_constraint_mismatch"),
        ("depends_on=999", "job_semantic_constraint_mismatch"),
    ],
)
def test_job_semantic_drift_blocks_apply(
    tmp_path: Path, assignment: str, expected: str
) -> None:
    db = tmp_path / "fixture.sqlite"
    manifest_path = tmp_path / "manifest.json"
    _make_db(db)
    _manifest(db, manifest_path)
    con = sqlite3.connect(db)
    con.execute(f"UPDATE joboutbox SET {assignment} WHERE id=600")
    con.commit()
    con.close()

    with pytest.raises(repair.RepairBlocked, match=expected):
        repair.run(db, manifest_path, "apply")


def test_event_publication_ownership_drift_blocks_apply(tmp_path: Path) -> None:
    db = tmp_path / "fixture.sqlite"
    manifest_path = tmp_path / "manifest.json"
    _make_db(db)
    _manifest(db, manifest_path)
    con = sqlite3.connect(db)
    con.execute("UPDATE event_publication SET event_id=10 WHERE id=701")
    con.commit()
    con.close()

    with pytest.raises(repair.RepairBlocked, match="event_publication_constraint_mismatch"):
        repair.run(db, manifest_path, "apply")


def test_poster_hash_and_raw_identity_disagreement_fails_closed(tmp_path: Path) -> None:
    db = tmp_path / "fixture.sqlite"
    manifest_path = tmp_path / "manifest.json"
    _make_db(db)
    con = sqlite3.connect(db)
    # The obsolete row's derived hash points to canonical poster 203 while its
    # raw bytes point to canonical poster 200.  The repair must not pick one.
    con.execute("UPDATE eventposter SET raw_sha256=NULL WHERE id=202")
    con.execute(
        "INSERT INTO eventposter(id,event_id,poster_hash,raw_sha256,review_status) VALUES(205,11,'canonical-derived','raw-shared','approved')"
    )
    con.commit()
    con.close()
    _manifest(db, manifest_path)

    with pytest.raises(
        repair.RepairBlocked,
        match="poster_identity_collision_ambiguous:sos-replay:205:200,203",
    ):
        repair.run(db, manifest_path, "apply")

    con = sqlite3.connect(db)
    assert con.execute("SELECT event_id,review_status,duplicate_of_id FROM eventposter WHERE id=205").fetchone() == (11, "approved", None)
    assert con.execute("SELECT COUNT(*) FROM event_identity_decision_log WHERE decision='repair_merge'").fetchone()[0] == 0
    assert con.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    con.close()


@pytest.mark.parametrize("surface", ["candidate", "occurrence"])
def test_exact_candidate_and_occurrence_constraints_refuse_drift(tmp_path: Path, surface: str) -> None:
    db = tmp_path / "fixture.sqlite"
    manifest_path = tmp_path / "manifest.json"
    _make_db(db)
    _manifest(db, manifest_path)
    con = sqlite3.connect(db)
    if surface == "candidate":
        con.execute("UPDATE smart_update_candidate_state SET occurrence_key='drift' WHERE id=300")
    else:
        con.execute("UPDATE event_source SET occurrence_key='drift' WHERE id=101")
    con.commit()
    con.close()
    before = db.read_bytes()
    expected = "candidate_state_constraint_mismatch" if surface == "candidate" else "source_occurrence_constraint_mismatch"
    with pytest.raises(repair.RepairBlocked, match=expected):
        repair.run(db, manifest_path, "apply")
    assert db.read_bytes() == before


def test_rollback_restores_rows_and_refuses_post_apply_drift(tmp_path: Path) -> None:
    db = tmp_path / "fixture.sqlite"
    manifest_path = tmp_path / "manifest.json"
    _make_db(db)
    _manifest(db, manifest_path)
    repair.run(db, manifest_path, "apply")

    drifted = tmp_path / "drifted.sqlite"
    drifted.write_bytes(db.read_bytes())
    con = sqlite3.connect(drifted)
    con.execute("UPDATE event SET title='Post repair operator edit' WHERE id=11")
    con.commit()
    con.close()
    with pytest.raises(repair.RepairBlocked, match="rollback_cas_mismatch"):
        repair.run(drifted, manifest_path, "rollback")

    ledger_drifted = tmp_path / "ledger-drifted.sqlite"
    ledger_drifted.write_bytes(db.read_bytes())
    con = sqlite3.connect(ledger_drifted)
    con.execute(
        "UPDATE event_identity_decision_log SET decision_payload='{}' "
        "WHERE decision='FINAL_DISTINCT'"
    )
    con.commit()
    con.close()
    with pytest.raises(repair.RepairBlocked, match="verification_cas_mismatch"):
        repair.run(ledger_drifted, manifest_path, "verify")
    with pytest.raises(repair.RepairBlocked, match="rollback_cas_mismatch"):
        repair.run(ledger_drifted, manifest_path, "rollback")

    rolled_back = repair.run(db, manifest_path, "rollback")
    assert rolled_back["status"] == "rolled_back"
    con = sqlite3.connect(db)
    assert con.execute("SELECT lifecycle_status,silent,identity_status,merged_into_event_id FROM event WHERE id=11").fetchone() == ("active", 0, "canonical", None)
    assert con.execute("SELECT event_id FROM event_source WHERE id=101").fetchone()[0] == 11
    assert con.execute("SELECT event_id FROM event_source WHERE id=102").fetchone()[0] == 11
    assert con.execute("SELECT event_id,source_id FROM event_source_fact WHERE id=502").fetchone() == (11, 102)
    assert con.execute("SELECT source_id FROM event_identity_decision_log WHERE id=50").fetchone()[0] == 102
    assert con.execute("SELECT event_id,review_status,duplicate_of_id FROM eventposter WHERE id=202").fetchone() == (11, "approved", None)
    assert con.execute("SELECT event_id,review_status,duplicate_of_id FROM eventposter WHERE id=204").fetchone() == (11, "approved", None)
    assert con.execute("SELECT status,last_error FROM joboutbox WHERE id=600").fetchone() == ("pending", None)
    assert con.execute("SELECT accepted_event_id,current_outcome FROM smart_update_candidate_state WHERE id=300").fetchone() == (11, "CREATED")
    assert con.execute("SELECT linked_event_ids FROM event WHERE id=30").fetchone()[0] == "[11]"
    assert con.execute("SELECT COUNT(*) FROM event_identity_decision_log WHERE decision='repair_merge'").fetchone()[0] == 0
    assert con.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    con.close()


def test_manifest_cross_cluster_and_census_hash_are_fail_closed(tmp_path: Path) -> None:
    db = tmp_path / "fixture.sqlite"
    manifest_path = tmp_path / "manifest.json"
    _make_db(db)
    manifest = _manifest(db, manifest_path)
    broken = copy.deepcopy(manifest)
    broken["clusters"][1]["canonical_id"] = 11
    broken["clusters"][1]["expected_row_hashes"] = {"11": broken["clusters"][0]["expected_row_hashes"]["11"], "21": broken["clusters"][1]["expected_row_hashes"]["21"]}
    broken["clusters"][1]["anchors"] = {"11": broken["clusters"][0]["anchors"]["11"], "21": broken["clusters"][1]["anchors"]["21"]}
    broken["census"]["sha256"] = repair.census_hash(broken["census"]["cutoff"], broken["clusters"])
    manifest_path.write_text(json.dumps(broken), encoding="utf-8")
    with pytest.raises(repair.RepairBlocked, match="cross_cluster_event_id:11"):
        repair.run(db, manifest_path)

    broken = copy.deepcopy(manifest)
    broken["census"]["sha256"] = "0" * 64
    manifest_path.write_text(json.dumps(broken), encoding="utf-8")
    with pytest.raises(repair.RepairBlocked, match="census_hash_mismatch"):
        repair.run(db, manifest_path)


def test_mixed_component_executes_merges_and_only_explicit_pair_decisions(
    tmp_path: Path,
) -> None:
    db = tmp_path / "fixture.sqlite"
    manifest_path = tmp_path / "manifest.json"
    _make_db(db)
    _mixed_component_manifest(db, manifest_path)

    dry = repair.run(db, manifest_path)
    assert dry["status"] == "ready"
    assert sum(item["action"] == "merge" for item in dry["diff"]) == 3
    applied = repair.run(db, manifest_path, "apply")
    assert applied["status"] == "applied"
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    assert tuple(
        con.execute(
            "SELECT identity_status,merged_into_event_id FROM event WHERE id=23"
        ).fetchone()
    ) == ("merged", 20)
    assert tuple(
        con.execute(
            "SELECT identity_status,merged_into_event_id FROM event WHERE id=24"
        ).fetchone()
    ) == ("merged", 21)
    rows = con.execute(
        "SELECT event_id,candidate_event_id,decision_payload "
        "FROM event_identity_decision_log WHERE decision='FINAL_DISTINCT' "
        "AND json_extract(decision_payload,'$.component_id')='admissions_departments'"
    ).fetchall()
    assert len(rows) == 8
    relations = [json.loads(row["decision_payload"])["relation"] for row in rows]
    assert relations.count("related_but_distinct") == 4
    assert relations.count("parent_child") == 4
    assert {(row["event_id"], row["candidate_event_id"]) for row in rows}.isdisjoint(
        {(20, 23), (21, 24)}
    )
    con.close()
    second = repair.run(db, manifest_path, "apply")
    assert second["status"] == "noop"
    assert second["changed"] is False
    assert second["diff"] == []


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        ("drop", "pair_coverage_incomplete"),
        ("reverse_duplicate", "pair_verdict_duplicate"),
        ("bad_canonical", "pair_merge_canonical_invalid"),
        ("unsafe_order", "pair_merge_execution_order_unsafe"),
    ],
)
def test_mixed_component_pair_contract_is_complete_unique_and_canonical(
    tmp_path: Path, mutation: str, expected: str
) -> None:
    db = tmp_path / "fixture.sqlite"
    manifest_path = tmp_path / "manifest.json"
    _make_db(db)
    manifest = _mixed_component_manifest(db, manifest_path)
    pairs = manifest["clusters"][1]["pair_verdicts"]
    if mutation == "drop":
        pairs.pop()
    elif mutation == "reverse_duplicate":
        duplicate = copy.deepcopy(pairs[0])
        duplicate["left_id"], duplicate["right_id"] = (
            duplicate["right_id"],
            duplicate["left_id"],
        )
        pairs.append(duplicate)
    else:
        if mutation == "bad_canonical":
            pairs[0]["canonical_id"] = 21
        else:
            pairs[2] = {
                "left_id": 20,
                "right_id": 21,
                "relation": "MERGE",
                "canonical_id": 20,
            }
    manifest["census"]["sha256"] = repair.census_hash(
        manifest["census"]["cutoff"], manifest["clusters"]
    )
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(repair.RepairBlocked, match=expected):
        repair.run(db, manifest_path)
