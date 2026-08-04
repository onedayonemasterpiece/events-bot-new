from __future__ import annotations

import importlib.util
import hashlib
from pathlib import Path
import sqlite3

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "ops" / "repair_smart_update_identity_20260804.py"
SPEC = importlib.util.spec_from_file_location("repair_smart_identity_20260804", SCRIPT)
assert SPEC and SPEC.loader
repair = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(repair)


def _make_db(path: Path) -> None:
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE event (
          id INTEGER PRIMARY KEY, title TEXT, date TEXT, time TEXT, ticket_link TEXT,
          end_date TEXT, location_name TEXT, location_address TEXT, city TEXT,
          description TEXT, short_description TEXT, search_digest TEXT,
          event_type TEXT, topics TEXT, festival TEXT,
          source_text TEXT, source_texts TEXT, telegraph_url TEXT, telegraph_path TEXT,
          ics_url TEXT, is_free INTEGER, ticket_price_min INTEGER, ticket_price_max INTEGER, ticket_status TEXT,
          photo_urls TEXT, photo_count INTEGER, preview_3d_url TEXT, silent INTEGER DEFAULT 0,
          age_restriction TEXT, age_restriction_status TEXT,
          age_restriction_provenance TEXT, age_restriction_source_url TEXT,
          age_restriction_confidence REAL
        );
        CREATE TABLE event_source (
          id INTEGER PRIMARY KEY, event_id INTEGER NOT NULL, source_type TEXT NOT NULL,
          source_url TEXT NOT NULL, source_text TEXT, canonical_source_url TEXT,
          source_role TEXT, source_fingerprint TEXT
        );
        CREATE TABLE event_source_fact (
          id INTEGER PRIMARY KEY, event_id INTEGER NOT NULL, source_id INTEGER NOT NULL,
          fact TEXT
        );
        CREATE TABLE eventposter (
          id INTEGER PRIMARY KEY, event_id INTEGER NOT NULL,
          review_status TEXT, review_reason TEXT, duplicate_of_id INTEGER,
          supabase_url TEXT, raw_sha256 TEXT, display_order INTEGER
        );
        CREATE TABLE joboutbox (
          id INTEGER PRIMARY KEY, event_id INTEGER, task TEXT, status TEXT
        );
        CREATE TABLE event_identity_decision_log (
          id INTEGER PRIMARY KEY, event_id INTEGER, source_type TEXT, source_url TEXT,
          decision TEXT, decision_reason TEXT, decision_payload TEXT
        );
        """
    )
    events = [
        (3216, "Великие учителя", "2026-04-09", "", "ticket:exhibition"),
        (3864, "Концерт Константина Хачикяна", "2026-08-07", "20:00", repair.PIANISSIMO_URL),
        (7024, "Иржи Килиан: Кагуя – лунная принцесса", "2026-08-09", "17:00", repair.KAGUYA_URL),
        (7244, "Право женщин на море", "2026-08-09", "14:00", repair.WOMEN_SEA_URL),
        (7435, "Встреча Хочу машину", "2026-08-08", "16:00", None),
        (7151, "Negative control", "2026-08-05", "19:00", "ticket:7151"),
    ]
    for event_id, title, date, time, ticket in events:
        con.execute(
            "INSERT INTO event(id,title,date,time,ticket_link,end_date,location_name,location_address,city,description,short_description,search_digest,event_type,topics,festival,source_text,source_texts,telegraph_url,telegraph_path,ics_url,is_free,ticket_price_min,ticket_price_max,ticket_status,photo_urls,photo_count,preview_3d_url,silent,age_restriction_status) VALUES(?,?,?,?,?,NULL,'Филиал Третьяковской галереи, Парадная наб. 3, Калининград','Парадная наб. 3','Калининград','direct description','direct short','direct digest','other','[]',NULL,'legacy','[]',?,?,?,0,1500,2000,'available','[]',0,'preview',0,'conflict')",
            (event_id, title, date, time, ticket, f"https://telegra.ph/{event_id}", str(event_id), f"https://ics/{event_id}"),
        )
    con.execute("UPDATE event SET telegraph_path='Velikie-uchitelya-04-13' WHERE id=3864")
    con.execute("UPDATE event SET description='Pianissimo concert', short_description='Pianissimo', search_digest='Pianissimo Khachikyan', event_type='концерт', topics='[\"CONCERTS\",\"EXHIBITIONS\"]', festival='Pianissimo', age_restriction='12+' WHERE id=3864")
    repair.PIANISSIMO_TEXT_HASHES = {
        "description": hashlib.sha256(b"Pianissimo concert").hexdigest(),
        "short_description": hashlib.sha256(b"Pianissimo").hexdigest(),
        "search_digest": hashlib.sha256(b"Pianissimo Khachikyan").hexdigest(),
    }
    con.execute(
        "UPDATE event SET age_restriction_source_url=? WHERE id=7024",
        (repair.WOMEN_SEA_URL,),
    )
    con.execute("UPDATE event SET is_free=1 WHERE id=7244")
    con.execute(
        "INSERT INTO event_identity_decision_log(id,event_id,source_type,source_url,decision,decision_reason,decision_payload) VALUES(2979,7435,'telegram',?,'allow_merge','same_event_update','{\"relation\":\"same_event\",\"blocking_conflicts\":[]}')",
        (repair.SIGNAL_TG_URL,),
    )

    expected_events = {
        **{sid: 3864 for sid in repair.DUPLICATE_SOURCES},
        **{sid: 3864 for sid in repair.QUARANTINE_DELETE_SOURCES},
        9404562: 7024,
        **repair.IDENTITY_SOURCES,
        **repair.CONTEXT_SOURCES,
    }
    expected_events[713970] = 3864
    source_urls = {
        2377812: repair.PIANISSIMO_URL,
        9404476: repair.KAGUYA_URL,
        9404562: repair.WOMEN_SEA_URL,
        9573730: repair.WOMEN_SEA_URL,
        10971967: repair.URBAN_CONTEXT_URL,
        10949059: repair.SIGNAL_VK_URL,
        10971966: repair.SIGNAL_TG_URL,
        8618622: repair.URBAN_CONTEXT_URL,
        9404519: repair.URBAN_CONTEXT_URL,
        9745653: repair.URBAN_CONTEXT_URL,
    }
    for source_id, event_id in expected_events.items():
        url = source_urls.get(source_id, f"https://source.invalid/{source_id}")
        con.execute(
            "INSERT INTO event_source(id,event_id,source_type,source_url,source_text) VALUES(?,?,?,?,?)",
            (source_id, event_id, "parser:test", url, f"public event source {source_id}"),
        )
    # Exact destination duplicates are the deletion precondition.
    for offset, (source_id, destination) in enumerate(repair.DUPLICATE_SOURCES.items(), 1):
        url = con.execute("SELECT source_url FROM event_source WHERE id=?", (source_id,)).fetchone()[0]
        con.execute(
            "INSERT INTO event_source(id,event_id,source_type,source_url,source_text) VALUES(?,?,?,?,?)",
            (20_000_000 + offset, destination, "parser:test", url, "canonical duplicate"),
        )
    for idx, source_id in enumerate(sorted(repair.TOUCHED_SOURCE_IDS), 1):
        if source_id == 10971967:
            continue
        con.execute(
            "INSERT INTO event_source_fact(id,event_id,source_id,fact) SELECT ?,event_id,id,'grounded fact' FROM event_source WHERE id=?",
            (30_000_000 + idx, source_id),
        )
    extra_fact_id = 31_000_000
    for source_id, extra_count in ((713970, 15), (9404562, 8)):
        event_id = con.execute("SELECT event_id FROM event_source WHERE id=?", (source_id,)).fetchone()[0]
        for _ in range(extra_count):
            con.execute(
                "INSERT INTO event_source_fact(id,event_id,source_id,fact) VALUES(?,?,?,'grounded fact')",
                (extra_fact_id, event_id, source_id),
            )
            extra_fact_id += 1
    for fact_id in repair.PRICE_FACT_IDS:
        con.execute(
            "INSERT INTO event_source_fact(id,event_id,source_id,fact) VALUES(?,7244,9573730,'derived price')",
            (fact_id,),
        )
    poster_events = {
        **{poster_id: 3864 for poster_id in repair.MOVE_POSTERS},
        **{poster_id: 3864 for poster_id in repair.REJECT_POSTERS_3864},
        15555: 7024,
        15915: 7024,
        16554: 7244,
        17132: 7244,
        18588: 7244,
    }
    for poster_id, event_id in poster_events.items():
        con.execute(
            "INSERT INTO eventposter(id,event_id,review_status,supabase_url,raw_sha256,display_order) VALUES(?,?,'pending',?,?,0)",
            (poster_id, event_id, f"https://static.invalid/{poster_id}.webp", f"sha-{poster_id}"),
        )
    con.execute(
        "INSERT INTO eventposter(id,event_id,review_status,duplicate_of_id,supabase_url,raw_sha256,display_order) VALUES(15214,3864,'duplicate',14180,'https://static.invalid/15214.webp',?,24)",
        (repair.RETAIN_CONCERT_POSTER_SHA256,),
    )
    con.commit()
    con.close()


def _db_bytes(path: Path) -> bytes:
    return path.read_bytes()


def test_dry_apply_second_apply_and_verify(tmp_path: Path) -> None:
    db = tmp_path / "fixture.sqlite"
    _make_db(db)
    before = _db_bytes(db)

    dry = repair.run(str(db), "dry-run")
    assert dry["status"] == "ready"
    assert dry["changed"] is False
    assert _db_bytes(db) == before

    with pytest.raises(repair.RepairBlocked, match="manual_confirmation_required"):
        repair.run(str(db), "apply")
    applied = repair.run(str(db), "apply", confirm_7244_free=True)
    assert applied["status"] == "applied"
    assert applied["changed"] is True
    assert applied["verification"]["quick_check"] == "ok"

    con = sqlite3.connect(db)
    assert con.execute("SELECT COUNT(*) FROM event_source WHERE id IN (%s)" % ",".join("?" for _ in repair.DELETE_SOURCES), tuple(sorted(repair.DELETE_SOURCES))).fetchone()[0] == 0
    assert con.execute("SELECT event_id FROM event_source WHERE id=713970").fetchone()[0] == 3216
    assert con.execute("SELECT source_role FROM event_source WHERE id=10971967").fetchone()[0] == "context_only"
    assert con.execute("SELECT title FROM event WHERE id=7151").fetchone()[0] == "Negative control"
    assert con.execute("SELECT topics FROM event WHERE id=3864").fetchone()[0] == '["CONCERTS"]'
    assert con.execute("SELECT photo_count FROM event WHERE id=3864").fetchone()[0] == 1
    assert con.execute("SELECT telegraph_path FROM event WHERE id=3216").fetchone()[0] == "Velikie-uchitelya-04-13"
    assert con.execute("SELECT COUNT(*) FROM event WHERE id IN (3216,3864,7024,7244) AND silent=1").fetchone()[0] == 4
    con.close()

    second = repair.run(str(db), "apply")
    assert second["status"] == "noop"
    assert second["changed"] is False
    verified = repair.run(str(db), "verify")
    assert verified["status"] == "verified"
    assert verified["verification"]["quarantine_released"] is False

    con = sqlite3.connect(db)
    con.execute(
        "UPDATE event SET telegraph_url='https://telegra.ph/Pianissimo-Khachikyan', telegraph_path='Pianissimo-Khachikyan' WHERE id=3864"
    )
    con.commit()
    con.close()
    with pytest.raises(repair.RepairBlocked, match="manual_confirmation_required"):
        repair.run(str(db), "release")
    released = repair.run(str(db), "release", confirm_surfaces_ready=True)
    assert released["status"] == "released"
    assert released["verification"]["quarantine_released"] is True

    rolled_back = repair.run(str(db), "rollback")
    assert rolled_back["status"] == "rolled_back"
    con = sqlite3.connect(db)
    assert con.execute("SELECT title FROM event WHERE id=3864").fetchone()[0] == "Концерт Константина Хачикяна"
    assert con.execute("SELECT COUNT(*) FROM event_source WHERE id IN (%s)" % ",".join("?" for _ in repair.DELETE_SOURCES), tuple(sorted(repair.DELETE_SOURCES))).fetchone()[0] == len(repair.DELETE_SOURCES)
    con.close()


def test_precondition_mismatch_is_no_write(tmp_path: Path) -> None:
    db = tmp_path / "fixture.sqlite"
    _make_db(db)
    con = sqlite3.connect(db)
    con.execute("UPDATE event SET time='18:00' WHERE id=7024")
    con.commit()
    con.close()
    before = _db_bytes(db)

    with pytest.raises(repair.RepairBlocked, match="event_anchor_mismatch:7024"):
        repair.run(str(db), "apply", confirm_7244_free=True)
    assert _db_bytes(db) == before


def test_rollback_cas_mismatch_is_no_write(tmp_path: Path) -> None:
    db = tmp_path / "fixture.sqlite"
    _make_db(db)
    repair.run(str(db), "apply", confirm_7244_free=True)
    con = sqlite3.connect(db)
    con.execute("UPDATE event SET search_digest='concurrent operator edit' WHERE id=3864")
    con.commit()
    con.close()
    before = _db_bytes(db)
    with pytest.raises(repair.RepairBlocked, match="rollback_cas_mismatch"):
        repair.run(str(db), "rollback")
    assert _db_bytes(db) == before
