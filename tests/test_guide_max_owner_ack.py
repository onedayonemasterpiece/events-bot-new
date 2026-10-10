"""Owner acknowledgement is terminal but never provider-verified publication."""
import copy
import json

import pytest

from guide_excursions import max_digest as md, visual_digest as vd
from guide_excursions.max_delivery import prepare_delivery, drive_delivery, get_delivery, _record
from guide_excursions.vibepublish_client import VibePublishClient
from test_guide_max_delivery import DB, Transport
from test_guide_max_pipeline import seed, enable, ROWS, TG, VK

PAYLOAD = {"alias": "max_guide", "native_id": "native-123", "binding_revision": 1}


def receipt(release="done"):
    return {"operation_id": "op_original", "resource_id": "pub_original", "revision": 1,
            "action": "publish", "state": "outcome_unknown", "operation_complete": True,
            "error": {"code": "max_outcome_unknown"},
            "deliveries": [{"destination": "max_guide", "provider": "max", "state": "outcome_unknown",
                            "observed": "unknown", "attempt_id": "attempt_original"}],
            "adjudication": {"adjudication_id": "adj_original", "disposition": "owner_confirmed_present",
                "original_operation_id": "op_original", "attempt_id": "attempt_original",
                "destination": "max_guide", "native_target": "native-123", "native_id": "PostSlug",
                "item_ref": "item_observed", "url": "https://max.ru/channel_uh_kaliningrad/PostSlug",
                "media_sha256": ["a" * 64], "recorded_at": "2026-10-10T15:00:00Z",
                "quarantine_release": release, "original_outcome": "outcome_unknown", "replay_allowed": False}}


def client():
    return VibePublishClient(PAYLOAD, base_url="https://vibe.example", token="fixture")


def test_owner_acknowledgement_is_not_provider_verified():
    result = client()._publication_result({"receipts": [receipt()]})
    assert result["state"] == "owner_confirmed_present"
    assert result["error_code"] == "max_outcome_unknown"
    assert result["receipt"]["verification"] == "owner_confirmed_present"
    assert result["receipt"]["adjudication"]["original_outcome"] == "outcome_unknown"


@pytest.mark.parametrize("field,value", [
    ("original_operation_id", "op_other"), ("attempt_id", "attempt_other"),
    ("destination", "max_other"), ("native_target", "other"),
    ("native_id", "OtherSlug"), ("disposition", "published"),
    ("replay_allowed", True), ("quarantine_release", "unknown"),
    ("original_outcome", "verified"), ("media_sha256", []),
    ("media_sha256", ["not-a-hash"]), ("url", "https://evil.example/PostSlug"),
    ("url", "https://max.ru:invalid/channel/PostSlug"),
    ("url", "https://max.ru/c/wrong-target/PostSlug"),
    ("url", "https://max.ru/channel/extra/PostSlug"),
    ("url", "https://max.ru/channel/%50ostSlug"),
    ("url", "https://max.ru/channel/PostSlug/"),
    ("recorded_at", "2026-10-10"), ("adjudication_id", "../unsafe"),
])
def test_mismatched_or_malformed_adjudication_stays_unknown(field, value):
    data = receipt()
    data["adjudication"][field] = value
    assert client()._publication_result(data)["state"] == "outcome_unknown"


def test_delivery_attempt_and_single_destination_are_required():
    for mutate in (lambda r: r["deliveries"][0].pop("attempt_id"),
                   lambda r: r["deliveries"].append(copy.deepcopy(r["deliveries"][0])),
                   lambda r: r["adjudication"].update(extra="unexpected")):
        data = receipt()
        mutate(data)
        assert client()._publication_result(data)["state"] == "outcome_unknown"


@pytest.mark.asyncio
async def test_pending_release_observes_only_then_terminal_survives_restart(tmp_path):
    db = DB(tmp_path / "ack.sqlite")
    row = await prepare_delivery(db, issue_id=289, provider="max", target_id="native-123", payload=PAYLOAD)
    transport = Transport()
    transport.response = {"state": "outcome_unknown", "operation_id": "op_original",
                          "error_code": "max_outcome_unknown"}
    await drive_delivery(db, row, transport)
    adapter = client()
    calls = []
    data = receipt("pending")
    async def request(method, path, **kwargs):
        calls.append((method, path))
        assert method == "GET"
        return {"receipts": [data]}
    adapter._request = request
    first = await drive_delivery(db, row, adapter)
    assert first["state"] == "outcome_unknown"
    assert first["receipt"]["adjudication"]["quarantine_release"] == "pending"
    data["adjudication"]["quarantine_release"] = "done"
    second = await drive_delivery(db, row, adapter)
    assert second["state"] == "owner_confirmed_present"
    assert second["error_code"] == "max_outcome_unknown"
    before = len(calls)
    assert (await drive_delivery(DB(db.path), row, adapter))["state"] == "owner_confirmed_present"
    assert len(calls) == before
    # A late concurrent observer cannot erase the terminal owner decision.
    assert (await _record(db, row, state="failed", error_code="late"))["state"] == "owner_confirmed_present"
    assert len(transport.submitted) == 1


@pytest.mark.asyncio
async def test_native_entrypoint_owner_ack_preserves_edition_and_siblings(tmp_path, monkeypatch):
    db = DB(tmp_path / "native.sqlite")
    await seed(db)
    enable(monkeypatch)
    row = await prepare_delivery(db, issue_id=289, provider="max", target_id="native-123", payload=PAYLOAD)
    transport = Transport()
    transport.response = {"state": "outcome_unknown", "operation_id": "op_original"}
    await drive_delivery(db, row, transport)
    adapter = client()
    calls = []
    async def request(method, path, **kwargs):
        calls.append(method)
        assert method == "GET"
        return {"receipts": [receipt()]}
    adapter._request = request
    async def load(*args, **kwargs): return {"id": 289, "items": ROWS}
    async def no_schema(*args): pass
    async def snapshot(*args): return {"content": {"paragraphs": [[{"kind": "text", "text": "Frozen"}]]}}
    async def forbidden(*args, **kwargs): raise AssertionError("No new edition or sibling send")
    monkeypatch.setattr(vd, "load_visual_digest_issue", load)
    monkeypatch.setattr(vd, "ensure_visual_digest_schema", no_schema)
    monkeypatch.setattr(vd, "build_visual_digest_issue", forbidden)
    monkeypatch.setattr(vd, "_publish_visual_digest_to_vk_once", forbidden)
    monkeypatch.setattr(vd, "VISUAL_DIGEST_TG_TARGET_CHATS", ["@youwillsee39"])
    monkeypatch.setattr(md, "get_snapshot", snapshot)
    monkeypatch.setattr(md, "VibePublishClient", lambda payload=None: adapter)
    class Bot:
        send_photo = forbidden
    first = await vd.publish_visual_digest_daily(db, Bot(), vk_group_id=238875824, resume_issue_id=289)
    second = await vd.publish_visual_digest_daily(db, Bot(), vk_group_id=238875824, resume_issue_id=289)
    assert first["complete"] and second["complete"]
    assert first["state"] == second["state"] == "complete_with_owner_confirmation"
    assert first["max"]["state"] == "owner_confirmed_present"
    assert not first["max"]["published"]
    assert calls == ["GET"]
    async with db.raw_conn() as conn:
        targets = json.loads((await (await conn.execute("SELECT published_targets_json FROM guide_digest_issue WHERE id=289")).fetchone())[0])
    assert targets == {"tg:@youwillsee39:visual": TG, "vk:uhtykaliningrad:visual": VK}
    stored = await get_delivery(db, row["request_key"])
    assert stored["payload"] == PAYLOAD
    assert stored["receipt"]["adjudication"]["replay_allowed"] is False
