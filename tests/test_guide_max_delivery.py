from contextlib import asynccontextmanager
import asyncio
import json

import aiosqlite
import pytest

from guide_excursions.max_delivery import prepare_delivery, drive_delivery, get_delivery


class DB:
    def __init__(self, path):
        self.path = path

    @asynccontextmanager
    async def raw_conn(self):
        async with aiosqlite.connect(self.path, timeout=5) as conn:
            yield conn


class Transport:
    def __init__(self):
        self.submitted = []
        self.observed = []
        self.raise_submit = False
        self.response = {"state": "published", "operation_id": "op_123",
                         "receipt": {"message_ids": [123], "post_urls": ["https://max.ru/test/123"]}}

    async def submit(self, payload, key):
        self.submitted.append((payload, key))
        if self.raise_submit:
            raise TimeoutError("Authorization: secret; https://provider/?token=secret")
        return self.response

    async def observe(self, key, operation_id):
        self.observed.append((key, operation_id))
        return self.response


async def prepare(db, **kwargs):
    return await prepare_delivery(db, issue_id=289, provider="max", target_id="native-123",
                                  payload=kwargs or {"text_html": '<a href="https://t.me/guide/123">Экскурсия</a>',
                                                    "image_b64": "frozen-image", "alias": "max_guide"})


@pytest.mark.asyncio
async def test_receipt_and_payload_survive_restarts_and_alias_changes(tmp_path):
    db = DB(tmp_path / "delivery.sqlite")
    row = await prepare(db)
    transport = Transport()
    first = await drive_delivery(db, row, transport)
    assert first["state"] == "published"
    restarted = DB(db.path)
    replay = await prepare(restarted, alias="renamed", text_html="changed", image_b64="changed")
    assert replay["payload"] == row["payload"]
    assert replay["request_key"] == row["request_key"]
    assert (await drive_delivery(restarted, replay, transport))["state"] == "published"
    assert len(transport.submitted) == 1
    assert transport.observed == []


@pytest.mark.asyncio
async def test_accepted_then_reply_lost_only_observes_original_identity(tmp_path, caplog):
    db = DB(tmp_path / "delivery.sqlite")
    row = await prepare(db)
    transport = Transport()
    transport.raise_submit = True
    first = await drive_delivery(db, row, transport)
    assert first["state"] == "outcome_unknown"
    assert "secret" not in caplog.text
    # Even after the provider's implicit dedup horizon, a restart cannot publish.
    async with db.raw_conn() as conn:
        await conn.execute("UPDATE guide_visual_delivery SET created_at='2000-01-01', updated_at='2000-01-01'")
        await conn.commit()
    second = await drive_delivery(DB(db.path), row, transport)
    assert second["state"] == "published"
    assert len(transport.submitted) == 1
    assert transport.observed == [(row["request_key"], None)]


@pytest.mark.asyncio
async def test_crash_after_claim_before_submit_never_blindly_retries(tmp_path):
    db = DB(tmp_path / "delivery.sqlite")
    row = await prepare(db)
    async with db.raw_conn() as conn:
        await conn.execute("UPDATE guide_visual_delivery SET state='submitting'")
        await conn.commit()
    transport = Transport()
    transport.response = {"state": "not_found"}
    result = await drive_delivery(db, row, transport)
    assert result["state"] == "outcome_unknown"
    assert not transport.submitted
    assert len(transport.observed) == 1


@pytest.mark.asyncio
async def test_concurrent_claim_has_one_submission(tmp_path):
    db = DB(tmp_path / "delivery.sqlite")
    row = await prepare(db)
    transport = Transport()
    await asyncio.gather(*(drive_delivery(db, row, transport) for _ in range(8)))
    assert len(transport.submitted) == 1
    assert (await get_delivery(db, row["request_key"]))["state"] == "published"


@pytest.mark.asyncio
async def test_accepted_is_not_published_and_later_observed(tmp_path):
    db = DB(tmp_path / "delivery.sqlite")
    row = await prepare(db)
    transport = Transport()
    transport.response = {"state": "accepted", "operation_id": "op_123"}
    assert (await drive_delivery(db, row, transport))["state"] == "accepted"
    transport.response = {"state": "published", "operation_id": "op_123", "receipt": {"message_ids": [321]}}
    assert (await drive_delivery(db, row, transport))["state"] == "published"
    assert transport.observed == [(row["request_key"], "op_123")]
    assert len(transport.submitted) == 1


@pytest.mark.asyncio
async def test_false_published_without_provider_receipt_is_unknown(tmp_path):
    db = DB(tmp_path / "delivery.sqlite")
    row = await prepare(db)
    transport = Transport()
    transport.response = {"state": "published", "operation_id": "op_123"}
    assert (await drive_delivery(db, row, transport))["state"] == "outcome_unknown"


@pytest.mark.asyncio
async def test_malformed_receipt_is_recorded_as_unknown(tmp_path):
    db = DB(tmp_path / "delivery.sqlite")
    row = await prepare(db)
    transport = Transport()
    transport.response = {"state": "published", "receipt": ["malformed"]}
    result = await drive_delivery(db, row, transport)
    assert result["state"] == "outcome_unknown"
    assert result["error_code"] == "invalid_receipt"
