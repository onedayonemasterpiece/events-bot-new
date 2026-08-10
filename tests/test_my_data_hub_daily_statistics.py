from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

import pytest

import my_data_hub_daily_statistics as producer

SOURCE_REVISION = "3828f19a64e772a030097a466dd817c25955d4eb"
REPORTING_DATE = date(2026, 8, 9)
NOW = datetime(2026, 8, 10, 1, tzinfo=UTC)


def aggregate_fixture() -> producer.DailyAggregate:
    return producer.DailyAggregate(
        reporting_date=REPORTING_DATE,
        timezone="Europe/Kaliningrad",
        events_added_total=4,
        counts_by_city={"Калининград": 2, "Светлогорск": 1, "unknown": 1},
        counts_by_type={"concert": 2, "lecture": 1, "unknown": 1},
        source_revision=SOURCE_REVISION,
    )


def accepted_receipt(item: producer.SpoolItem) -> producer.Receipt:
    envelope = item.validated.value
    return producer.Receipt(
        receipt_id=str(uuid5(NAMESPACE_URL, f"receipt:{item.validated.envelope_sha256}")),
        status="accepted",
        connector_id=producer.CONNECTOR_ID,
        batch_id=envelope["batch_id"],
        idempotency_key=envelope["idempotency_key"],
        payload_sha256=envelope["payload_sha256"],
        envelope_sha256=item.validated.envelope_sha256,
        accepted_at="2026-08-10T01:00:00Z",
    )


def create_source_database(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE event (
                id INTEGER PRIMARY KEY,
                city TEXT,
                event_type TEXT,
                added_at TIMESTAMP NOT NULL
            )
            """
        )
        connection.executemany(
            "INSERT INTO event (city, event_type, added_at) VALUES (?, ?, ?)",
            [
                ("excluded-before", "other", "2026-08-08 21:59:59"),
                ("Калининград", "concert", "2026-08-08 22:00:00"),
                ("Калининград", "concert", "2026-08-09 10:00:00"),
                (None, None, "2026-08-09 21:59:59"),
                ("excluded-after", "other", "2026-08-09 22:00:00"),
            ],
        )


def test_daily_aggregate_reads_source_sqlite_without_mutating_it(tmp_path: Path) -> None:
    database = tmp_path / "events.sqlite"
    create_source_database(database)
    before = hashlib.sha256(database.read_bytes()).hexdigest()

    aggregate = producer.read_daily_aggregate(
        database,
        reporting_date=REPORTING_DATE,
        timezone_name="Europe/Kaliningrad",
        source_revision=SOURCE_REVISION,
    )

    assert aggregate.events_added_total == 3
    assert aggregate.counts_by_city == {"unknown": 1, "Калининград": 2}
    assert aggregate.counts_by_type == {"concert": 2, "unknown": 1}
    assert hashlib.sha256(database.read_bytes()).hexdigest() == before


def test_envelope_has_exact_canonical_bytes_and_pinned_hashes() -> None:
    exact = producer.build_envelope(aggregate_fixture())
    value = json.loads(exact)

    assert exact == producer.canonical_json_bytes(value)
    assert len(exact) == 1000
    assert producer.sha256_bytes(exact) == (
        "33d4422f74394240efacff11e8564ddb54066b028546b86e830acd6edfaf8b71"
    )
    assert value["payload_sha256"] == (
        "8a8d5e0ca948f97504be8b12b02f8009c297b784c0645e6a1473b3bf2a6d8e81"
    )
    assert value["contract_version"] == "my-data-hub-data-connector.v1"
    assert value["data_product"] == "events-bot.daily-statistics.v1"
    assert value["record_count"] == len(value["inline_records"]) == 1
    assert value["inline_records"][0].keys() == {
        "counts_by_city",
        "counts_by_type",
        "events_added_total",
        "reporting_date",
        "source_revision",
        "timezone",
    }


def test_outage_restart_retries_identical_bytes_and_stores_validated_receipt(
    tmp_path: Path,
) -> None:
    exact = producer.build_envelope(aggregate_fixture())
    spool_root = tmp_path / "spool"
    first_spool = producer.DurableDailyStatisticsSpool(spool_root)
    first_spool.enqueue(exact, queued_at=NOW)

    class Unavailable:
        def __init__(self) -> None:
            self.submissions: list[bytes] = []

        def submit(self, item: producer.SpoolItem) -> producer.DeliveryResult:
            self.submissions.append(item.validated.exact_bytes)
            return producer.DeliveryResult(
                producer.DeliveryDisposition.RETRY,
                message="synthetic outage",
            )

    unavailable = Unavailable()
    first = producer.deliver_ready(first_spool, unavailable, now=NOW)
    assert first == producer.DeliverySummary(attempted=1, deferred=1)

    class Available:
        def __init__(self) -> None:
            self.submissions: list[bytes] = []

        def submit(self, item: producer.SpoolItem) -> producer.DeliveryResult:
            self.submissions.append(item.validated.exact_bytes)
            return producer.DeliveryResult(
                producer.DeliveryDisposition.ACCEPTED,
                receipt=accepted_receipt(item),
            )

    restarted_spool = producer.DurableDailyStatisticsSpool(spool_root)
    available = Available()
    second = producer.deliver_ready(
        restarted_spool,
        available,
        now=NOW + timedelta(hours=2),
    )

    assert second == producer.DeliverySummary(attempted=1, delivered=1)
    assert unavailable.submissions == [exact]
    assert available.submissions == [exact]
    assert list(restarted_spool.pending_dir.glob("*.json")) == []
    delivered = list(restarted_spool.delivered_dir.glob("*.json"))
    receipts = list(restarted_spool.receipts_dir.glob("*.json"))
    assert len(delivered) == len(receipts) == 1
    assert delivered[0].read_bytes() == exact
    stored_receipt = json.loads(receipts[0].read_bytes())
    producer.validate_receipt(
        stored_receipt,
        expected=producer.validate_envelope_bytes(exact),
        status=stored_receipt["status"],
    )


def test_auth_failure_is_retained_in_quarantine_without_retry(tmp_path: Path) -> None:
    spool = producer.DurableDailyStatisticsSpool(tmp_path / "spool")
    exact = producer.build_envelope(aggregate_fixture())
    spool.enqueue(exact, queued_at=NOW)

    class AuthFailure:
        def submit(self, item: producer.SpoolItem) -> producer.DeliveryResult:
            return producer.DeliveryResult(
                producer.DeliveryDisposition.AUTH_FAILURE,
                message="credential repair required",
            )

    summary = producer.deliver_ready(spool, AuthFailure(), now=NOW)

    assert summary == producer.DeliverySummary(attempted=1, quarantined=1)
    assert list(spool.pending_dir.glob("*.json")) == []
    quarantined = [
        path for path in spool.quarantine_dir.glob("*.json") if not path.name.endswith(".state.json")
    ]
    assert len(quarantined) == 1
    assert quarantined[0].read_bytes() == exact


def test_producer_is_disabled_by_default_and_requires_dedicated_service_credential(
    tmp_path: Path,
) -> None:
    disabled = producer.ProducerConfig.from_env({})
    result = producer.run_once(disabled)
    assert result == {"enabled": False, "status": "disabled"}
    assert not disabled.spool_root.exists()

    with pytest.raises(producer.ConfigurationError, match="dedicated intake URL and service token"):
        producer.ProducerConfig.from_env(
            {
                "MY_DATA_HUB_DAILY_STATISTICS_ENABLED": "1",
                "MY_DATA_HUB_EVENTS_BOT_INTAKE_URL": "https://hub.example/intake/v1/batches",
                # An unrelated operator credential must never activate producer transport.
                "PRIVATE_EVENTS_MCP_OPERATOR_TOKEN": "not-a-connector-credential",
            }
        )

    enabled = producer.ProducerConfig.from_env(
        {
            "MY_DATA_HUB_DAILY_STATISTICS_ENABLED": "1",
            "MY_DATA_HUB_DAILY_STATISTICS_DB_PATH": str(tmp_path / "source.sqlite"),
            "MY_DATA_HUB_DAILY_STATISTICS_SPOOL_DIR": str(tmp_path / "spool"),
            "MY_DATA_HUB_EVENTS_BOT_INTAKE_URL": "https://hub.example/intake/v1/batches",
            "MY_DATA_HUB_EVENTS_BOT_SERVICE_TOKEN": "dedicated-connector-token",
            "MY_DATA_HUB_EVENTS_BOT_SOURCE_REVISION": SOURCE_REVISION,
        }
    )
    assert enabled.enabled is True
    assert enabled.service_token == "dedicated-connector-token"


def test_run_once_delivers_one_day_and_does_not_regenerate_an_accepted_identity(
    tmp_path: Path,
) -> None:
    database = tmp_path / "events.sqlite"
    create_source_database(database)
    config = producer.ProducerConfig.from_env(
        {
            "MY_DATA_HUB_DAILY_STATISTICS_ENABLED": "1",
            "MY_DATA_HUB_DAILY_STATISTICS_DB_PATH": str(database),
            "MY_DATA_HUB_DAILY_STATISTICS_SPOOL_DIR": str(tmp_path / "spool"),
            "MY_DATA_HUB_EVENTS_BOT_INTAKE_URL": "https://hub.example/intake/v1/batches",
            "MY_DATA_HUB_EVENTS_BOT_SERVICE_TOKEN": "dedicated-connector-token",
            "MY_DATA_HUB_EVENTS_BOT_SOURCE_REVISION": SOURCE_REVISION,
        }
    )

    class Available:
        def __init__(self) -> None:
            self.submissions: list[bytes] = []

        def submit(self, item: producer.SpoolItem) -> producer.DeliveryResult:
            self.submissions.append(item.validated.exact_bytes)
            return producer.DeliveryResult(
                producer.DeliveryDisposition.ACCEPTED,
                receipt=accepted_receipt(item),
            )

    transport = Available()
    first = producer.run_once(
        config,
        reporting_date=REPORTING_DATE,
        now=NOW,
        transport=transport,
    )
    database.unlink()
    second = producer.run_once(
        config,
        reporting_date=REPORTING_DATE,
        now=NOW + timedelta(hours=1),
        transport=transport,
    )

    assert first["created"] is True
    assert first["delivery"] == {
        "attempted": 1,
        "deferred": 0,
        "delivered": 1,
        "quarantined": 0,
    }
    assert second["created"] is False
    assert second["delivery"]["attempted"] == 0
    assert len(transport.submissions) == 1
