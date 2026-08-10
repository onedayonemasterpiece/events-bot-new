# my-data-hub daily-statistics producer

Status: **implemented, disabled by default; no production canary or deployment**

`my_data_hub_daily_statistics.py` is the bounded producer for data product
`events-bot.daily-statistics.v1`. It reads the events-bot SQLite database in
`mode=ro` with `PRAGMA query_only=ON`, computes one aggregate for a completed
Kaliningrad reporting day, writes the exact connector envelope to a durable
filesystem outbox, and submits only those retained bytes to the my-data-hub HTTPS
intake.

This is not a database replication path. The producer has no PostgreSQL driver or
my-data-hub database credential and must never write `hub`, `analysis`,
`orchestration`, `region_talk`, publication, or other shared canonical tables.

## Data contract

| Field | Value |
|---|---|
| connector | `events-bot.daily-statistics` |
| data product | `events-bot.daily-statistics.v1` |
| payload schema | `events-bot-daily-statistics.v1` |
| envelope contract | `my-data-hub-data-connector.v1` |
| delivery mode | `push` |
| default reporting timezone | `Europe/Kaliningrad` |

The runtime contract matches
`my-data-hub/schemas/data-connector-envelope.v1.schema.json` (source SHA-256
`7c5858600aa1bbf7f05296fc0c06d7874ff67abd3351e8b9965420ea9bbb5a53`).
Both `inline_records` and the full envelope use RFC 8785 canonical UTF-8 JSON.
`payload_sha256` hashes the exact canonical `inline_records`; the spool records
and verifies the full canonical envelope SHA-256. Because the envelope contains
the normalized default `trace: {}`, the exact byte hash equals the hash attested
by the current my-data-hub connector receipt contract.

One non-sensitive aggregate record contains only:

- reporting date and timezone;
- total event rows added in that reporting window;
- aggregate counts by `city` and `event_type` (`unknown` for absent/unsafe labels);
- the non-secret, full lowercase 40-hex events-bot Git revision.

It does **not** contain event text, source messages, user/chat/account identifiers,
URLs, credentials, access tokens, database dumps, or individual event rows.

The batch UUID and idempotency key are deterministic for
`reporting date + timezone + schema version`. `produced_at` is the reporting-period
end, not the retry time. A retry therefore cannot silently acquire a new identity.
Late corrections require a future explicit superseding-batch flow; this v1 producer
does not overwrite an accepted day.

## Durable outbox and receipt behavior

The default spool is
`/data/my-data-hub-connectors/events-bot.daily-statistics`, on the existing durable
Fly volume. All directories are `0700` and files are atomically replaced as `0600`.

```text
pending/       exact canonical envelopes plus mutable retry state
delivered/     exact accepted envelope bytes retained for reconciliation
receipts/      validated, envelope-bound acceptance/replay receipts
quarantine/    exact terminal conflict/rejection/auth-failure envelopes + evidence
```

Each attempt reads `pending/*.json` as bytes, validates its recorded SHA-256, and
POSTs those same bytes. It never reserializes an envelope during retry. Timeouts,
`429`, and `502/503/504` retain the envelope and apply bounded exponential backoff
with deterministic jitter (or bounded `Retry-After`). A restart resumes the same
outbox. `200/201/202` is complete only after the returned receipt validates all of:

- receipt and batch UUIDs;
- connector, idempotency key, payload SHA-256 and full envelope SHA-256;
- timezone-aware acceptance timestamp;
- accepted/replayed disposition.

The receipt is fsynced before the pending item is removed. The exact envelope is
also retained under `delivered/`. Recovery finishes an interrupted
receipt-to-delivered transition without resubmitting different bytes. Conflicts,
contract rejection, and authentication failure are retained in `quarantine/` and
require operator repair; authentication failures are not allowed to form a retry
storm.

## Configuration (default off)

The command is inert unless the exact opt-in is enabled:

```dotenv
MY_DATA_HUB_DAILY_STATISTICS_ENABLED=0
MY_DATA_HUB_DAILY_STATISTICS_DB_PATH=/data/db.sqlite
MY_DATA_HUB_DAILY_STATISTICS_SPOOL_DIR=/data/my-data-hub-connectors/events-bot.daily-statistics
MY_DATA_HUB_DAILY_STATISTICS_TIMEZONE=Europe/Kaliningrad
MY_DATA_HUB_DAILY_STATISTICS_TIMEOUT_SECONDS=15
MY_DATA_HUB_EVENTS_BOT_INTAKE_URL=https://<my-data-hub-host>/intake/v1/batches
MY_DATA_HUB_EVENTS_BOT_SERVICE_TOKEN=<dedicated-connector-service-token>
MY_DATA_HUB_EVENTS_BOT_SOURCE_REVISION=<full-deployed-events-bot-git-sha>
```

`MY_DATA_HUB_EVENTS_BOT_SERVICE_TOKEN` is a separate service-to-service connector
credential. Do not reuse MCP OAuth clients/tokens, Telegram credentials, Supabase
keys, PostgreSQL credentials, Fly tokens, or operator tokens. The implementation
does not read those variables as a fallback. An enabled producer fails closed unless
the dedicated URL is HTTPS and both dedicated intake values are present.

Manual offline/default-off check:

```bash
python3 my_data_hub_daily_statistics.py
# {"enabled": false, "status": "disabled"}
```

After the canary prerequisites below are approved and configured, one bounded day
can be invoked explicitly:

```bash
MY_DATA_HUB_DAILY_STATISTICS_ENABLED=1 \
python3 my_data_hub_daily_statistics.py --reporting-date YYYY-MM-DD
```

The JSON result exposes only safe local health: pending count, oldest spooled time,
last receipt time, quarantine count, and attempt/delivery disposition. Exit `1`
means the exact envelope remains deferred or quarantined; exit `2` is a fail-closed
configuration/spool error.

## Canary and scheduling gates

Do not set `MY_DATA_HUB_DAILY_STATISTICS_ENABLED=1` or add a production schedule
until all of these are complete:

1. the my-data-hub `/intake/v1/batches` route is deployed and proven to return the
   exact `ConnectorReceipt` fields expected by this producer;
2. a registry row binds a dedicated service principal only to
   `events-bot.daily-statistics` and its payload/size/rate limits;
3. the service token is delivered through the secret store under the dedicated env
   name and is absent from logs, files, envelopes and receipts;
4. `/data` capacity, backup/restore, `0700`/`0600` spool permissions and restart
   persistence are verified;
5. an empty/synthetic private canary proves accept, exact replay, outage retention,
   restart retry, hash-conflict quarantine and invalid-receipt retention;
6. the accepted batch remains source evidence/landing state until the independent
   my-data-hub normalizer/committer advances canonical state;
7. owner approval records the cadence, local run time, alert path, retention policy,
   rollback (`ENABLED=0`) and receipt evidence.

Only after that receipt may the existing scheduling system invoke this command once
per completed local day. This change intentionally does not edit `scheduling.py`,
`fly.toml`, secrets, or production state.

## Local verification

```bash
python3 -m pytest tests/test_my_data_hub_daily_statistics.py -q
python3 -m py_compile my_data_hub_daily_statistics.py
```

Focused tests pin a canonical 1,000-byte envelope, payload SHA-256 and full envelope
SHA-256; prove the source SQLite file is unchanged; prove restart retry submits the
same bytes; validate the stored receipt; retain auth failures; and prove the default
is disabled with no fallback to an unrelated operator credential.
