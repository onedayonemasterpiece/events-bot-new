# R11 — ordinary-ingestion facts-v3 replay harness

## Result

Implemented on `agent/static-collection-product-loop/ingestion-harness` from
`integration/static-collection-facts-v3@c625f180`.

The new bounded CLI accepts an explicitly mutable SQLite copy and a versioned
manifest of real source/event fixture bindings. Each fixture is sent through the
ordinary Telegram, VK, or official-parser production entry point; the harness
does not call `apply_collection_decisions`. Publication jobs, Telegram/VK
reconciliation, Telegraph rebuilds, and live web/media fallback are disabled.
The same fixture is then replayed identically and must produce no Event or
EventSource mutation and no collection-adjudication call/write.

The JSON report binds exact command/repository/DB/manifest/fixture hashes and
records redacted adapter result, safe provider trace, write diff, receipt input
hashes, fact values, and exact persisted-source quote grounding. It refuses
`/data`, requires `--allow-mutable-copy`, and returns nonzero on any failed case.

## Files

- `scripts/run_static_collection_ingestion_replay.py`
- `docs/review-data/static_collection_ingestion_replay_manifest.schema.json`
- `tests/test_static_collection_ingestion_replay.py`
- `docs/operations/static-collection-facts-v3.md`
- `CHANGELOG.md`

## Validation

No provider calls, production writes, deploys, or publication actions were run
while implementing this lane. Tests use local fakes only.

```text
/opt/venvs/events-bot-modern/bin/python -m pytest -q tests/test_static_collection_ingestion_replay.py
6 passed
```

## Real-fixture acquisition boundary

The production database retains normalized Event/EventSource evidence, not the
original Telegram `telegram_results.json`, VK `EventDraft`, or parser
`TheatreEvent` packet. Source-faithful acceptance therefore requires downloading
already-completed Kaggle Telegram/parser artifacts and an actually captured VK
draft; reconstructing packets from DB rows is only a smoke test. The runbook
states this explicitly and gives the required >=12 boundary/adapter mix. No
historical packet was invented and Gate E is not claimed by this implementation
lane.
