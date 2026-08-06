# Volunteer Monitor source skeleton

Status: **fixture-debugged; real GitHub/Kaggle live-canary not yet executed**.

This package is the read-only source boundary for the first `Добро.рф` pilot. It does not write Fly SQLite, create festivals, publish UI, or run BGE/LLM matching.

Implemented:

- canonical `/event/<id>` discovery from the event search surface;
- Playwright region and `С доступными вакансиями` filter interaction using accessible selectors;
- bounded `Показать еще` traversal with an explicit zero-supply state;
- source-local extraction of title, organizer, dates, location, roles and explicit outbound links;
- `OPEN / CLOSED / EXPIRED / UNKNOWN` availability state;
- separate semantic and availability hashes;
- target-region proof, PII redaction and fail-closed partial-run semantics;
- fixture CLI, source/search-provider tests and direct/Kaggle read-only canary workflows.

Local verification:

```text
19 passed
fixture CLI: PASS, 2 source pages, 1 OPEN, 1 CLOSED
```

Run fixtures:

```bash
PYTHONPATH=. pytest -q \
  tests/test_dobro_volunteer_monitor.py \
  tests/test_festival_source_search.py

PYTHONPATH=. python scripts/run_volunteer_monitor.py \
  --mode fixture \
  --search-html tests/fixtures/volunteer_monitor/search.html \
  --event-map tests/fixtures/volunteer_monitor/event-map.json \
  --today 2026-08-04 \
  --output /tmp/volunteer-monitor.json
```

Run a read-only live canary after Playwright Chromium is installed:

```bash
python -m playwright install --with-deps chromium
PYTHONPATH=. python scripts/run_volunteer_monitor.py \
  --mode live_canary \
  --output /tmp/volunteer-monitor-live.json \
  --evidence-dir /tmp/volunteer-monitor-evidence
```

The live canary must be run only after the source-monitoring permission reference and allowed cadence are approved. A successful read-only canary is not permission to write production state.
