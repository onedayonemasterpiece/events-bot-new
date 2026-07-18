# preprod_release — R06/R07 results

## Lane contract

- Lane: `preprod_release`
- Requirements: `R06`, `R07`
- Base SHA: `4542a7dfaedf3d86ea4b5e4618e06e717f0dc8cf`
- Head SHA (implementation): `cec13af1148ac5c35358d381d8ce26a010125e4b`
- Branch: `agent/static-event-preprod/release-routing`
- Production DB/root mutations: none

## Result

### R06 — canonical preproduction event-template/review routing: Done

- Accepted `static-event-detail-v11` baseline plus v12 fidelity/idempotency corrections are documented as the primary preproduction event-page family, not a lab alternative.
- Dated `preview-…` URLs in canonical docs are explicitly historical evidence.
- Added one read-only, fail-closed `resolve_current_secret_candidate()` source for the current review URL. Bot/outbox review-link generation and `scripts/request_static_site_build.py --show-current-review` consume it; no public redirect or root `current.json` is created.
- The durable receipt binds build/run/repo/snapshot/input fingerprint/effective Kaliningrad date/result SHA/manifest SHA/token SHA/object count/verified URL and negative root/stable-ICS mutation evidence.
- Bearer URL is delivered only through the notifier/operator output. Runtime structured logs and `JobOutbox.last_result` retain only `current_review_ready`, not the URL.

### R07 — Smart Update to refreshed immutable secret candidate: Done in source

The audited source path is:

1. an effectful Smart Update reaches `schedule_event_update_tasks()`;
2. when `ENABLE_STATIC_SITE_KAGGLE_BUILDER=1`, it writes one durable `static_site_build:prod` request, delayed 15 minutes after the latest effect;
3. SQLite `BEGIN IMMEDIATE` keeps one pending/running owner and exactly one coalesced follow-up;
4. the worker creates and verifies an immutable online-backup snapshot;
5. the public-projection fingerprint includes the `Europe/Kaliningrad` effective date, so unchanged input is a zero-push no-op while local date rollover changes the fingerprint;
6. one status-aware Kaggle run builds the root-form proof and noindex secret candidate;
7. the trusted result validator now requires every production check, including `template_matrix`, and every candidate noindex/no-referrer/prefix/root-isolation check to be `ok`;
8. the create-only publisher verifies result, manifest, bucket-list privacy, all object hashes/MIME and public HTTP before the internal current-review receipt advances.

Failed, unchanged/no-op, incomplete-receipt and artifact-only runs preserve the previous published review pointer. Existing crash adoption remains exact-dataset-first, so a stale waiter cannot launch a concurrent duplicate.

## Evidence and commands

- `python3 -m py_compile main.py static_site_release.py scripts/request_static_site_build.py` — passed.
- `/home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -q tests/test_static_site_release.py tests/test_static_site_build_handoff.py tests/test_static_site_build_debounce.py tests/test_static_site_public_gate.py` — `39 passed`.
- `npm --prefix site run test:static-release` — `5 passed`.
- `git diff --check` — passed.
- `rg ... 'https://kenigevents.ru/preview-2026|preview-2026[0-9]' main.py main_part2.py static_site_release.py scripts site/scripts tests` — no hard-coded dated review links in link-producing code/scripts.
- `scripts/request_static_site_build.py --db /tmp/no-such-static-site.sqlite --show-current-review` — fail-closed JSON `current_review_unavailable` (expected nonzero after the final CLI correction).

New regression coverage proves:

- newest fully checked publication becomes the canonical review target;
- no-op and failed runs preserve the previous target;
- incomplete publication receipts are rejected without pointer replacement;
- the bot/outbox link producer reads the shared resolver;
- downloaded production-candidate results with an incomplete accepted template/noindex check set fail closed.

## Changed files

- `main.py`
- `static_site_release.py`
- `scripts/request_static_site_build.py`
- `tests/test_static_site_release.py`
- `docs/features/static-site-pages/README.md`
- `docs/features/static-site-pages/astro-preview.md`
- `docs/features/static-site-pages/event-desktop-media-families-2026-07-12.md`
- `docs/features/static-site-pages/release-plan.md`
- `docs/operations/kaggle-static-site-builder.md`
- `CHANGELOG.md`
- `.codex/lanes/preprod_release/RESULTS.md`

## Risks / runtime blockers

- No production deploy, live Kaggle run, bucket upload, or production DB migration was performed in this lane.
- Runtime refresh remains gated by `ENABLE_STATIC_SITE_KAGGLE_BUILDER=1`, `ENABLE_STATIC_SITE_SECRET_PUBLISH=1`, exact pushed `STATIC_SITE_REPO_SHA`, Kaggle/status callback credentials, Object Storage credentials and anonymous ListObjects disabled. Without those, the source path is ready but no new live candidate is created.
- The first deployed claim lazily adds the nullable internal receipt column to `static_site_build_state`; this is not an event/business-data migration. The resolver itself is read-only and returns unavailable before that column exists.
- Production root promotion remains intentionally blocked; `/`, `current.json` and stable `/ics/*` are outside the publisher API.
