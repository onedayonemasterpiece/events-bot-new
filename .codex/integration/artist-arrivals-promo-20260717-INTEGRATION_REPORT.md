# Artist arrivals promo — integration report (2026-07-17)

Branch: `integration/artist-arrivals-promo-20260717`
Base: `origin/main` at `f2c9c83f`
Release posture: **shadow-only; no deploy and no public delivery**.

## Requirement closure

| ID | Status | Evidence / remaining gate |
|---|---|---|
| R01 | Done | Versioned canonical JSON + sparse Fly SQLite overlay + sanitized SSG projection; YDB rejected for this event-domain state. |
| R02 | Partial | Evidence-reviewed local suppression overlay is seeded, including explicit Kaliningrad evidence for Alexey Markov Quartet and current regional institutional evidence for HOFFMANN TRIO. Automatic LLM-first discovery/matching of every new event remains a later stage. |
| R03 | Done for reviewed snapshot | 20 current appearances and 26 profiles are captured with row-level event/locality evidence. This is a dated reviewed snapshot, not continuous discovery. |
| R04 | Partial | Daily frozen manifest, stable artist+project-family dedupe, target-aware TG+VK ledger, cancellation and full source-revision invalidation, ambiguous-send reservation/reconciliation are implemented. Continuous LLM-first discovery remains outstanding. |
| R05 | Done | Configurable default threshold: 3 unique artists, preferred 4, at least 2 projects, max 8 cards. |
| R06 | Partial | Telegram RichMessage slideshow and VK carousel rendering/publisher paths are implemented with identity/rights, HTTPS allowlist, public-IP and 12 MiB streaming gates. Current snapshot intentionally has no public-approved artist photos, so auto-publication remains blocked. |
| R07 | Partial | Sanitized static JSON and Hero Talk preview component are implemented with active campaign/activity/expiry gating. Production homepage remains the noindex placeholder and is not changed by this branch. |
| R08 | Partial | Independent digest and Hero promo activities are modeled and disabled by default. A durable Hero exposure/cooldown ledger is not yet implemented. |

## Safety gates

- scheduler default: off;
- publication mode default: `shadow`;
- independent public switch default: off;
- campaign created as `draft`; digest and Hero activities disabled;
- publisher requires active in-window campaign, enabled digest activity, verified media identity, rights status and evidence;
- event/source changes invalidate reviewed appearances;
- local, unknown, mixed/mobile, expired, cancelled, tribute/recording/author-only entries fail closed;
- test targets cannot poison production dedupe;
- `sending` reservations require explicit operator reconciliation;
- built-in photo fetcher requires a configured HTTPS host allowlist, public DNS result and bounded streaming.

## Validation

- `pytest -q tests/test_artist_arrivals.py` — **19 passed**.
- `pytest -q tests/test_scheduling.py tests/test_promo.py` — **92 passed**.
- `python -m py_compile artist_arrivals/*.py models.py db.py scheduling.py promo.py handlers/promo_cmd.py site/scripts/export-production-preview-data.py` — PASS.
- `git diff --check` — PASS.
- Curated JSON and YAML route/lane documents parse — PASS.
- Alembic programmatic upgrade/downgrade and SQLModel/`Database.init()` schema checks — PASS in integration validation.
- `npm run build` — PASS in independent merge review (420 pages); no site code changed after that gate.
- Independent merge review: **PASS for merge in shadow-only mode; FAIL for public/auto release** until the Partial gates above are closed.

## Release decision

Safe to merge as disabled shadow foundation. Not approved for deploy/public auto. A later release must add continuous LLM-first discovery/eval, approved photo assets and rights, production-homepage acceptance, Hero exposure cooldown accounting, and a fresh evidence/release review.
