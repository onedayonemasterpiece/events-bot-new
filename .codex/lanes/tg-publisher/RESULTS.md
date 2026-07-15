# TG publisher lane result

- Status: **Done (implementation/test/docs); not pushed or deployed**
- Requirements:
  - **R02 Done** — canonical `@kldevents` event publishing resolves curated organizer/venue, minimal KGD80 festival, curated source and Pushkin identities; posts with matches use a deterministic `1300×330` graphical strip as the bottom standalone RichMessage media block. Event `6811` resolves exactly `konb + kgd80-80-stories + znanie-russia`. Existing RichMessages edit in place; legacy modes migrate by send-first/delete-after-success. RichMessages do not enqueue or receive legacy custom-emoji medallions.
  - **R03 Done** — every event footer mode uses the intentional 12-space separation; RichMessage HTML serializes it as twelve `&nbsp;` tokens so `Подробнее` and `Max` stay visually separate on one row while preserving `Max · Вконтакте` links.
  - **Incident Done (implementation lane)** — created `INC-2026-07-15-tg-rich-medallion-rendering-gaps`, indexed it, and ran the relevant `INC-2026-06-25` unit regression contract. Production release/catch-up evidence is explicitly left to the integrator.
- Branch: `agent/tg-rich-medallions/publisher`
- Worktree: `/home/dev/.codex/worktrees/events-bot-new/tg-rich-medallions-publisher`
- Base: `553ae2bb06b43503bd639547a688245ccbc0cb82` (`origin/main` at lane start)
- Head: single lane commit containing this file; resolve with `git rev-parse agent/tg-rich-medallions/publisher`

## Files / scope

- Runtime: `tg_graphic_medallions.py`, `main_part2.py`, `main.py`, `requirements.txt`
- Minimal KGD80 provenance/runtime: `site/src/data/festivalMedallions.json`, `site/src/assets/festivals/source/{README.md,kgd80-logo-hero.svg}`, `site/public/assets/festivals/kgd80-80-stories.{svg,png}`
- Tests: `tests/test_tg_event_publish.py`
- Canonical docs: `docs/features/tg-publishing/README.md`, `docs/features/static-site-pages/event-token-medallions.md`
- Incident/change records: `docs/reports/incidents/INC-2026-07-15-tg-rich-medallion-rendering-gaps.md`, `docs/reports/incidents/README.md`, `CHANGELOG.md`

## Commands / tests

Python environment: `/home/dev/.codex/worktrees/events-bot-new/event-age-rating-calibration/.venv/bin/python` (aiogram 3.29.1).

- `pytest` focused publisher/RichMessage/medallion/footer suite — **11 passed**.
- `pytest tests/test_tg_event_publish.py::test_tg_event_publish_job_does_not_enqueue_premium_editor_for_rich_message` — **1 passed**.
- aiogram session-level serialization regression proves all two event photos plus the strip are rewritten to distinct `attach://...` multipart file fields.
- `INC-2026-06-25` regression selection (unknown outbox task, worker-loop health, Storage endpoint, ticket-site public fanout) — **4 passed**; `test_running_vk_sync_stale_retries_instead_of_terminal_dependency_block` reached the expected stale retry state but its assertion failed on an existing Python 3.12 SQLite naive/aware datetime comparison. The incident's general stale replacement regression was run separately and **1 passed**.
- `python -m py_compile main.py main_part2.py tg_graphic_medallions.py tests/test_tg_event_publish.py` — **passed**.
- `git diff --check` — **passed**.
- Visual QA: event-6811 strip inspected at its native `1300×330`; KОНБ, KGD80 and Znanie are centered, readable and remain near the accepted `260px` visual size on graphite.

## Risks / merge notes

- Requires aiogram `>=3.29.1`; the pinned minimum is updated. The Bot API 10.2 `InputRichMessage.media` field is forward-compatible extra data in this aiogram release; a session-level test verifies recursive multipart extraction, not only DummyBot behavior.
- The festival inventory is deliberately minimal: only the source-faithful KGD80 entry/assets required here were brought from the existing medallion integration history. No unrelated organizer/festival asset churn is included; Znanie and KОНБ reuse current `origin/main` organizer assets.
- Production deployment must follow incident/release governance, requeue/catch up event `6811` and run a normal source-grounded RichMessage canary. Candidate event `6901` is currently blocked by the independent public-writer budget incident, per production evidence from the integrator.
- No OpenAI image generation was used. KGD80 PNG is a deterministic local headless-Chromium raster fallback from the committed SVG; runtime strip composition is Pillow-only.
