# INC-2026-06-04 Telegram Monitoring Media And Digest Quality

Status: mitigated
Severity: sev2
Service: Telegram Monitoring / VK Publishing / Smart Update
Opened: 2026-06-04
Closed: —
Owners: Codex
Related incidents: `INC-2026-06-04-tg-monitoring-vk-fanout-llm-quota-storm`, `INC-2026-06-02-vk-captcha-text-only-posts`, `INC-2026-05-17-future-event-quality-regressions`, `INC-2026-05-07-vk-auto-import-merge-regression-gemma4`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/vk-publishing/README.md`, `docs/features/smart-event-update/README.md`, `docs/llm/prompts.md`

## Summary

Three Telegram-origin events from `https://t.me/k_mira101/424` were created without media and then published as managed `klgdevents` VK posts with `attachments=0`. The same rows were repeatedly skipped by CherryFlash/video announce because no renderable posters existed. The operator also reported a fresh generic digest-like VK event titled `Дайджест, Мы давно его ждали`; exact fresh DB evidence for that title was not found, but the class is covered by a new LLM-first prompt rule and fail-closed guards.

## User / Business Impact

- Telegram Monitoring could create legitimate event rows that looked complete enough for `vk_sync`, but public VK posts appeared without illustrations.
- Events without renderable posters were invisible to video announce selection and therefore could miss promo surfaces.
- A generic digest wrapper without event-local time/place could be materialized as a public event if an upstream LLM/parser returned it as a candidate.

## Detection

- Operator noticed Telegram Monitoring publications in VK without pictures and asked for production investigation.
- Production DB rows and runtime file logs confirmed the issue:
  - `event_id=5640`, `5641`, `5642` have `photo_urls=[]`, no poster rows, source `https://t.me/k_mira101/424`.
  - `post_to_vk ok ... post_id=1983/1984/1985 ... attachments=0`.
  - `video_announce.popular_review: skipped event without renderable posters event_id=5640/5641/5642`.
- Search for the exact `Дайджест, Мы давно его ждали` row in current production DB did not find a fresh matching event.

## Timeline

- 2026-06-04 08:32:39 UTC: Smart Update created `event_id=5640`, `added_posters=0`.
- 2026-06-04 08:32:54 UTC: Smart Update created `event_id=5641`, `added_posters=0`.
- 2026-06-04 08:33:05 UTC: Smart Update created `event_id=5642`, `added_posters=0`.
- 2026-06-04 10:32-12:28 UTC: video announce repeatedly skipped all three rows because they had no renderable posters.
- 2026-06-04 14:04:43 UTC: VK created `wall-231920894_1983` for `event_id=5642` with `attachments=0`.
- 2026-06-04 14:04:50 UTC: VK created `wall-231920894_1984` for `event_id=5641` with `attachments=0`.
- 2026-06-04 14:04:58 UTC: VK created `wall-231920894_1985` for `event_id=5640` with `attachments=0`.
- 2026-06-04 20:00-21:00 UTC: investigation confirmed DB/log evidence and added prevention changes.

## Root Cause

1. `sync_vk_source_post` treated empty `event.photo_urls` / empty Telegraph fallback as acceptable for new Telegram-origin managed VK posts.
2. Telegram multi-event text posts can legitimately produce several event rows without media, but the publication boundary did not distinguish "DB row without media" from "public VK post without media".
3. The digest prompt policy already discouraged low-detail digest imports, but there was no regression guard for a generic wrapper title with no event-local time/place after the LLM/parser had already produced a candidate.

## Contributing Factors

- Existing captcha fail-closed coverage only handled media upload failures after photos existed; it did not cover the no-media-from-source case.
- Video announce correctly rejected rows without renderable posters, but this signal did not block VK publication.
- The exact reported digest row was not present by the time of DB search, so prevention had to target the class rather than repair one row.

## Automation Contract

### Treat as regression guard when

- Changing Telegram Monitoring result import, media/poster ingestion, `event.photo_urls`, `eventposter`, `sync_vk_source_post`, `post_to_vk`, Smart Update parser/defender logic, or digest/multi-event prompt rules.
- Changing video announce media eligibility for Telegram-origin events.

### Affected surfaces

- `main_part2.py::sync_vk_source_post`
- `main.py::_event_parse_defender_check`
- `smart_event_update.py::_smart_event_update_impl`
- `docs/llm/prompts.md`
- `JobOutbox(vk_sync)` and managed VK community `klgdevents`
- CherryFlash/video announce media selection

### Mandatory checks before closure or deploy

- Unit coverage that Telegram-origin `vk_sync` raises `vk_sync_missing_media_for_telegram_event` before `wall.post` when no attachment is available.
- Existing VK captcha text-only regression still passes.
- Parser/Smart Update guard tests for generic digest wrapper without logistics and negative control with time+venue.
- Production evidence collected from `/data/runtime_logs` and `/data/db.sqlite`.
- No compensating rerun/requeue/data repair unless the operator explicitly asks for it.

### Required evidence

- deployed SHA:
- tests:
- production DB/log evidence:
- confirmation that fix is reachable from `origin/main`:

## Immediate Mitigation

- Added a fail-closed VK publication guard for Telegram-origin events without any renderable `photo...` attachment. Default is enabled; emergency override is `VK_REQUIRE_MEDIA_FOR_TG_SOURCE_POSTS=0`.
- Added a prompt rule that generic digest/afisha/podborka wrapper titles are not event titles unless the post contains one concrete event-local title plus time and venue/address.
- Added parser and Smart Update safety-net guards for generic digest shells with no time/place.

## Corrective Actions

- `main_part2.py`: new Telegram-origin media requirement before creating a managed VK event post.
- `main.py`: parser defender now flags `generic_digest_shell` and forces the existing retry/fail-safe path.
- `smart_event_update.py`: candidates like `Дайджест`/`Афиша`/`Подборка`/`Мы давно его ждали` without event-local logistics are skipped as `skipped_non_event:generic_digest_shell`.
- Docs and changelog updated; replay fixture added in `tests/replays/INC-2026-06-04-tg-monitoring-media-and-digest-quality/`.

## Follow-up Actions

- [ ] Add a broader future-active quality audit for giveaway-derived and charity-collection rows such as `event_id=5629` and `event_id=5657` without performing automatic repair during this incident.
- [ ] Consider an operator alert for repeated `video_announce.popular_review: skipped event without renderable posters` on newly-created Telegram-origin rows.

## Release And Closure Evidence

- deployed SHA:
- deploy path:
- regression checks:
- post-deploy verification:

## Prevention

- Telegram-origin VK publication now fails closed when the media set is empty, so a missing-media import issue remains visible as a `vk_sync` failure instead of becoming a text-only public VK post.
- Digest-wrapper prompt and safety-net tests prevent generic editorial wrappers from becoming public event rows without their own logistics.
