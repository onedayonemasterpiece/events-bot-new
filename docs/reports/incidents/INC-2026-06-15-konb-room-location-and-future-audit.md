# INC-2026-06-15 KОНБ room/floor location and future-event audit

Status: monitoring
Severity: sev2
Service: VK auto-import / Smart Update / future event catalog
Opened: 2026-06-15
Closed: —
Owners: events-bot
Related incidents: `INC-2026-06-12-future-event-quality-llm-first-repair`, `INC-2026-06-14-morning-import-quality-and-outbox-stale`
Related docs: `docs/features/vk-auto-queue/README.md`, `docs/features/telegram-monitoring/README.md`, `docs/reference/locations.md`, `docs/reference/location-aliases.md`

## Summary

Two KОНБ VK auto-import events were published with room/floor strings as public venue data:

- `https://t.me/kldevents/573` / event `6042`: `ЛЕКЦИОННЫЙ ЗАЛ` instead of `Научная библиотека, Мира 9`.
- `https://t.me/kldevents/575` / event `6041`: `4 ЭТАЖ ЛЕКЦИОННЫЙ ЗАЛ` instead of `Научная библиотека, Мира 9`.

The same audit found active future rows with generic/duplicate records: two generic `Музыкальный фестиваль` rows for the same `Калининградская музыкальная ночь` donor/map notice, over-split 80th-region concert rows, and older duplicate rows for `Алиса` and `Играем с оркестром`.

## User / Business Impact

- Public Telegram event posts carried misleading venue names for real library events.
- Future month/event listings contained duplicate/generic rows that reduced trust in the catalog.
- Promo/repost surfaces could amplify incorrect rows unless the data was repaired and future reposts stayed lead-time guarded.

## Detection

- User report on 2026-06-15 pointed to `@kldevents/573` and asked why KОНБ source default did not apply.
- Production DB and runtime log checks confirmed the rows were produced through VK auto-import from `wall-30777579_*`.
- Future active-event SQL audit checked generic titles, exact duplicate clusters, linked duplicate pairs, KОНБ source defaults, and source excerpts for the reported festival/80th-region clusters.

## Timeline

- 2026-06-15 16:30 UTC — VK auto-import parsed `https://vk.com/wall-30777579_15423`; runtime log shows Smart Update input `location=4 ЭТАЖ ЛЕКЦИОННЫЙ ЗАЛ`; event `6041` created.
- 2026-06-15 16:37 UTC — VK auto-import parsed `https://vk.com/wall-30777579_15425`; runtime log shows Smart Update input `location=ЛЕКЦИОННЫЙ ЗАЛ`; event `6042` created.
- 2026-06-15 16:55 UTC — event `6042` Telegram publication completed as `@kldevents/573`.
- 2026-06-15 17:41 UTC — event `6041` Telegram publication completed as `@kldevents/575`.
- 2026-06-15 — immediate production repair changed both DB rows and public captions to `Научная библиотека, Мира 9, #Калининград`.
- 2026-06-15 — first code mitigation added VK LLM prompt rule: room/floor labels are not venues; promo and poll reposts skip already-started/near-start events by default.
- 2026-06-15 — follow-up investigation found `telegram_source.default_location` for `kaliningradlibrary` was present, but `vk_source.location` for `konb39` was `NULL` in production.

## Root Cause

1. The affected rows came from VK source `group_id=30777579` (`konb39`), not from the Telegram source `@kaliningradlibrary`; therefore the Telegram source `default_location` could not help this import path.
2. Production `vk_source.location` for `konb39` was unset, so VK auto-import did not pass a stable `Научная библиотека, Мира 9, Калининград` source-location hint into extraction.
3. The VK extraction prompt treated an explicit `📍 лекционный зал, 4 этаж` as the event venue, even though for a stable single-building source this is only room/floor metadata.
4. Smart Update had enough context to reason about `Мира 9`/library in the matching rationale, but the created event still inherited the draft venue from VK intake.

## Contributing Factors

- Commit `91b13348` (`fix(events): guard future event quality imports`, 2026-06-12) intentionally reduced blind Telegram default-location writes for risky/un-grounded defaults after the future-quality incident. This was correct for Telegram false-default drift but made source-default behavior more conservative in the broader system.
- Commit `9f3b1c99` (`fix: add event quality fail-closed gates`, 2026-05-05) started fail-closed behavior for unsupported extracted Telegram venues instead of blindly replacing them with defaults.
- Those commits did not directly cause the KОНБ VK incident: this incident's immediate failure was missing VK source default plus VK LLM extraction choosing a room label. They are nevertheless relevant regression context because future fixes must not reintroduce blind default-location substitution.
- The future audit exposed accumulated duplicates from multi-source/aggregate announcements where the same real event was represented by several generic rows.

## Automation Contract

### Treat as regression guard when

- Changing `vk_intake.py`, `vk_auto_queue.py`, `smart_event_update.py`, `source_parsing/telegram/handlers.py`, or source default seeding.
- Changing `vk_source.location` / `telegram_source.default_location` semantics.
- Changing future-event duplicate, generic-title, or lead-time eligibility logic.

### Affected surfaces

- VK auto-import draft extraction and LLM prompt context.
- Smart Update create/merge venue handoff.
- Production `vk_source`/`telegram_source` source-location defaults.
- Future event listings, Telegraph month pages, Telegram/VK event publications, promo/poll repost eligibility.

### Mandatory checks before closure or deploy

- Verify `vk_source.location` for `group_id=30777579` is `Научная библиотека, Мира 9, Калининград` in prod or fresh DB init.
- Regression test that VK prompt treats `лекционный зал` / `4 этаж` as non-venue and mentions source-location/location hint recovery.
- Regression test that DB init repairs known VK source defaults including KОНБ.
- Re-audit active future rows for:
  - room/floor-only public `location_name`;
  - generic titles such as `Музыкальный фестиваль` / `Концерт к 80-летию региона`;
  - same-source/date/title/location duplicates.
- Verify promo/poll reposts do not repost events that have started or are within the 4-hour default lead-time cutoff.
- Verify deployed SHA is reachable from `origin/main` before closure if prod is deployed.

### Required evidence

- Runtime logs or DB rows for the reported event IDs/source URLs.
- SQL output for KОНБ VK/TG source defaults.
- SQL diff/summary for repaired future duplicate rows.
- Test output for the prompt/default/lead-time regression checks.
- Deploy SHA and Fly health/status after deploy.

## Immediate Mitigation

- Repaired event `6041` and `6042` DB/public Telegram captions to `Научная библиотека, Мира 9, #Калининград`.
- Added an LLM-first VK extraction prompt rule that room/floor labels are not venues.
- Added default 4-hour lead-time guards for Telegram/VK promo reposts and Poll to Repost.

## Corrective Actions

- Seed/repair `vk_source.location` for `konb39` to `Научная библиотека, Мира 9, Калининград` so VK imports have the same stable venue prior as Telegram imports.
- Strengthen the VK LLM prompt: if the explicit place is only room/hall/auditorium/floor, treat the venue as missing and use source organization/source-location/location hint when grounded by source text/OCR.
- Repair production duplicate/generic future rows and rebuild affected public listing jobs.

## Follow-up Actions

- [ ] Add a full replay fixture for `wall-30777579_15423`/`15425` through VK auto-import + Smart Update on a prod snapshot, not only prompt/unit tests.
- [ ] Add a scheduled future-catalog audit report that flags generic titles, same-source duplicate rows, room/floor-only venues, and public rows linked to hidden duplicate event IDs.
- [ ] Decide whether multi-venue citywide festivals should be represented as one umbrella event, venue-specific events, or both with explicit linking/display rules.

## Release And Closure Evidence

- deployed SHA: `e6713833` (`fix(vk-intake): seed konb source location`)
- deploy path: manual `flyctl deploy -a events-bot-new-wngqia --remote-only`, image `deployment-01KV6AB81SAJ2RRBJB3SFRFMTQ`, machine version `1424`
- regression checks:
  - `tests/test_vk_auto_queue_gemma4.py::test_vk_intake_prompt_treats_room_floor_as_non_venue`
  - `tests/test_vk_default_time.py::test_db_init_repairs_known_vk_source_location_defaults`
  - `tests/test_promo.py::test_promo_tg_repost_skips_event_inside_four_hour_lead_time`
  - `tests/test_poll_to_forward.py::test_load_eligible_events_skips_started_or_near_start_events`
- post-deploy verification: `/healthz` returned `ok=true`, `ready=true`; production SQL audit confirmed KОНБ VK/TG defaults, repaired `6041`/`6042`, no active room/floor-only locations, and no active generic `Музыкальный фестиваль` / `Концерт к 80-летию региона` rows.

## Prevention

The prevention contract is source-default plus LLM-first: source defaults are available as strong priors for stable sources, but room/floor labels must be interpreted as logistics inside a building, not as the building itself. Blind default replacement remains prohibited for real off-site venues; LLM/source grounding must decide the semantic venue.
