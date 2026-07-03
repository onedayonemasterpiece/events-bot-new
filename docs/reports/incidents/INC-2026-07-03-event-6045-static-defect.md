# INC-2026-07-03 Event 6045 Static Defect

Status: mitigated
Severity: sev2
Service: Telegram Monitoring / Smart Update / public event pages
Opened: 2026-07-03
Closed: —
Owners: events-bot data-quality / Smart Update
Related incidents: `INC-2026-06-24-future-event-date-default-venue-regressions.md`, `INC-2026-06-12-future-event-quality-llm-first-repair.md`, `INC-2026-05-05-smart-update-gemma3-fallback-hallucination.md`, `INC-2026-06-16-vk-quality-duplicates-non-events.md`, `INC-2026-06-27-telegraph-footer-backfill-content-loss.md`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/smart-event-update/README.md`, `docs/features/static-site-pages/README.md`, `docs/operations/incident-management.md`

## Summary

Admin reported that static KenigEvents page for event `6045` showed an unsupported future date (`2027-03-01`) and invented programme/attendance copy for a coffee/music source post from `@signalkld/11052`.

Investigation confirmed this as a data-quality incident, not a static-rendering bug. Telegram poster OCR contained record metadata `LP 33 1/3 RPM`, which the Telegram Monitoring OCR date helper interpreted as `1/3` and rolled forward to `2027-03-01`. Smart Update then allowed the weak social candidate into the create path and the writer generated unsupported generic “artists / master-classes / ticket” copy. The static site projected the bad production DB row.

## User / Business Impact

- The public preview page displayed a non-existent future event.
- Existing public Telegraph and Telegram event surfaces repeated the unsupported date/copy.
- Static search/listing inventories could keep surfacing a false future event until the canonical row was hidden and a new static build excludes it.

## Detection

- Admin report from the static site UI for `event_id=6045`.
- Source-grounded checks compared:
  - static page `preview-20260703t084038-2ef8dd83/.../6045/`;
  - Telegram source `https://t.me/signalkld/11052`;
  - VK/source-managed URL `https://vk.com/wall-231920894_3468`;
  - production `event`, `event_source`, `eventposter`, `event_source_fact`, `joboutbox`;
  - Telegraph page and Telegram `@kldevents` post.

## Timeline

- 2026-06-15 08:00 UTC — source Telegram post `@signalkld/11052` published; text is coffee/music promo copy with no explicit `1 марта 2027` event.
- 2026-06-15 23:09 UTC — production event `6045` created with `date=2027-03-01`; poster OCR stored `Blues & Roots Lp 33 1/3 RPM Charlie Mingus`.
- 2026-06-16 06:40 UTC — public Telegram event post `@kldevents/591` published with `📅 1 марта`.
- 2026-07-03 — admin reported the static page defect; investigation confirmed unsupported date and generated copy.
- 2026-07-03 — production mitigation marked event `6045` cancelled/silent, edited Telegraph and Telegram public surfaces, and verified static active selection count is zero.

## Root Cause

1. Telegram Monitoring OCR date extraction treated the vinyl speed fragment `LP 33 1/3 RPM` as numeric date `1/3`.
2. The single-event OCR merge path then let that OCR date override the extracted event date.
3. Smart Update did not route social candidates with ungrounded dates to the LLM-first eventness gate before create.
4. Smart Update writer generated unsupported generic copy for a weak/non-event source candidate.

## Contributing Factors

- Existing date provenance was persisted but not used as a create-time fail-closed routing signal.
- The static site correctly exported active future DB rows, so it amplified canonical DB corruption.
- Telegram deletion was no longer possible for the older channel message; the message had to be edited in place.

## Automation Contract

### Treat as regression guard when

- Changing Telegram Monitoring OCR date extraction, OCR-to-event merge, or prompt date rules.
- Changing `source_parsing/telegram` candidate handoff for OCR/date fields.
- Changing Smart Update create/eventness/date-provenance routing for Telegram/VK candidates.
- Changing writer/create bundle grounding or public static event export filters.

### Affected surfaces

- `kaggle/TelegramMonitor/telegram_monitor.py`
- `smart_event_update.py`
- production `event`, `event_source`, `eventposter`, `event_source_fact`, `joboutbox`
- Telegraph event page
- Telegram `@kldevents` event post
- managed/external VK wall URL
- static event page export/render

### Mandatory checks before closure or deploy

- OCR helper must return no date for `Blues & Roots Lp 33 1/3 RPM Charlie Mingus`.
- Valid compact OCR/source dates such as `10.05 14:00` must still parse as dates/times.
- A social candidate with an ungrounded date must route through LLM-first eventness review and skip when the source does not confirm an event.
- A real relative-date invite can survive that LLM eventness review.
- Replay fixture for source `@signalkld/11052` must remain available under `tests/replays/INC-2026-07-03-event-6045-static-defect/`.
- Production repair evidence must show event `6045` is no longer selected by active static export predicates.

### Required evidence

- Test output for Telegram OCR guard and Smart Update eventness guard.
- Production DB backup table names and post-repair row.
- Public Telegram post after in-place edit or deletion evidence.
- Telegraph page after repair.
- VK API exact-post check.
- Deployed SHA and `origin/main` reachability if code is deployed.

## Immediate Mitigation

- Created row-level production backups:
  - `codex_backup_20260703_6045_event`
  - `codex_backup_20260703_6045_event_source`
  - `codex_backup_20260703_6045_eventposter`
  - `codex_backup_20260703_6045_event_source_fact`
  - `codex_backup_20260703_6045_joboutbox`
- Marked event `6045` as `lifecycle_status='cancelled'`, `silent=1`, `date_is_inferred=1`, `date_provenance='ungrounded'`, cleared Telegram publication pointers/hashes, and verified `static_active_count=0`.
- Edited Telegram `@kldevents/591` in place to a removal notice because Bot API deletion returned `message can't be deleted`.
- Edited the Telegraph page to a removal notice.
- VK API `wall.getById -231920894_3468` returned no items; no VK delete was needed.

## Corrective Actions

- Added a narrow Telegram Monitoring OCR metadata guard so record/vinyl speed fragments such as `LP 33 1/3 RPM`, `33⅓ RPM`, and `45 RPM` are not treated as dates.
- Added prompt guidance that record/vinyl metadata and catalogue numbers are not event dates/times.
- Routed Telegram/VK candidates whose date is ungrounded in source text or poster OCR through Smart Update’s LLM-first eventness review before create.
- Added regression tests and a replay fixture for event `6045`.

## Follow-up Actions

- [ ] Add a full production-import replay harness for Telegram Monitoring result JSON → server import → Smart Update shadow DB when the current incident runner is available.
- [ ] Add writer-grounding review for unsupported generic programme/ticket boilerplate in create descriptions, beyond the eventness/date fix.
- [ ] Trigger or verify the next production static-site build/publication after deploy so the stale preview artifact is superseded by a build that excludes event `6045`.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending manual `flyctl deploy`
- regression checks:
  - `tests/test_tg_monitor_gemma4_contract.py::test_tg_monitor_ocr_date_ignores_vinyl_speed_metadata`
  - `tests/test_smart_event_update_non_event_guards.py::test_ungrounded_social_date_routes_to_llm_eventness_and_skips`
  - `tests/test_smart_event_update_non_event_guards.py::test_ungrounded_relative_date_can_survive_llm_eventness_review`
- post-deploy verification: pending

## Prevention

- OCR date parsing now has a narrow metadata-noise guard for this failure family.
- Smart Update create routing now treats ungrounded social dates as an LLM-first semantic eventness risk rather than a low-risk create candidate.
