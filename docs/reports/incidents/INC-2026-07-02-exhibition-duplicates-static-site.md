# INC-2026-07-02 Exhibition Duplicates Surfaced By Static Site

Status: open
Severity: sev2
Service: Telegram Monitoring / VK import / parsers / Smart Update / static site event inventory
Opened: 2026-07-02
Closed: —
Owners: Codex / events-bot maintainers
Related incidents: `INC-2026-06-24-future-event-date-default-venue-regressions.md`, `INC-2026-06-18-tg-location-prose-still-extracted.md`, `INC-2026-05-30-active-duplicate-events-recall-gate.md`, `INC-2026-05-05-kitoboya-garage-date.md`, `INC-2026-05-05-event-quality-regression.md`, `INC-2026-05-01-future-event-quality-audit.md`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/smart-event-update/README.md`, `docs/features/static-site-pages/astro-preview.md`, `docs/operations/incident-management.md`

## Summary

The new static site exhibition/long-running listing made visible a systemic production data-quality issue: many real exhibitions are represented by multiple active event cards with different titles, inferred periods, venues, and public pages. The issue is not only read-side presentation; production `event` rows contain duplicate exhibition identities, malformed dates, prose/emoji venue fields, parser source contamination, and at least one prompt/comment leak into an attendee-facing title.

Read-only production evidence on 2026-07-02: `PRAGMA quick_check=ok`; `379` active current/future rows; `82` exhibition-like or continuing candidates selected by the static-site-style filter.

## User / Business Impact

- Static `/vystavki/` and today/continuing sections can show the same real exhibition as several cards.
- Readers can see wrong periods, wrong venues, prompt text, or non-date strings as active future inventory.
- Existing Telegram/VK/Telegraph rows may also stay wrong until repaired or republished.
- Operators cannot rely on current exhibition inventory for static site, daily/week/month surfaces, or video/search candidate pools.

## Detection

- Operator report on 2026-07-02 after the static site exposed many exhibition duplicates.
- Production SQL dump saved under `artifacts/codex/exhibition-duplicates-20260702/prod_exhibition_dump.json`.
- Heuristic report saved under `artifacts/codex/exhibition-duplicates-20260702/heuristic_report.txt`.
- Runtime mirror checked: `ENABLE_RUNTIME_FILE_LOGGING=1`, `/data/runtime_logs/events-bot.log*`, 27 files. Most offending imports are older than the 24h normal runtime window; DB/source rows are primary evidence.
- External a-opus review requested and saved under `artifacts/codex/exhibition-duplicates-20260702/a_opus_full_review.md`.

## Timeline

- 2026-07-02 UTC — incident investigation opened from operator report.
- 2026-07-02 UTC — prior incident contracts for event quality, exhibition dates, duplicate recall, date/default-venue drift, and prose-location were reviewed.
- 2026-07-02 UTC — production DB read-only dump found `82` exhibition-like/continuing active current/future candidates.
- 2026-07-02 UTC — high-confidence clusters identified: KОИХМ `С чего начинается Родина`/`Древние воины`, `Куплю гараж`, `Билетёры 2.0`, `Розовый натюрморт`/`Выставка одной картины`, Tretyakov `Великие учителя`, `Точка и линия`, `Альбрехт Дюрер`, and corrupt-field rows.
- 2026-07-02 UTC — a-opus consultation completed and agreed that missing canonical exhibition identity plus permissive as-of exhibition materialization is the highest-leverage root cause.

## Confirmed Candidate Classes

### Duplicate or identity-drift clusters

| Cluster | Evidence | Current problem |
|---|---|---|
| KОИХМ `С чего начинается Родина` / `Древние воины` | `2781`, `4512`, `3986`, `6301`, `6302`, `698`, plus prompt-leak row `6518` | Roundups/spotlights and linked old sources created or enriched multiple active cards; `6301/6302` have venue `🔹Выставка «С чего начинается Родина»`; `6518` title is prompt/comment text. |
| `Куплю гараж. Калининград` | canonical `4517` and duplicate `5752` | Prior incident class recurred: later source/update created another active card with venue fragment `История автомобиля` and different inferred period. |
| `Билетёры 2.0` | `5765` and `5766` from same source posts | Exact active duplicate with same title/time/venue, but different inferred end dates (`2026-07-13` vs `2027-01-05`). |
| `Розовый натюрморт` / `Выставка одной картины` | `5391`, `6080`, `6296`, plus related concert `5238` and misclassified concert `3730` | Several same-exhibition cards and one concert classified as exhibition; closure concert must remain separate. |
| `Великие учителя` / Tretyakov | `3788`, `3216`, `4352`, `4366`, parser-contaminated `3864` | Official exhibition, spotlights, art breakfasts, and unrelated parser ticket pages have been merged or materialized as separate/mixed cards. |
| `Точка и линия` | canonical-ish `5370`, mentions `5969`, `5971`, `6214` | Anniversary/reviewer/first-visitor posts became new cards with inferred one-month ranges and wrong venues (`День России`, `Дом книжного наследия...`). |
| `Альбрехт Дюрер. Секретный код` | canonical-ish `5703`, duplicate `6382` | Spotlight/update became new card; `location_name='☝Гений Северного Возрождения'`. |

### Data integrity / field leakage

- `5698` active row has `end_date='://'`.
- `115` active row has `end_date` filled with a full unrelated Dom Kitoboya lecture text.
- `6485` has `location_name='обычном режиме'`, `location_address='до 20:00'`.
- `6518` has public title equal to internal prompt/comment text.

## Root Cause

1. **No first-class canonical exhibition identity.** One real exhibition is represented only by ordinary event rows, so source posts, roundups, reviews, spotlights, opening/closing notes, and parser pages all compete as separate attendee-facing cards.
2. **Permissive Telegram Monitoring exhibition rescue prompts.** The current prompt family allows ongoing exhibition/museum spotlight posts to materialize a new `event_type="выставка"` card with `date=message_date` as an as-of merge date. This is useful for first discovery but amplifies duplicates when Smart Update does not merge the update into the canonical row.
3. **Long-running range inference masks bad provenance.** Default/inferred one-month `end_date` makes updates look like legitimate temporary exhibitions with distinct periods.
4. **Roundup and spotlight extraction can leak structure into fields.** Bullets, prose, operational schedule text, and section titles can become `location_name`/`location_address`.
5. **Smart Update duplicate recall is not exhibition-identity aware.** Title/venue/date drift and inferred ranges defeat same-exhibition merging; sharing source URLs or linked-source context is not currently a hard active-duplicate guard for long-running exhibitions.
6. **Parser source contamination.** Tretyakov/Sobor parser rows can attach multiple unrelated ticket/program URLs to one row, mixing concerts, films, art breakfasts, and exhibitions.
7. **Write-time schema/data validation is insufficient.** Invalid date strings and prompt/comment text reached active public rows.
8. **Static site is an exposure amplifier, not the root cause.** It made accumulated active rows visible because it correctly separates long-running/exhibition-like inventory from quick today listings.

## Contributing Factors

- Multiple source types publish recurring updates about the same exhibition over weeks/months.
- Source posts can mention several exhibitions and events in one roundup.
- Existing incident closures focused on specific known duplicate/date/location cases, not continuous exhibition identity health.
- There is no scheduled quality report for active long-running duplicates, invalid dates, or public-field prompt leakage.

## Automation Contract

### Treat as regression guard when

- Changing Telegram Monitoring prompts/schema for exhibitions, museum spotlights, as-of date handling, date range inference, or roundup extraction.
- Changing Smart Update duplicate matching, source URL/idempotency, inferred end-date handling, or long-running event merge logic.
- Changing static site filters for `/vystavki/`, continuing events, or event quality suppression.
- Changing Tretyakov/Sobor/Museum parser source grouping or ticket-page ingestion.
- Performing production repair of active exhibition/long-running rows.

### Affected surfaces

- `kaggle/TelegramMonitor/telegram_monitor.py`
- `source_parsing/telegram/handlers.py`
- `smart_event_update.py`
- Tretyakov/Sobor and museum parser paths
- production SQLite `event`, `event_source`, `eventposter`, `joboutbox`, `event_publication`
- static site data export and `/vystavki/` listing
- Telegram/VK/Telegraph event public surfaces

### Mandatory checks before closure or deploy

- Replay raw source fixtures for the confirmed clusters through production import boundary + Smart Update on a prod snapshot/shadow DB.
- Confirm one real exhibition does not survive as multiple active cards for the clusters listed above.
- Confirm roundup/spotlight/update posts attach to canonical exhibition identity or become non-public updates, not new active cards with `date=message_date` and inferred one-month range.
- Confirm `event.date` and `event.end_date` are valid ISO dates or NULL; invalid strings must be rejected/quarantined before active rows.
- Confirm prompt/comment/code-like text cannot enter title/location/description fields.
- Confirm prose/emoji/operational text does not enter `location_name`/`location_address`.
- Confirm concerts, closure events, and festival series are not auto-merged into exhibitions and do not inherit exhibition `event_type`.
- Confirm static site has a read-side defensive quality gate or explicit suppression for known-bad active rows until production repair is complete.
- Check runtime log mirror or documented fallback evidence.

### Required evidence

- Production before/after SQL for every repaired row and side table.
- Source links/text excerpts for each cluster decision.
- Replay fixture outputs with pre/post DB diff.
- Static-site export/listing verification after cleanup.
- Public Telegram/VK/Telegraph verification for edited/deactivated rows when applicable.
- Test output for prompt/schema/Smart Update/static-site guard changes.
- Deployed SHA reachable from `origin/main` if code changes are deployed.

## Immediate Mitigation

2026-07-02 investigation-only pass preserved evidence under `artifacts/codex/exhibition-duplicates-20260702/`.

2026-07-06 production mitigation repaired the high-confidence active/current-future subset found by a fresh audit. Evidence is under `artifacts/codex/20260706-future-event-incident-audit/`.

## Corrective Actions

### 2026-07-06 production repair

LLM-first source review classified the repaired rows as duplicate exhibition identities, prompt/code leakage, invalid date fields, prose/default-venue drift, or retrospective/non-event rows. The repair intentionally used canonical Fly SQLite/Smart Update semantics first; Supabase pgvector remains a downstream static/search sidecar that must be resynced from canonical exports after cleanup, not patched as a source of truth.

Production backup tables were created before writes:

- `codex_backup_20260706_future_event_incident_audit_event`
- `codex_backup_20260706_future_event_incident_audit_event_source`
- `codex_backup_20260706_future_event_incident_audit_eventposter`
- `codex_backup_20260706_future_event_incident_audit_joboutbox`
- `codex_backup_20260706_future_event_incident_audit_velikie_event`
- `codex_backup_20260706_future_event_incident_audit_velikie_event_source`
- `codex_backup_20260706_future_event_incident_audit_velikie_eventposter`
- `codex_backup_20260706_future_event_incident_audit_velikie_joboutbox`

Repaired canonical rows:

- Invalid date fields: `115` `end_date` prose → `NULL`; `5698` `end_date='://'` → `NULL`; `3730` range cleared and `event_type` corrected to `концерт`.
- Duplicate/non-public exhibition updates cancelled with `silent=1` and `merged_into_event_id` where applicable: `3986→2781`, `4512→2781`, `4989→4913`, `5752→4517`, `5766→5765`, `5969→5370`, `5971→5370`, `6080→5391`, `6214→5370`, `6296→5391`, `6301→2781`, `6302→698`, `6382→5703`, `3788→3216`, `4352→3216`, `4366→3216`.
- Non-event/prompt/default-venue defects cancelled: `6485`, `6518`, `6691`.
- Survivor `3216` (`Великие учителя`) date/location corrected to source-grounded `2026-04-09`, `Филиал Третьяковской галереи`, `Парадная наб. 3`.

Public surface repair:

- Re-armed and verified page rebuild jobs: `month_pages:2026-07`, `week_pages:2026-W28`, `weekend_pages:2026-07-11` finished `done`.
- Authenticated VK cleanup deleted/verified stale managed posts for cancelled rows `4512`, `5766`, `5969`, `5971`, `6080`, `6214`, `6296`, `6301`, `6518`, `6691`; exact `wall.getById` verification returned `still_found=false`, and stale cancelled-row `source_vk_post_url` values were cleared.
- Telegram `@kldevents` captions were conservatively edited (not album-deleted) for cancelled rows `4512`, `5766`, `5969`, `5971`, `6080`, `6214`, `6296`, `6301`, `6302`, `6518`, `6691`; Bot API returned `ok=true` for all edited caption messages.

Verification:

- Production `PRAGMA quick_check` returned `ok`.
- Repaired/touched rows no longer appear in the active future inventory.
- `invalid_end_date_future=[]`; prompt/comment leak and repaired bad venue probes returned no repaired-row hits.
- Confirmed one active survivor for the repaired clusters: `Точка и линия` (`5370`), `Билетёры 2.0` (`5765`), `С чего начинается Родина` (`2781`), `Древние воины Янтарного края` (`698`), `Альбрехт Дюрер` (`5703`), `Куплю гараж` (`4517`), `Великие учителя` (`3216`).

## Follow-up Actions

- [x] Build and run a production-safe repair for Tier 1 high-confidence rows: prompt leak `6518`, invalid date rows `5698`/`115`, obvious exact duplicates, and bad venue fragments.
- [x] Repair the high-confidence parts of Tier 2 clusters: KОИХМ duplicate/update rows, `Великие учителя`, `Розовый натюрморт`, `Билетёры 2.0`. Remaining ambiguous policy work stays open below.
- [ ] Add a static-site read-side quality gate for invalid dates, prompt/comment leakage, prose/emoji venue fields, and exact normalized exhibition duplicates.
- [ ] Replace or narrow `date=message_date` as-of exhibition materialization with source-post role classification: `announcement`, `update`, `roundup_mention`, `spotlight`, `review`, `closure`.
- [x] Add Smart Update replay guard for retrospective recap posts with a thin future teaser and ungrounded extracted venue/address (`skipped_non_event:retrospective_future_teaser`, regression fixture `wall-127107743_14691`).
- [ ] Add write-time schema validation/quarantine for ISO dates and public field anti-leak rules.
- [ ] Add Smart Update exhibition duplicate logic: normalized exhibition title, venue aliases, shared source URL guard, and inferred end-date-aware matching.
- [ ] Design/backfill a canonical exhibition identity model (`canonical_exhibition_id` or equivalent) and seed it from the confirmed clusters.
- [ ] Isolate Tretyakov/Sobor parser source URLs so one event cannot absorb unrelated ticket/program pages.
- [ ] Add weekly active exhibition quality report and alert thresholds.

## Release And Closure Evidence

- deployed SHA: pending until `agent/e6691-prevention` is merged/deployed.
- deploy path: clean worktree `.worktrees/incident-e6691-prevention` from `origin/main`; production deploy pending.
- regression checks: `python3 -m py_compile smart_event_update.py`; targeted pytest replay for E6691/grounded-future controls passed (`4 passed`). Full local file run still has pre-existing unrelated `test_zero_ticket_price_without_explicit_free_evidence_stays_not_free` failure caused by LLM-disabled date-provenance review, not this guard.
- post-deploy verification: pending.

## Prevention

The durable prevention path is to treat exhibitions as long-lived canonical objects and source posts as evidence/updates to those objects, not as independent attendee-facing event cards by default. Prompt changes alone are insufficient: closure requires replay fixtures, write-time validation, Smart Update canonical matching, static-site read-side defenses, and public-surface verification after repair.
