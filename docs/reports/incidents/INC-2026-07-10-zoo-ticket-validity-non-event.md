# INC-2026-07-10 Zoo ticket-validity notice imported as event

Status: mitigated
Severity: sev2
Service: Telegram Monitoring / Smart Update / Telegram, VK, Telegraph and ICS event fanout
Opened: 2026-07-10
Closed: —
Owners: events-bot maintainer / Codex
Related incidents: `INC-2026-07-10-future-event-semantic-audit.md`, `INC-2026-07-09-recurring-occurrence-date-drift.md`, `INC-2026-07-07-new-event-quality-degradation.md`, `INC-2026-07-03-event-6045-static-defect.md`, `INC-2026-06-24-future-event-date-default-venue-regressions.md`, `INC-2026-04-30-tg-monitoring-event-quality-regressions.md`, `INC-2026-04-30-tg-monitoring-work-schedule-false-skips.md`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/smart-event-update/README.md`, `docs/llm/request-guide.md`, `docs/operations/runtime-logs.md`

## Summary

Telegram source `https://t.me/kldzoo/7641` contained only Kaliningrad Zoo visitor/cash-desk hours, normal operating-mode wording, ticket-purchase instructions and the statement that an admission ticket is valid until **31 December 2026**. Telegram Monitoring converted that ticket-validity deadline into event `6783` dated `2026-12-31 09:00`; Smart Update classified it as an exhibition and inferred `end_date=2027-01-31`. The false row then published to Telegram, Telegram Calendar/ICS, VK and Telegraph.

The same investigation rechecked the operator's second report, `Рыцарский турнир` event `3980`. That separate series-vs-occurrence defect is already mitigated and tracked by `INC-2026-07-09-recurring-occurrence-date-drift.md`; its current canonical/public date `2026-07-10 20:00` is source-grounded and must not be deleted.

## User / Business Impact

- VK `klgdevents` and Telegram `@kldevents` advertised an ordinary zoo work-hours notice as a 31 December event.
- The generated public copy hallucinated an exhibition narrative unsupported by the source.
- ICS/Telegram Calendar and Telegraph amplified the false logistics.
- The vector identity gate returned `allow_create`: it checked identity/dedup recall, not whether the candidate was a real event or whether `31 декабря` had occurrence semantics.

## Detection

- Operator reported the Zoo 31 December and Knight 1 May shapes on 2026-07-10.
- Production DB/source/public-surface evidence confirmed event `6783` as a high-confidence `non_event`, `wrong_date_time` and inferred-range defect.
- Authenticated VK API resolved stored postponed id `6856` to live managed post `https://vk.com/wall-231920894_6892`.
- Telegram public event post was `https://t.me/c/3954607218/2181`; calendar post was `https://t.me/kenigeventscalendar/7387`; Telegraph page was `https://telegra.ph/Kaliningradskij-zoopark-07-09`.
- Runtime mirror was checked. `ENABLE_RUNTIME_FILE_LOGGING=0`; retained logs ended on 2026-07-08, so `telegram_scanned_message`, `ops_run`, `event_identity_decision_log`, `joboutbox`, source rows and authenticated public APIs are the durable fallback evidence.

## Timeline

- 2026-07-09 06:03 UTC — source `@kldzoo/7641` published.
- 2026-07-09 23:20 UTC — scheduled Telegram Monitoring run `ops_run=3392` extracted/imported one event; vector identity decision `436` allowed create with unrelated nearest event `6562` at similarity `0.7575`.
- 2026-07-09 23:21 UTC — ICS, Telegraph and VK fanout completed.
- 2026-07-10 05:00 UTC — Telegram event post `2181` published.
- 2026-07-10 — operator reported the issue; incident opened and exact public/source evidence collected.
- 2026-07-10 — production root overlay was independently found at 100% while taking the incident snapshot. Terminal/published temporary video outputs `844`–`851` were removed, restoring about 5.7 GB and a successful `/tmp` write probe before repair continued.
- 2026-07-10 — row-level backups were created; Telegram event/calendar posts, live VK post and ICS object were deleted; Telegraph was replaced with a neutral tombstone; event `6783` was cancelled and silenced.
- 2026-07-10 13:16 UTC — prevention SHA `732e34702f68f94b47b3c034d34999d1444a8efd` was deployed from clean `origin/main` as Fly image `deployment-01KX62P844FWPSCVYCPZDBE8NR`, machine version `1615`; the post-deploy health check was ready with no issues.
- 2026-07-10 13:21–13:24 UTC — scoped compensating Telegram Monitoring run `ops_run=3421`, `trigger=incident_catchup`, reprocessed only `@kldzoo/7641` through the reserved remote `S22` role. It completed `success`: one forced message, zero extracted/imported events and zero errors. The force row was consumed and no replacement event was created.

## Root Cause

1. Telegram Monitoring's structural `schedule_like` routing treated any Russian day/month plus at least two `HH:MM` values as a timetable candidate. The Zoo source satisfied it with one ticket-validity date and visitor/cash-desk hours.
2. The main event extractor was suppressed for this shape. Because no genuine date-header block existed, schedule rescue passed the whole message to a prompt that assumed a date header followed by event time lines.
3. The schedule prompt named only a narrow set of work-hours negatives and did not explicitly assign `ticket_valid_until` / `work_hours` roles. The model therefore turned `31 декабря` and `09:00` into occurrence logistics.
4. Smart Update considered the date source-grounded because the same day/month was lexically present. Its LLM eventness router did not recognize operational-hours/ticket-validity date-role ambiguity.
5. Upstream `event_type=выставка` plus `до 31 декабря` triggered the default one-month end-date inference, compounding the defect.
6. Existing pgvector recall is identity-only. It correctly found no duplicate identity but cannot be interpreted as a quality/eventness approval.

## Contributing Factors

- The extraction schema did not carry date-role/evidence semantics.
- Schedule rescue's whole-message fallback violated its own prompt precondition.
- Public fanout had no independent current source-grounding quality decision.
- The personalization vector sidecar was stale during the audit: only `191/305` current future canonical rows were present (`62.6%`), with latest indexing on 2026-07-02.

## Automation Contract

### Treat as regression guard when

- Changing Telegram Monitoring main/schedule/single-event prompts or `schedule_like` routing.
- Changing Smart Update work-hours, eventness, date provenance or inferred exhibition-range logic.
- Treating vector identity decisions as any form of event-quality approval.
- Running a future-event quality audit or publishing a newly imported social candidate.

### Affected surfaces

- `kaggle/TelegramMonitor/telegram_monitor.py`
- `smart_event_update.py`
- `tests/test_tg_monitor_gemma4_contract.py`
- `tests/test_smart_event_update_non_event_guards.py`
- production `event`, `event_source`, `eventposter`, `telegram_scanned_message`, `event_identity_decision_log`, `joboutbox`
- Telegram `@kldevents`, Telegram Calendar, VK `klgdevents`, Telegraph, ICS and static/vector projections

### Mandatory checks before closure or deploy

- Replay `tests/replays/INC-2026-07-10-zoo-ticket-validity-non-event/source.json` through Telegram Monitoring and the server import boundary; it must produce no public event.
- A semantic schedule screen must distinguish `event_timetable`, `institution_hours_or_ticket_terms` and `other`, with evidence spans/date role.
- A message without genuine date-header blocks must never enter whole-message schedule extraction.
- Smart Update must LLM-review operational-hours/ticket-validity ambiguity and fail closed on `non_event`, uncertainty or LLM unavailability.
- Positive controls: a dated zoo excursion and a genuine dated visitor timetable remain events.
- `INC-2026-07-09` occurrence-vs-series tests and closure-grade replay remain green.
- Production repair must be verified through DB plus authenticated Telegram/VK and real Telegraph/ICS checks.
- Vector-first full-future audit must report exact catalog coverage; missing/stale vectors or missing LLM verdicts cannot be counted as pass.
- Fix SHA must be reachable from `origin/main`; post-deploy `/healthz` and SQLite quick check must pass.

### Required evidence

- Prod DB/public/source artifacts under `artifacts/codex/INC-2026-07-10-future-date-quality/`.
- Targeted unit/prompt-contract tests and production-equivalent replay.
- All-future vector/LLM audit summary with exact `305/305` pre-repair inventory coverage.
- Backup table counts and public deletion/tombstone verification.
- Deploy SHA/image and post-deploy health.

## Immediate Mitigation

- Backup tables:
  - `codex_backup_20260710_zoo_ticket_validity_event` — 1 row;
  - `..._event_source` — 2 rows;
  - `..._eventposter` — 2 rows;
  - `..._joboutbox` — 6 rows.
- Deleted Telegram event post `2181` and Telegram Calendar post `7387` through Bot API.
- Deleted live managed VK post `wall-231920894_6892` after owner/postponed-id verification.
- Deleted generated ICS object `event-6783-2026-12-31.ics`.
- Replaced the non-deletable Telegraph page with a neutral `Публикация удалена` tombstone.
- Set event `6783` to `lifecycle_status=cancelled`, `silent=1` and cleared all managed-publication URLs/hashes while preserving source evidence.

## Corrective Actions

- Added an LLM-first schedule screen with strict decision/date-role/evidence schema.
- Removed the unsafe whole-message schedule-rescue fallback when no date-header blocks exist.
- Expanded main/schedule prompt contracts for normal operating mode, visitor/cash-desk hours and ticket-validity/expiry dates.
- Added a routing-only Smart Update safety net that sends operational date-role ambiguity to the LLM eventness reviewer; deterministic code does not decide eventness.
- Added exact incident replay data and positive-control regression tests.

## Follow-up Actions

- [x] Complete the zero-write full-future vector-first + LLM audit: exact frozen `305/305` coverage, `193` reused vectors + `112` local fills, zero provider-error verdicts after retries; nine rows imported after freeze also received vector + LLM `pass`, yielding exact active-future coverage `308/308` at the explicit `2026-07-10T14:07:17Z` cutoff. Source-confirmed repairs and the remaining fail-closed review queue are tracked in `INC-2026-07-10-future-event-semantic-audit.md`.
- [ ] Add a durable append-only quality-decision/publication gate keyed by event hash + source-bundle hash; identity-vector approval must remain separate.
- [ ] Restore/schedule current vector-sidecar coverage and alert on core-vs-sidecar lag without making Supabase canonical.
- [ ] Add first-class date-role/evidence fields to the producer/server contract after replay evaluation.
- [ ] Finish `INC-2026-07-09` full VK import + Smart Update replay and whole-series update negative control.

## Release And Closure Evidence

- deployed SHA: `732e34702f68f94b47b3c034d34999d1444a8efd`, reachable from and equal to `origin/main` at deploy time.
- deploy path: clean-worktree `flyctl deploy -a events-bot-new-wngqia`; image `deployment-01KX62P844FWPSCVYCPZDBE8NR`, machine version `1615`, one passing machine check.
- regression checks: focused producer/Smart Update/incident/recurrence suite `59 passed`; the broader two-file run had one baseline failure also reproducible on pre-change `origin/main` (`test_zero_ticket_price_without_explicit_free_evidence_stays_not_free`).
- production replay: `ops_run=3421`, `success`, `sources_scanned=1`, `messages_processed=1`, `messages_forced=1`, `messages_with_events=0`, `events_created=0`, `events_merged=0`, `errors_count=0`.
- full-future audit: exact `305/305` pre-repair rows, `193` reused + `112` locally filled vectors, final LLM counts `174 pass / 82 repair-candidate / 6 remove-candidate / 42 needs_review / 1 indeterminate`, with zero provider-error verdicts; model flags remain fail-closed candidates until source adjudication.
- production repair verification: `telegram_scanned_message(20,7641)` is `done` with `events_extracted=0/events_imported=0`; event `6783` remains `cancelled/silent`; no later Zoo/31-December replacement row exists. Authenticated Telegram checks return message-not-found, VK returns `is_deleted=true`, Telegraph contains the tombstone, and the ICS object returns not-found.
- post-deploy health: `/healthz` returned `ok=true`, `ready=true`, `db=ok`, `issues=[]`; production snapshot and repaired DB both passed SQLite quick check.

## Prevention

Vector retrieval is the recall stage, not truth. Durable prevention is `vector-first candidates/prototypes -> LLM source-fact/date-role verification -> current hash-bound quality decision -> public fanout`. Missing vectors, missing source evidence, malformed LLM output or stale decisions fail closed; canonical state remains in Fly SQLite.
