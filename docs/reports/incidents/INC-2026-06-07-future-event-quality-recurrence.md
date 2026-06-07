# INC-2026-06-07 Future Event Quality Recurrence

Status: monitoring
Severity: sev2
Service: Telegram Monitoring / Smart Update / VK event publishing
Opened: 2026-06-07
Closed: —
Owners: Codex
Related incidents: `INC-2026-05-17-future-event-quality-regressions.md`, `INC-2026-06-04-tg-monitoring-media-and-digest-quality.md`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/llm/request-guide.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

Production future-event audit on 2026-06-07 found newly published problematic event cards and VK posts:
short valid theatre titles were overwritten by umbrella/service text, poster date lists were interpreted as times,
and default technical times leaked into managed VK posts. Additional future rows exposed non-place
`location_name` fragments such as weekday prose.

## User / Business Impact

- Public `klgdevents` VK posts showed misleading event titles (`завтра в театре`, `Появился в продаже репертуар АВГУСТА!`).
- Two future Greza posts exposed `09:08`, parsed from a follow-up date token `9.08`, while the poster showed `16.00`.
- At least one visible VK post omitted the real time although the DB row had it.
- Future event inventory contained bad location fragments, making downstream Telegraph/month/VK surfaces untrustworthy.

## Detection

- Operator reported specific public VK links:
  - `wall-231920894_2362`
  - `wall-231920894_2351`
  - `wall-231920894_2345`
  - `wall-231920894_2344`
  - `wall-231920894_2316`
- A production DB snapshot audit found 503 active future/ongoing rows and 69 heuristic quality candidates.
- Runtime file mirror was available and confirmed root-cause logs under `/data/runtime_logs`.

Artifacts:

- `artifacts/codex/prod-future-quality-audit-2026-06-07/audit_report.md`
- `artifacts/codex/prod-future-quality-audit-2026-06-07/future_quality_candidates.json`
- `artifacts/codex/prod-future-quality-audit-2026-06-07/opus_consultation.md`
- `artifacts/db/prod_future_quality_audit_20260607_092147.sqlite`

## Timeline

- 2026-06-07 09:21 UTC: production SQLite snapshot captured from Fly.
- 2026-06-07 09:30 UTC: `/healthz` was green (`ok=true`, `ready=true`, `db=ok`).
- 2026-06-07 09:30-09:40 UTC: public VK examples were mapped to DB rows and source posts.
- 2026-06-07 09:40 UTC: runtime logs confirmed title inference and poster multiday expansion as direct causes.
- 2026-06-07 09:45 UTC: Opus consultation requested for LLM-first/prompt-design review.
- 2026-06-07 10:00 UTC: code prevention deployed to Fly image `deployment-01KTGRBMVAKWKCPS39JBA525C9`.
- 2026-06-07 10:08-10:09 UTC: production data repair deleted the bad rows and public posts, tombstoned Telegraph pages, and returned source rows to scan state.

## Root Cause

1. Server-side title fallback treated short valid titles as bad (`Идиот`, `Гараж`, `№ 13`) and rewrote them from the first message line, which was often umbrella/service text.
2. Poster OCR multiday expansion accepted ambiguous dotted tokens such as `9.08` as `09:08` when scanning the tail after earlier date markers.
3. VK source header rendered `event.time` even when `time_is_default=true`, leaking weak technical anchor times to public posts.
4. Existing semantic venue/title quality gates did not fully prevent all future bad rows from reaching public surfaces; full LLM-stage tightening remains required.

## Contributing Factors

- The previous May regression contract covered the symptom class, but the new short-title/title-fallback path was not pinned by tests.
- Existing poster-date expansion was intentionally conservative on pair count, but lacked a dotted-date ambiguity guard.
- Some visible defects were publication/sync state problems, not only extraction problems, so data repair must clean public posts and stale pages.

## Automation Contract

### Treat as regression guard when

- changing Telegram Monitoring candidate build, title fallback, poster OCR date/time extraction, multiday expansion, or VK source-post rendering;
- changing Smart Update merge/create bundle handling for future public events;
- changing source reimport/recreate tooling for Telegram or VK-origin events.

### Affected surfaces

- `source_parsing/telegram/handlers.py`
- `main_part2.py::build_vk_source_header`
- `telegram_scanned_message`
- `vk_inbox`
- `event`, `event_source`, `joboutbox`
- managed Telegraph pages and `klgdevents` VK wall posts

### Mandatory checks before closure or deploy

- Regression tests for short valid titles (`Идиот`, `№ 13`) not being overwritten by umbrella text.
- Regression tests for Greza-style poster OCR: `9.08` in a follow-up date list must not produce `09:08`.
- Regression tests proving `time_is_default` is hidden in VK source headers while confirmed times remain visible.
- Production DB backup before data repair.
- Delete/clear problematic public VK posts, Telegraph pages, event rows, and joboutbox rows for the affected bad cards.
- Return source rows to re-scan state:
  - Telegram-origin: delete matching `telegram_scanned_message` marks for source/message pairs.
  - VK-origin: reset matching `vk_inbox` rows to `pending`, clear locks and `imported_event_id`.
- Verify prod `/healthz`, relevant runtime logs, and post-repair DB counts.

### Required evidence

- Deployed SHA reachable from `origin/main`.
- Test output.
- Fly deploy evidence.
- Production data-repair SQL/Python output and backup path.
- Confirmation that the reported VK posts/pages/events no longer exist or no longer expose bad data.

## Immediate Mitigation

- Reported bad `klgdevents` posts were deleted:
  `wall-231920894_2362`, `wall-231920894_2351`, `wall-231920894_2345`, `wall-231920894_2344`, `wall-231920894_2316`.
- Related managed posts were also deleted:
  `wall-231920894_2128`, `wall-231920894_2311`, `wall-231920894_2322`, `wall-231920894_2323`,
  `wall-231920894_2326`, `wall-231920894_2328`, `wall-231920894_2329`,
  `wall-231920894_2339`, `wall-231920894_2340`.
- Managed Telegraph pages for the deleted rows were edited to a tombstone page because Telegraph has no delete API.
- Event rows were deleted: `4870`, `5743`, `5746`, `5747`, `5751`, `5753`, `5758`, `5767`, `5769`.
- Telegram source messages were returned to force-scan state:
  `dramteatr39/4361`, `dramteatr39/4375`, `grezahutor/2169`, `signalkld/10924`,
  `prodetstvo_su/3075`, `yantarholl/4644`, `open_fest/48`, `open_fest/603`, `open_fest/606`.
- VK inbox row `8278` (`wall-100137391_164880`) was reset to `pending` with `imported_event_id=NULL`.

## Corrective Actions

- Short, contentful LLM-produced titles are no longer considered bad solely because they are short or numeric/numbered.
- Title fallback skips obvious umbrella/service heading lines instead of replacing a bad title with them.
- Poster OCR date/time extraction now ignores ambiguous dotted date-like tokens without nearby time context.
- VK source headers suppress `time_is_default` while preserving confirmed non-default times.

## Follow-up Actions

- [ ] Add the full LLM-first umbrella-post classifier / poster-schedule extractor split proposed in the Opus consultation.
- [ ] Add a production quality audit command that reports active future service titles, default-time publications, and prose-like locations before public fanout.
- [ ] Add LLM venue-review replay for the remaining non-place location candidates found by the 2026-06-07 audit.

## Release And Closure Evidence

- deployed SHA: `7da618bfdeb731587d8906c9a51eed6a1aa12dae`
- deploy path: clean linked worktree `hotfix/2026-06-07-future-event-quality`, pushed to `origin/main`, deployed manually with `flyctl deploy -a events-bot-new-wngqia --remote-only`
- regression checks:
  - `python -m pytest -q tests/test_tg_candidate_location_grounding.py tests/test_vk_daily.py` -> `36 passed`
  - `python -m py_compile source_parsing/telegram/handlers.py main_part2.py` -> passed
- deploy evidence:
  - Fly image: `events-bot-new-wngqia:deployment-01KTGRBMVAKWKCPS39JBA525C9`
  - Fly machine: `48e42d5b714228`, version `1219`, checks `1 passing`
  - `/healthz`: `ok=true`, `ready=true`, `db=ok`, `issues=[]`
- data repair evidence:
  - Local pre-repair production backup: `artifacts/db/prod_before_INC_2026_06_07_future_event_quality_20260607T100329Z.sqlite.gz`
  - Repair log: `artifacts/codex/prod-future-quality-audit-2026-06-07/prod_repair_apply_20260607T100843Z.log`
  - Post-repair DB check: `remaining_events=0`, VK inbox `8278` is `pending`, all matching Telegram force-message rows exist.
  - VK API verification for the five reported public posts returned `is_deleted=true`.

## Prevention

This incident is now a mandatory regression contract for future event-quality changes. The narrow deterministic guards documented here are safety checks around LLM-owned output; semantic extraction, non-event classification, title choice, venue grounding, and duplicate decisions remain LLM-first.
