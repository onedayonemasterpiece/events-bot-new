# INC-2026-06-12 Future Event Quality LLM-First Repair

Status: open
Severity: sev2
Service: Telegram Monitoring / VK auto-import / Smart Update / public Telegraph, Telegram and VK event surfaces
Opened: 2026-06-12
Closed: —
Owners: Codex / events-bot maintainers
Related incidents: `INC-2026-06-07-future-event-quality-recurrence`, `INC-2026-05-17-future-event-quality-regressions`, `INC-2026-05-09-event-location-alias-free-dup-regressions`, `INC-2026-05-01-future-event-quality-audit`, `INC-2026-05-01-daily-location-drift`, `INC-2026-04-29-bar-bastion-city-jazz-location`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/smart-event-update/README.md`, `docs/llm/request-guide.md`, `docs/llm/prompts.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

The 2026-06-12 production future-event audit found another active batch of public event-quality regressions. Confirmed issues include active rows with malformed dates, source prose or unrelated fragments in `location_name`, date markers persisted as times (`12.06` -> `12:06`, `13.06` -> `13:06`), false `Калининград Сити Джаз Клуб` venue assignments, and duplicate public cards for the same real event. A user screenshot confirmed the Westside Movieclub `Род мужской` duplicate leaking into a Telegram public surface with conflicting venues.

This is a recurrence of already-known future-event quality incidents. Closure requires both production data repair and LLM-first prevention in the primary import/update path, not only SQL cleanup or deterministic fallback recovery.

## User / Business Impact

- Readers see duplicate cards for the same real event in Telegram and Telegraph surfaces.
- Some public rows show unsupported or wrong venues, including `Тёрка` overriding source-grounded `ОКЦ`/`Сигнал`, and false `Калининград Сити Джаз Клуб` defaults.
- Date-marker times such as `12:06` and `13:06` can create misleading calendar/VK/Telegram publication output.
- Malformed active date rows can pollute future-event queries because lexicographic string comparison treats non-ISO text as future.
- Repeated regressions erode trust in `/daily`, `@kldevents`, `klgdevents`, Telegraph event pages, month/day pages and video candidate pools.

## Detection

- Operator requested a production audit of future events on 2026-06-12.
- Production read-only export through Fly SSH found `PRAGMA quick_check=ok`, 571 active future/ongoing rows, 568 ISO-date rows and 3 malformed-date rows.
- Operator later supplied a screenshot showing `@lovekenig` / public Telegram duplicate cards for `Род мужской` on 2026-06-12.
- Targeted City Jazz audit found 3 active/ongoing rows using `Калининград Сити Джаз Клуб`; 2 of them had no source-grounded City Jazz evidence.

Artifacts:

- `artifacts/codex/future-event-quality-audit-2026-06-12/report.md`
- `artifacts/codex/future-event-quality-audit-2026-06-12/future_export.json`
- `artifacts/codex/future-event-quality-audit-2026-06-12/details.json`
- `artifacts/codex/future-event-quality-audit-2026-06-12/westside.json`
- `artifacts/codex/future-event-quality-audit-2026-06-12/city_jazz.json`
- `artifacts/codex/future-event-quality-audit-2026-06-12/opus_consultation.md`

## Timeline

- 2026-06-12 07:54 UTC — attempted full production SQLite snapshot through `scripts/sync_prod_db.sh`; SFTP stalled after Fly-side backup succeeded.
- 2026-06-12 07:57 UTC — stalled SFTP process killed; switched to targeted read-only SQL export through Fly SSH.
- 2026-06-12 07:58 UTC — schema probe returned `quick_check=ok`; `event` table has `lifecycle_status` active marker.
- 2026-06-12 07:59 UTC — active future/ongoing export captured: 571 rows, 568 ISO-date rows, 3 malformed-date rows.
- 2026-06-12 08:01 UTC — selected high-confidence source evidence captured for bad venues, bad times and duplicate clusters.
- 2026-06-12 08:03 UTC — Opus consultation captured for LLM-first remediation plan.
- 2026-06-12 08:03 UTC — Westside screenshot cluster mapped to production rows `5868`, `5933`, `5936`, `5869`, `5934`.
- 2026-06-12 08:06 UTC — City Jazz targeted audit captured; `3999` and `5367` confirmed unsupported City Jazz assignments, `5873` likely valid.
- 2026-06-12 UTC — this incident record opened before code/data repair.
- 2026-06-12 UTC — preventive code path updated: Telegram LLM location-review prompt now instructs event-local venue to beat source default; Telegram handoff now requires grounding for risky City Jazz defaults and lets source-grounded local venues override defaults; VK drops `DD.MM` date markers from time even when other real times exist; Smart Update recalls same-specific-ticket/date/time candidates before location filtering can hide them.
- 2026-06-12 UTC — replay fixture saved under `tests/replays/INC-2026-06-12-future-event-quality-llm-first-repair/sources.json`; targeted regression tests added for Westside default conflict, unsupported/valid City Jazz, VK date-marker time, and same-ticket wrong-default duplicate recall.
- 2026-06-12 08:35 UTC — production DB repair applied transactionally after compressed logical backup `/data/db.sqlite.inc20260612_future_quality_full_dump_202606120835.sql.gz`; row-level backup table `incident_future_quality_20260612_event` contains 39 pre-mutation rows; `PRAGMA quick_check` stayed `ok`.
- 2026-06-12 08:35-08:38 UTC — affected `telegraph_build` jobs rebuilt active event Telegraph pages; post-repair probe showed no active known malformed-date rows and no active unsupported City Jazz rows.
- 2026-06-12 08:39 UTC — public duplicate cleanup removed managed VK posts for archived duplicate rows `5156`, `5263`, `5410`, `5636`, `5641`, `5645`, `5848`, `5871`, `5890`, `5906`, `5933`, `5934`, `5936`; `PRAGMA quick_check` stayed `ok`.
- 2026-06-12 08:39-08:41 UTC — Telegram duplicate posts for archived rows `5871`, `5890`, `5906`, `5933`, `5934` were deleted. Telegram refused deletion of archived row `5848` (`Bad Request: message can't be deleted`), so its caption was edited to `Снято как дубль. Актуальная карточка: https://t.me/c/3954607218/115`.

## Confirmed Candidate Inventory

### Malformed `date`

| Event | Current data | Evidence | Expected action |
| --- | --- | --- | --- |
| `1` | `date='лекция'`, `time='12 июля 16:00'` | Legacy source text only. | Reprocess if source/year can be grounded; otherwise archive malformed row. |
| `107` | `date` contains unrelated Teatro HD text; actual source says `26-27.07`, row added in 2025. | `source_post_url=https://t.me/kenigevents/1236`. | Archive if 2025 event; recreate clean ISO rows only if current annual event is source-grounded. |
| `117` | `date='21.07.2025'`, active. | Past lecture, no source URL. | Archive. |

### Bad venue/location fields

| Event | Current bad public data | Source-grounded target / note |
| --- | --- | --- |
| `3986` | `location_name` and `location_address` are exhibit-description prose. | KOIHM source; recover `Историко-художественный музей`, `Клиническая 21`, or archive if source is not an event announcement. |
| `4352` | `location_name='которые сейчас представлены...'`. | Tretyakov source is a wallpaper/content post; recover branch venue only if event-like, otherwise archive. |
| `4584` | Barn venue is curator-description sentence. | Source has `Барн, Каштановая аллея 1а`; merge with canonical Barn row if present. |
| `4984` | `location_name` is a chess-program title fragment. | KOIHM Night of Museums; public venue should be KOIHM, not program fragment. |
| `5805`, `5848` | Farm trip rows use title/prose as venue; same ticket/source. | Merge to one row; recover source-grounded farm/transfer facts or leave venue empty for LLM review. |
| `5931` | `location_name='который пришёлся не ко времени'`. | Source says `кирха Гердауэн`. |
| `5791` | `location_name='🔹Выставка «С чего начинается Родина»'`. | KOIHM concert; venue should be KOIHM, not adjacent exhibition bullet. |
| `5749` | `location_name='о котором сегодня говорит музыкальная Россия'`. | Source says closing gala at `замок Тапиау`. |
| `3999` | False `Калининград Сити Джаз Клуб, Мира 33-35`. | Full `terkatalk/4735` source has only `уютный кабинет в центре города`; remove venue or leave empty pending review. |
| `5367` | False `Калининград Сити Джаз Клуб`. | VK source has no venue/address; remove venue or leave empty pending review. |
| `5873` | `Калининград Сити Джаз Клуб`. | qTickets/poster gives `пр-т Мира, 33`; likely valid negative control. |

### Bad time

| Event | Current time | Evidence | Expected action |
| --- | --- | --- | --- |
| `5285` | `12:06` on `2026-06-12`. | Source date marker `12.06`; poster says `сбор 19:00`, `начало 20:00`. | Set `time=20:00`. |
| `5890` | `13:06` on `2026-06-13`. | Same ticket/source as `5942`; source says `13.06 в 15:30 — «На причале»`. | Merge/drop `5890`, keep `5942` at `15:30`. |

### Duplicate / split cards

| Cluster | Why confirmed | Expected action |
| --- | --- | --- |
| `5868`, `5933`, `5936` | Same `Род мужской`, same 2026-06-12 20:30, same Timepad ticket; screenshot confirms public duplicate. `5933` has wrong venue `Тёрка` while its own source says `Новый ОКЦ, Горького 116`. | Keep one `ОКЦ на Горького` row; merge/archive others; rebuild surfaces. |
| `5869`, `5934` | Same `Солнцестояние`, same 2026-06-13 20:00, same ticket; `5934` has wrong `Тёрка`, source says `Сигнал, Леонова 22`. | Keep one `Сигнал` row; merge/archive duplicate. |
| `5906`, `5922` | Same День России Agropark program/date/time/venue. | Keep one canonical event and merge sources. |
| `5564`, `5741` | Same Kantata `Сказка о царе Салтане` at `Понарт`. | Keep one ticket/source-rich row. |
| `5641`, `5857` | Same organ+guitar concert, performers, venue, date/time. | Keep one LLM-reviewed title/program. |
| `5156`, `5263`, `5636`, `5645`, `5728` | Same Russian baroque / Pratum Integrum Kantata concert at `Замок Нойхаузен`. | Collapse to one 19:00 row. |
| `5203`, `5410` | Same Egor Kadnikov / Pianissimo concert. | Keep official/ticketed row. |
| `5050`, `5051` | Same source/date/time/venue/ticket for Vasya Shakulin show. | Keep one source-grounded title. |
| `5828`, `5871` | Same symphony concert at Dramteatr. | Keep official/ticketed row. |

Known negative controls:

- Dramteatr pairs such as `4819/5039`, `4820/5041`, `4821/5042`, `4824/5127`, `4825/5128` are distinct productions sharing date/time/venue area and must not be collapsed.
- `5873` is a source-grounded City Jazz row and must not be removed by a City Jazz blacklist.

## Root Cause

1. LLM producer / location-review triggers still under-fire for source-non-grounded sentence fragments, channel-default venue conflicts, and known risky default venues such as `Калининград Сити Джаз Клуб`.
2. Some import paths still allow malformed or non-ISO `date` values to become active event rows instead of failing closed or routing through LLM rescue.
3. Date marker protection (`DD.MM` -> not `HH:MM`) is not uniform across all Telegram/VK/parser import paths and still allowed `12:06` / `13:06`.
4. Smart Update duplicate recall/matching does not reliably compare cross-producer rows when one row has wrong/default venue, one lacks ticket, or titles differ by umbrella vs concrete item wording.
5. Production cleanup/audit is still reactive; no reusable future-event quality command prevents these rows before public fanout.

## Contributing Factors

- Several source posts are umbrella schedules with multiple events and venue blocks; wrong channel defaults can override explicit source-local venues.
- Generic ticket URLs and missing tickets make deterministic identity insufficient; duplicate decisions need a source-grounded LLM matching stage.
- Previous incidents added narrow safety guards, but the root LLM-first process still lacked a dedicated venue-default-conflict and duplicate-review pass for these shapes.
- Some rows are legacy malformed data; future queries rely on string dates and can include non-ISO garbage.

## Automation Contract

### Treat as regression guard when

- changing Telegram Monitoring producer prompts/schema, venue review, default-location handling, schedule rescue, or server import boundary;
- changing VK auto-import draft extraction, date/time parsing, default-location use, or Smart Update handoff;
- changing Smart Update create/update duplicate recall, LLM match/create bundle, source attachment, or public field writers;
- changing `docs/reference/locations.md`, `docs/reference/location-aliases.md`, or known risky source defaults such as City Jazz;
- repairing or rebuilding future public event rows, Telegraph pages, Telegram event posts, `@lovekenig`/`@kldevents`, `klgdevents`, month/day pages, or video candidate pools.

### Affected surfaces

- `kaggle/TelegramMonitor/telegram_monitor.py`
- `source_parsing/telegram/handlers.py`
- `vk_intake.py`
- `smart_event_update.py`
- `location_reference.py`
- `docs/llm/prompts.md`, `docs/llm/request-guide.md`
- `event`, `event_source`, `event_source_fact`, `eventposter`, `telegram_scanned_message`, `vk_inbox`, `joboutbox`
- Telegraph event pages, Telegram event posts, daily posts, `@lovekenig`, `@kldevents`, `klgdevents`, month/day pages, video candidate pools

### Mandatory checks before closure or deploy

- Preserve minimal replay fixtures under `tests/replays/INC-2026-06-12-future-event-quality-llm-first-repair/`.
- Replay representative sources through the same production import boundary plus Smart Update on a prod snapshot or shadow DB:
  - Westside `Род мужской` / `Солнцестояние` source-default conflict;
  - City Jazz false default and valid City Jazz negative control;
  - `12.06`/`13.06` date-marker time cases;
  - prose venue rows such as Barn/KOIHM;
  - duplicate clusters and negative distinct-production controls.
- Add tests proving LLM-first review triggers fire for source-non-grounded venue/default conflicts without blacklisting valid venues.
- Add tests proving non-ISO dates are rejected before active row creation.
- Add tests proving `time == DD:MM-of-date` is dropped or replaced only when a separate source-grounded time exists.
- Add tests proving duplicate recall includes wrong/default-venue same-event variants and still preserves legitimate same-time different productions.
- Run targeted tests and `py_compile` for touched modules.
- Check runtime-log file mirror state per `docs/operations/runtime-logs.md`; save fallback evidence if disabled/empty.
- Before production data repair: create production DB backup, apply transactionally, and verify `PRAGMA quick_check`.
- Rebuild affected Telegraph/month/day surfaces and repair/delete/update public Telegram/VK posts where the platform still permits editing; record explicit blockers for posts that cannot be edited.
- Release governance: `git fetch origin --prune`, clean worktree, commit pushed, deployed SHA reachable from `origin/main`, no unreconciled release/hotfix drift.

### Required evidence

- Incident-linked replay fixtures and test output.
- Pre/post DB diff or query output for repaired production rows.
- Source links/API output for each repaired/deleted/merged row.
- Telegraph rebuild evidence and Telegram/VK edit/delete evidence or explicit platform-limit blocker.
- Fly deploy evidence and post-deploy `/healthz`.
- Confirmation that the screenshot duplicate cluster no longer appears in regenerated/active public surfaces.
- Confirmation that negative controls remain separate/valid.

## Immediate Mitigation

- Audit artifacts preserved in `artifacts/codex/future-event-quality-audit-2026-06-12/`.
- Confirmed candidate inventory and LLM-first remediation plan documented.
- Production data repair was applied transactionally on 2026-06-12 after full compressed logical backup and in-DB row backup.
- Active public data now has the audited malformed-date rows archived, false City Jazz defaults removed, date-marker times repaired, and confirmed duplicate clusters merged/archived.
- Public Telegram/VK duplicate posts were removed where platform permissions allowed. The only remaining Telegram deletion blocker is event `5848` / message `209`; the post is no longer misleading because its caption was edited to point at survivor `5805`.

## Corrective Actions

- Telegram Monitoring prompt tightened in `kaggle/TelegramMonitor/telegram_monitor.py`: event-local venue lines win over source default; `Калининград Сити Джаз Клуб` defaults require source/OCR/address evidence; `date`/`end_date` must stay ISO-only; `DD.MM` markers must not be emitted as `HH:MM`.
- Telegram candidate handoff tightened in `source_parsing/telegram/handlers.py`: source defaults can be overridden by source-grounded local venues; risky City Jazz defaults are kept only when grounded by name/address; contact/prose strings are not accepted as fallback venues; valid grounded City Jazz remains a negative control.
- VK draft cleanup tightened in `vk_intake.py`: `DD.MM` date-marker times are removed even when the same source also contains other real time tokens.
- Smart Update shortlist recall tightened in `smart_event_update.py`: same specific ticket URL plus overlapping date and no time conflict stays visible to duplicate matching even when a wrong source-default venue would otherwise filter it out.
- Minimal replay fixture committed under `tests/replays/INC-2026-06-12-future-event-quality-llm-first-repair/sources.json`.

## Follow-up Actions

- [x] Codex / current incident / implement LLM-first venue-default-conflict review in primary import path.
- [x] Codex / current incident / implement replay-backed Smart Update duplicate recall/match improvement.
- [x] Codex / current incident / repair confirmed production rows and rebuild public surfaces.
- [ ] Maintainers / no due date / add a reusable future-event quality audit command for active future rows before public fanout.

## Release And Closure Evidence

- deployed SHA:
- deploy path:
- regression checks: `tests/test_tg_candidate_location_grounding.py`, `tests/test_vk_default_time.py`, `tests/test_smart_event_update_duplicate_guards.py` passed (`47 passed`); `py_compile` passed for `source_parsing/telegram/handlers.py`, `vk_intake.py`, `smart_event_update.py`, `kaggle/TelegramMonitor/telegram_monitor.py`; replay fixture JSON validated.
- production data repair: compressed backup `/data/db.sqlite.inc20260612_future_quality_full_dump_202606120835.sql.gz`; row backup table `incident_future_quality_20260612_event` count `39`; final apply artifact `artifacts/codex/INC-2026-06-12-future-event-quality-llm-first-repair/prod_repair_apply_committed.jsonl`; post-repair probe artifact `artifacts/codex/INC-2026-06-12-future-event-quality-llm-first-repair/prod_post_repair_probe.json`; `PRAGMA quick_check=ok`.
- public surface rebuild/edit evidence: `telegraph_build` jobs for active affected rows completed between 08:34-08:38 UTC except row `5942`, whose previous valid Telegraph page remained present while a duplicate rebuild was marked running; managed VK duplicate deletion evidence in `prod_public_cleanup_apply.json`; Telegram duplicate deletion evidence in `prod_public_cleanup_apply.json`; Telegram `5848` delete blocker and caption edit evidence in `prod_tg_delete_5848_retry.json` and `prod_tg_edit_5848_fallback.json`.
- post-deploy verification:

## Prevention

The durable fix must keep semantic decisions LLM-owned: venue grounding, title choice, duplicate identity and free/ticket nuance should be decided by prompt/schema stages with source evidence. Deterministic code may reject invalid schemas, suppress unsupported technical anchors, canonicalize references, and route suspicious rows to LLM review, but must not replace LLM judgment with broad keyword/venue blacklists.
