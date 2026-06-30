# INC-2026-06-30 Prose Location, Campaign Non-Event, And Daily Duplicate

Status: mitigated
Severity: sev1
Service: Telegram Monitoring / Smart Update / public event fanout / Telegram daily scheduler
Opened: 2026-06-30
Closed: —
Owners: Codex / events-bot maintainers
Related incidents: `INC-2026-04-26-daily-location-fragments`, `INC-2026-06-18-tg-location-prose-still-extracted`, `INC-2026-06-16-tg-location-pianissimo-program-fragment`, `INC-2026-06-16-vk-quality-duplicates-non-events`, `INC-2026-06-30-generic-title-dropped-own-name`, `INC-2026-04-14-daily-delay-vk-auto-queue-lock-storm`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/smart-event-update/README.md`, `docs/operations/cron.md`, `docs/llm/request-guide.md`

## Summary

On 2026-06-30 the operator reported three fresh public-quality regressions:

- `https://t.me/kldevents/1634` / event `6512` published a concert with `location_name="В программе — бессмертные «Ave Maria» Ф. Шуберта"` instead of the source-owned venue `Евангелистко-Лютеранская церковь, Мира 101`.
- `https://t.me/kldevents/1658` / event `6486` published `location_name="И не забывайте"` for `Самосбор клубники` instead of the source-grounded AgroPark Nekrasovo location.
- `https://t.me/kldevents/1667` / event `6522` published the Pushkin-card discount campaign `Акция «Веди родителей в музей»` as an event, although it is not one concrete attendable event.
- Earlier same-day title incident `https://t.me/kldevents/1630` / event `6508` was confirmed as already repaired by `INC-2026-06-30-generic-title-dropped-own-name`.

The same day the daily announcement was posted again at `2026-06-30 16:36 Europe/Kaliningrad` (`14:36 UTC`) to `@kenigevents` and test mirror `@keniggpt` (`/2488`, `/2489`, `/2490`) although the configured slot is `08:00`.

## User / Business Impact

- Public event posts exposed misleading locations, affecting attendance and trust.
- A non-event campaign occupied event-feed, VK, Telegraph, and daily inventory surfaces.
- Daily subscribers saw a duplicate daily announcement outside the scheduled morning slot.

## Detection

- Operator reports with public Telegram links.
- Telethon reads confirmed the public post text before/after repair.
- Production DB inspection mapped posts to events `6508`, `6512`, `6486`, and `6522`.
- Runtime logs confirmed Smart Update/Telegram import decisions and daily scheduler state.
- Production active/future DB audit checked 274 active future/current rows; after repair the prose-location heuristic returned only the two reported rows before mitigation and none after.

## Timeline

- 2026-06-29 23:06 UTC — Telegram source `k_mira101/444` was imported. Runtime log shows deterministic location recovery replaced extracted/default-like `Евангелистко-Лютеранская церковь` with `В программе — ...`; event `6512` was created.
- 2026-06-30 05:40 UTC — `@kldevents/1634` published with the program-line location.
- 2026-06-30 09:56 UTC — `@kldevents/1658` published with the reminder-fragment location.
- 2026-06-30 14:36 UTC — daily scheduler sent a duplicate daily announcement to the production/test channels while DB `channel.last_daily` still showed `2026-06-29`; it was then set to `2026-06-30` after the duplicate send.
- 2026-06-30 14:40 UTC — `@kldevents/1667` published the non-event discount campaign.
- 2026-06-30 UTC — production rows and public surfaces for `6512`/`6486` repaired; non-event `6522` cancelled/silenced and public TG/VK posts deleted.

## Root Cause

1. The Telegram import path still allowed short source-grounded non-location fragments to act as event-local venue overrides. Existing guards covered long prose and some music-program items, but missed section starts such as `В программе — ...` and short reminders such as `И не забывайте`.
2. The source-default/offsite override branch did not consistently re-check inferred `grounded_loc` with the narrow event-local candidate guard, so a fragment found in the source text could overrule a safe default venue.
3. Smart Update eventness review routed weak rubrics/digests to LLM, but did not route campaign/discount/action posts. The LLM dedupe reason for event `6522` already described it as a federal initiative rather than a local concrete event, but that signal was not used as a pre-create eventness gate.
4. Telegram daily scheduler had only an in-process sent/inflight guard. The morning daily send did not persist `channel.last_daily` until much later; while the process lived, the in-memory cache prevented repeats. After a runtime restart/release, that memory was gone and the still-stale DB row allowed a second scheduled send.

## Automation Contract

### Treat as regression guard when

- Changing Telegram Monitoring prompts/schema or server import candidate building, especially `location_name`, `location_address`, source default recovery, and regex/OCR inference.
- Changing Smart Update eventness, candidate creation, duplicate/match reasoning, or campaign/action handling.
- Changing `/daily` / Telegram daily scheduler, channel `last_daily`, deploy/restart behavior, or premium emoji post-send editing.
- Changing public Telegram/VK/Telegraph event fanout for repaired rows.

### Affected surfaces

- `source_parsing/telegram/handlers.py`
- `smart_event_update.py`
- `main_part2.py::daily_scheduler`, `_daily_try_claim`, `_daily_release_claim`
- Production `event`, `event_source`, `eventposter`, `joboutbox`, `channel`
- Public `@kldevents`, `@kenigevents`, test mirror `@keniggpt`, managed VK `klgdevents`, Telegraph pages

### Mandatory checks before closure or deploy

- Telegram import tests for short program/reminder fragments being rejected as venues and source defaults/offsite addresses staying safe.
- Smart Update tests:
  - short prose location such as `И не забывайте` is fail-closed before create;
  - campaign/discount/action candidates route to LLM eventness review and skip when LLM says `non_event`;
  - existing weak digest and real concise invite controls remain green.
- Daily scheduler test proving a successful scheduled claim survives runtime-state reset/restart and blocks same-day duplicates.
- Current/future production audit for known incident classes: prose locations, generic category titles, and campaign/non-event candidates.
- Public repair evidence for `@kldevents/1634`, `/1658`, deleted `/1667`, repaired/confirmed `/1630`, Telegraph and VK where applicable.
- `/healthz` OK after repair/deploy.

### Required evidence

- Source artifacts/replay fixtures for `k_mira101/444`, `agropark39/1885`, `wall-29891284_13962`, and `kulturnaya_chaika/7913`.
- Runtime log excerpts for the prose-location replacements and daily duplicate window.
- Production DB backup table names and after-repair rows.
- Test output and deployed SHA reachable from `origin/main`.

## Immediate Mitigation

- Event `6512` repaired to `Евангелистко-Лютеранская церковь, Мира 101, Калининград`; Telegram `/1634`, Telegraph, and managed VK were edited/reconciled.
- Event `6486` repaired to `АгроПарк Некрасово поле, Гурьевский район, Некрасово`; Telegram `/1658`, Telegraph, and managed VK were edited/reconciled.
- Event `6522` marked `cancelled/silent`; public Telegram `/1667` and managed VK `_5221` were deleted; pending fanout was completed/cancelled.
- Daily duplicate was contained by setting production `channel.last_daily=2026-06-30`; prevention adds a durable per-channel/day guard table.

## Corrective Actions

- Added narrow import-boundary guards for short non-location program/reminder fragments; deterministic code only rejects unsafe venue shapes and routes semantic repair back to existing defaults/LLM stages.
- Added event-local candidate validation to the source-default override path and normalized `Место:/Адрес:/Локация:` prefixes before address inference.
- Added LLM-first eventness routing for campaign/discount/action candidates; the LLM decides event vs non-event.
- Added durable `daily_announcement_guard(channel_id, day_key)` scheduler claims so restarts/releases cannot forget a same-day scheduled send.

## Follow-up Actions

- [ ] Add operator-visible counters/reports for `location_prose_dropped` and campaign eventness decisions.
- [ ] Decide whether zero-send failed daily claims should surface an admin alert/manual catch-up button.
- [ ] Review source default coverage for `agropark39` so future samosбор posts do not depend on Smart Update repair.

## Release And Closure Evidence

Pending deploy evidence.

## Prevention

Semantic/eventness prevention remains LLM-first: regexes only route or fail-close high-risk shapes. Daily duplicate prevention is mechanical/idempotency and uses durable DB state instead of process memory.
