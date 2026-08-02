# INC-2026-06-29 KGD80 ticket/location drift in future event rows

Status: open
Severity: sev3
Service: Telegram Monitoring / Smart Update event import and future event inventory
Opened: 2026-06-29
Closed: —
Owners: events-bot operators
Related incidents: `INC-2026-06-04-kraftmarket271-tg-monitoring-tpm-import-cancel`, `INC-2026-06-24-future-event-date-default-venue-regressions`, `INC-2026-05-17-future-event-quality-regressions`, `INC-2026-05-11-zoo-lecture-premium-emoji-and-bullet-block-truncation`
Related docs: `docs/operations/incident-management.md`, `docs/operations/runtime-logs.md`, `docs/reports/incidents/README.md`, `docs/llm/request-guide.md`

## Summary

During the KGD80 / «80 историй о главном» future-event audit on 2026-06-29, three already-imported events needed correction before announcement:

- event `5077` (`Калининград и область как кинодекорация...`) stored `ticket_link=https://t.me/confidentmax` instead of the official KGD80 registration URL;
- event `5656` (`Калининград корабельный...`) stored generic `http://kgd80.ru` instead of the official KGD80 registration URL;
- event `4417` (`Калининградский морской торговый порт...`) stored address `Московский проспект 36`; the current official KGD80 page lists `Московский пр-т, 39`.

The source Telegram posts for `5077` and `5656` had correct hidden Telegram `MessageEntityTextUrl` registration links, but import logic failed to prefer those links over generic/fallback values.

## User / Business Impact

- KGD80 announcements and static/event surfaces could send users to a Telegram user or a generic festival homepage instead of the exact registration page.
- One event could show an address that no longer matches the official KGD80 page.
- The defect affects event-quality trust for festival campaign planning and pre-announcement checks.

## Detection

- Detected manually by the operator during the 2026-06-29 KGD80 future-event planning pass.
- Production DB and authenticated Telegram source inspection confirmed the drift.
- Runtime file mirror was checked on 2026-06-29; current mirror covers only the last ~24h, so the original May/June import-time logs for these rows are outside retention.

## Timeline

- 2026-04-29 06:29 UTC: `@kraftmarket39/199` imported as event `4417`; Telegram source text named `Московский проспект 36`.
- 2026-05-18 05:00 UTC: `@kraftmarket39/237` imported as event `5077`; production row later showed chat-author fallback `https://t.me/confidentmax`.
- 2026-06-04 10:51 UTC: `@kraftmarket39/271` scan recorded `events_extracted=1`, `events_imported=0`, `skipped_nochange=1`; event `5656` existed with generic `http://kgd80.ru` registration.
- 2026-06-29: audit found the bad fields; source post entities and DB source/fact rows were collected under `artifacts/codex/kgd80-missing-afishas/`.
- 2026-06-29 09:29-09:35 UTC: production DB rows were backed up and repaired; old managed VK postponed posts `2249`, `2214`, `1974` could not be edited because the VK edit window had expired, so they were deleted and replaced with corrected postponed posts `5006`, `5005`, `5004`.

## Root Cause

1. Telegram source posts use hidden `MessageEntityTextUrl` links: both a generic `https://kgd80.ru/` link on the festival name and a specific `https://kgd80.ru/sobytiya/.../?register=1` link on the “регистрации” word.
2. The Telegram monitor link picker treated `kgd80.ru/sobytiya/.../?register=1` as not strongly ticket-like unless the sliced label contained the full substring `регист`. In the observed posts, custom emoji / UTF-16 entity offsets made the Python-sliced label partial (`гистрации`, `истрации`), so the specific link was not selected when multiple links existed.
3. Server-side `_build_candidate` only inferred a link when `ticket_link` was missing. If LLM extraction emitted generic `http://kgd80.ru`, it was kept. If no link was emitted, the supergroup post-author fallback could set `https://t.me/confidentmax`.
4. Event `4417` address drift is source-vs-official-page drift: the Telegram source text stored and imported `Московский проспект 36`; the current official KGD80 page lists `Московский пр-т, 39`. This part is not confirmed as an importer hallucination.

## Contributing Factors

- `MessageEntityTextUrl` labels are fragile when sliced with UTF-16 offsets against Python strings after custom emoji handling.
- KGD80 official registration URLs were not treated as strong ticket/registration domains in the deterministic link picker.
- Generic festival-domain links were allowed to persist even when the same message carried a more specific registration URL.
- Import-time logs for May/June were outside the current runtime log retention window by the time the audit ran.
- `sync_vk_source_post` treated `edit_vk_post(...)=False`/unavailable edit as an update and wrote a fresh `vk_source_hash` even though VK public text did not change. This repair-path bug was exposed during mitigation.

## Automation Contract

### Treat as regression guard when

- changing Telegram Monitoring link extraction, custom emoji stripping, TextUrl/button handling, or post-author ticket fallback;
- changing Smart Update / server candidate ticket merge behavior;
- importing or repairing `@kraftmarket39` / KGD80 / «80 историй о главном» events;
- preparing KGD80 announcements where registration links matter.

### Affected surfaces

- `kaggle/TelegramMonitor/telegram_monitor.py` hidden/entity/button link mapping;
- `source_parsing/telegram/handlers.py::_infer_ticket_link_from_message_links` and `_build_candidate`;
- production `event.ticket_link`, `vk_ticket_short_url`, `content_hash`, ICS/Telegraph/static-event rebuild surfaces;
- KGD80 official pages as external source of current address truth.

### Mandatory checks before closure or deploy

- Unit/regression tests proving a message with both `https://kgd80.ru/` and `kgd80.ru/sobytiya/.../?register=1` chooses the specific registration link.
- Replay or targeted import-boundary check for saved `@kraftmarket39/237` and `@kraftmarket39/271` source artifacts through Telegram Monitoring/server import + Smart Update on a prod snapshot/shadow DB.
- Production DB post-repair query for events `4417`, `5077`, `5656`.
- Verify public/derived surfaces that consume these rows: Telegraph/static page/ICS and managed Telegram/VK posts if present.
- Release-governance checks if code is deployed: clean branch/worktree or isolated worktree, SHA reachable from `origin/main`, and relevant changelog entry.

### Required evidence

- Source artifact: `artifacts/codex/kgd80-missing-afishas/tg-source-posts-199-237-271.json`.
- Production DB before/after JSON for `4417`, `5077`, `5656`.
- Runtime mirror config/search evidence for 2026-06-29.
- Test output for the KGD80 registration-link regression.
- Deployed SHA and post-deploy verification when closure is attempted.

## Immediate Mitigation

- Production rows should be repaired to official KGD80 links/current address with row-level backups.
- `vk_ticket_short_*` and derived hashes should be invalidated for rows whose ticket/address changed so rebuilt surfaces do not keep stale shortlinks/content.

## Corrective Actions

- Code fix in progress: prefer `?register=1`, `/register`, `/registration`, and `kgd80.ru/sobytiya/...` URLs over generic KGD80/domain links; allow a specific inferred registration URL to replace generic `kgd80.ru` ticket links.
- `sync_vk_source_post` now leaves `vk_sync` incomplete when `wall.edit` is unavailable, instead of logging a successful update/hash on unchanged public text.
- Regression tests added for `@kraftmarket39`-style link sets and generic-to-specific KGD80 replacement.

## Follow-up Actions

- [ ] Replay saved offending source artifacts through the production import boundary + Smart Update on a prod snapshot/shadow DB.
- [x] Repair production DB rows `4417`, `5077`, `5656` and verify managed VK replacements `5006`, `5005`, `5004`.
- [ ] If needed, add a stricter UTF-16-safe TextUrl label helper so future link labels are correct even with custom emoji.
- [ ] Consider a periodic audit that flags generic `kgd80.ru` or Telegram-user ticket links on KGD80 future events when a specific official page exists.

## Release And Closure Evidence

- deployed SHA: —
- deploy path: —
- regression checks: targeted unit tests added; production DB/VK repair verified; replay/deploy still required before closure.
- post-deploy verification: —

## Prevention

- Keep this incident as the regression contract for KGD80 hidden registration links and generic-domain ticket drift.
- Treat exact official registration URLs as more specific than generic source/festival links.
