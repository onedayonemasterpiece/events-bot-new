# INC-2026-06-29 kldevents Solenaya Vorona Railway Gates Location Drift

Status: mitigated
Severity: sev2
Service: VK auto-import / Smart Update location normalization / public `@kldevents` + `klgdevents` event posts
Opened: 2026-06-29
Closed: —
Owners: Codex / events-bot maintainers
Related incidents: `INC-2026-04-15-gate-location-and-linked-facts-drift`, `INC-2026-05-16-tg-location-prose-cityjazz-recurrence`, `INC-2026-06-18-tg-location-prose-still-extracted`, `INC-2026-06-24-future-event-date-default-venue-regressions`, `INC-2026-06-26-vk-location-reference-fuzzy-park`
Related docs: `docs/reference/locations.md`, `docs/reference/location-aliases.md`, `docs/llm/request-guide.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

Operator forwarded `https://t.me/kldevents/1619` (`event.id=6481`, «Песни из кинофильмов») as a public event-quality incident. The public post showed `Железнодорожные ворота, Гвардейский проспект 51А, #Калининград`, while the original VK source and qTickets event page ground the event at musical/theatrical lounge `Солёная ворона`, `Железнодорожная 1, Зеленоградск`.

Investigation found the same defect on the whole weekly `Солёная ворона` import from `vk.com/wall-194927034_4720`: public posts `@kldevents/1616`..`1619` and managed VK posts `wall-231920894_4943`..`4946` had the wrong Kaliningrad gate venue/city.

## User / Business Impact

- Four future public event cards sent users to the wrong city and address.
- Ticket links and source text indicated Зеленоградск, but Telegram/VK/Telegraph rendered Калининград, creating a direct navigation risk.
- The defect is a recurrence of the gate-family venue normalization class: a deterministic alias guard overrode a source-grounded venue after the LLM/Smart Update stage had the correct Зеленоградск venue.

## Detection

- Operator forwarded `@kldevents/1619` to ArtKodex without additional comment; under incident workflow, the forward itself was treated as a serious event-quality signal.
- Telethon E2E inspection confirmed the exact public Telegram caption, one-photo media mode, no media group, and neighboring affected posts `1616`..`1619`.
- Production DB, runtime logs, VK API, qTickets pages, and source text were compared before repair.

## Timeline

- 2026-06-28 15:25 UTC — VK crawl collected source post `vk.com/wall-194927034_4720`.
- 2026-06-28 16:32–16:34 UTC — Smart Update created events `6478`..`6481`; runtime logs show incoming candidates had `location=Театральная гостиная Солёная ворона city=Зеленоградск`.
- 2026-06-28 16:34–16:35 UTC — event pipeline built Telegraph/VK jobs; final persisted rows had been canonicalized to `Железнодорожные ворота, Гвардейский проспект 51А, Калининград`.
- 2026-06-29 11:36–12:07 UTC — public Telegram posts `@kldevents/1616`..`1619` were published with the wrong venue.
- 2026-06-29 UTC — operator forwarded `@kldevents/1619`; investigation confirmed root cause, added prevention test/code, and repaired all four affected public event rows/surfaces.

## Root Cause

1. Smart Update produced source-grounded candidates for `Солёная ворона` in Зеленоградск.
2. `_canonicalize_location_fields()` in `smart_event_update.py` ran a deterministic Railway Gates alias guard before the general reference normalizer.
3. `_looks_like_railway_gates_alias()` treated any string containing `железнодорож` as `Железнодорожные ворота`, so the legitimate Зеленоградск street address `Железнодорожная 1` was rebound to the Kaliningrad gate venue.
4. Downstream Telegram/VK/Telegraph publishers trusted the final canonical DB fields and propagated the wrong city/address.

## Contributing Factors

- `docs/reference/location-aliases.md` correctly contains `Солёная ворона => Театральная гостиная Солёная ворона, Железнодорожная 1, Зеленоградск`, but the earlier gate guard short-circuited before reference normalization could preserve it.
- The old `INC-2026-04-15` gate-family fix intentionally added explicit gate handling, but it did not include a negative control for `Железнодорожная` as a street name in another city.
- The weekly source post is a multi-event roundup; one bad normalization hit four public posts at once.

## Automation Contract

### Treat as regression guard when

- changing `smart_event_update.py` location canonicalization, duplicate location normalization, or gate aliases;
- changing `location_reference.py`, `docs/reference/locations.md`, or `docs/reference/location-aliases.md`;
- importing or repairing VK/Telegram sources from `kafe_solenaya_vorona` / `Солёная ворона`;
- auditing public `@kldevents`, `klgdevents`, Telegraph, or calendar surfaces for venue/city drift.

### Affected surfaces

- `smart_event_update.py::_looks_like_railway_gates_alias`, `_canonicalize_location_fields`, `_normalize_location`;
- production `event`, `event_source`, `eventposter`, `joboutbox` rows for events `6478`, `6479`, `6480`, `6481`;
- public Telegram `@kldevents/1616`..`1619`;
- managed VK `https://vk.com/wall-231920894_4943`..`4946`;
- Telegraph event pages and ICS/calendar data for these events.

### Mandatory checks before closure or deploy

- Unit test: `Театральная гостиная Солёная ворона, Железнодорожная 1, Зеленоградск` must not normalize to `железнодорожные ворота` and must preserve the Зеленоградск venue/address.
- Positive controls: `Железнодорожные ворота` and `Гвардейский проспект 51А` must still normalize to the Kaliningrad gate venue.
- Production scan: no active future `Солёная ворона` / `zelenogradsk.qtickets.events` rows remain with `Железнодорожные ворота` or `Калининград` as venue/city.
- Public verification: affected Telegram, VK, and Telegraph surfaces must show `Театральная гостиная Солёная ворона, Железнодорожная 1, Зеленоградск` after repair.
- Runtime file mirror and source evidence must be checked before closure.
- Release-governance: deployed SHA reachable from `origin/main` if code changes are deployed.

### Required evidence

- Telethon read artifact for `@kldevents/1616`..`1619` including media hashes.
- Production DB before/after rows for events `6478`..`6481`.
- Runtime log excerpt showing Smart Update entered with the correct Зеленоградск location and persisted into the wrong gate venue.
- VK API verification of managed posts `4943`..`4946`.
- Telegraph/qTickets/source evidence grounding the correct venue.
- Test output and deployed SHA.

## Immediate Mitigation

- Confirmed the affected target set as events `6478`..`6481` only for the same `Солёная ворона` / Зеленоградск failure pattern.
- Backed up production rows before repair in `codex_backup_20260629_solenaya_railway_gates_*` tables.
- Repaired canonical production rows to `Театральная гостиная Солёная ворона`, `Железнодорожная 1`, `Зеленоградск` and invalidated content/publication hashes for regenerated surfaces.
- Rebuilt/edited Telegraph, Telegram, VK, and calendar/ICS surfaces for the four future events.

## Corrective Actions

- Tightened `_looks_like_railway_gates_alias()` so `железнодорож` only maps to the Kaliningrad Railway Gates when the text explicitly includes `ворота` or the true gate address/landmarks (`Гвардейский проспект 51А`, `Генерала Буткова`).
- Added a regression test for `Солёная ворона` on `Железнодорожная 1` plus existing positive gate controls.
- Documented this incident as an active regression contract.

## Follow-up Actions

- [ ] Add a broader location-consistency report that flags source/ticket city domains (`zelenogradsk.qtickets.events`) contradicting final public city (`Калининград`) before fanout.
- [ ] Consider an LLM-first pre-publication consistency review for high-risk canonicalization changes where a source-grounded city/address is changed by deterministic reference guards.

## Release And Closure Evidence

- deployed SHA: `pending`
- deploy path: `pending` manual Fly deploy from clean branch/worktree
- regression checks: `pending`
- post-deploy verification: `pending`

## Prevention

Deterministic gate normalization is now a narrow guardrail: it still recognizes explicit Railway Gates evidence, but it cannot override a different city venue merely because an address contains the adjective `Железнодорожная`. Semantic venue corrections remain LLM/source-evidence owned; deterministic code only prevents unsafe alias binding.
