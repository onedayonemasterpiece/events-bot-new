# INC-2026-06-29-tg-premium-ticket-calendar-icon Telegram premium editor changed date calendars into ticket icons

Status: mitigated
Severity: sev2
Service: Telegram `@kldevents` / premium emoji editor
Opened: 2026-06-29
Closed: —
Owners: events-bot
Related incidents: —
Related docs: `docs/features/tg-premium-emojis-update/README.md`, `.codex/skills/tg-premium-emojis-update/SKILL.md`

## Summary

The Telegram premium emoji editor interpreted the requested `📅` replacement too broadly and converted event-post date/calendar lines in `@kldevents` into `🎟`, making date and ticket rows visually ambiguous. The same rollout left paid ticket text as `🎟 Билеты 1000 руб.` instead of the corrected contract `🎟 Билеты 💰 1000`.

## User / Business Impact

- Public event posts in `@kldevents` showed `🎟` both for the date row and ticket/registration row.
- Users could confuse event dates with ticket metadata.
- Paid ticket rows were less clean than intended because textual `руб.` remained.

## Detection

- Detected by operator report for `https://t.me/kldevents/1605` on 2026-06-29.
- No automated regression existed for preserving `📅` date lines while premiumizing ticket rows.

## Timeline

- 2026-06-29 08:43–08:45 UTC: backfill edited recent `@kldevents` posts and converted `📅` to `🎟`.
- 2026-06-29: operator reported duplicated/ambiguous icons on `@kldevents/1605`.
- 2026-06-29: pre-repair snippet for `1605` saved under `artifacts/codex/INC-2026-06-29-tg-premium-ticket-calendar-icon/`.
- 2026-06-29: live repair restored `📅` date rows on posts `1606…1597`; `1606` price became `🎟 Билеты 💰 1000`.

## Root Cause

1. The editor had a global `📅` → `🎟` substitution, so it affected date rows and calendar-link text instead of only ticket context.
2. The ticket-price formatting contract was incomplete: it did not remove `руб.` and did not place the ruble/money emoji before the numeric value.

## Contributing Factors

- Tests covered the wrong contract (`📅` becoming `🎟`) instead of guarding date-line preservation.
- Manual backfill was run before this ambiguity was caught.

## Automation Contract

### Treat as regression guard when

- changing `tg_premium_emojis.py` replacement semantics;
- adding premium emoji rules for `@kldevents` event posts;
- changing Telegram ticket/date rendering in `main_part2.py`.

### Affected surfaces

- `tg_premium_emojis.apply_daily_free_premium_emojis`
- `@kldevents` public event posts
- automatic premium editor scheduled after `publish_tg_event_announcement`

### Mandatory checks before closure or deploy

- Unit test proving `📅` date/calendar text remains visible `📅`.
- Unit test proving ticket prices become `🎟 Билеты 💰 <number>` and textual `руб.` is removed.
- Live dry-run/idempotence on repaired `@kldevents/1605` or equivalent event post.
- Production `/healthz` after deploy and confirmation `ENABLE_TG_PREMIUM_EMOJI_EDITOR=1`.

### Required evidence

- deployed SHA reachable from `origin/main`;
- targeted pytest output;
- live Telegram post verification after repair;
- Fly app status/health evidence.

## Immediate Mitigation

- Repaired `@kldevents` posts `1606…1597`: date rows restored to `📅`; registration rows kept as `🎟`; paid row `1606` changed to `🎟 Билеты 💰 1000`.

## Corrective Actions

- Remove global `📅` → `🎟` replacement.
- Add ticket-price specific `💰` insertion and `руб.` suffix removal.
- Update skill/docs/tests/changelog to the corrected contract.

## Follow-up Actions

- [ ] Close after deploy and post-deploy live verification are recorded below.

## Release And Closure Evidence

- deployed SHA: —
- deploy path: —
- regression checks: —
- post-deploy verification: —

## Prevention

- This record is now a mandatory regression contract for Telegram premium emoji editor changes.
