# INC-2026-06-29-tg-premium-ticket-calendar-icon Telegram premium editor changed date calendars into ticket icons

Status: mitigated pending final deploy
Severity: sev2
Service: Telegram `@kldevents` / premium emoji editor
Opened: 2026-06-29
Closed: —
Owners: events-bot
Related incidents: —
Related docs: `docs/features/tg-premium-emojis-update/README.md`, `.codex/skills/tg-premium-emojis-update/SKILL.md`

## Summary

The Telegram premium emoji rollout reused the same visible/custom icon semantics for both date/calendar and ticket rows, making public `@kldevents` posts show two calendar-like icons. The corrected product contract is resilient when Telethon/Premium editing is unavailable: generator fallback keeps `📅` for date/calendar and `🎫` for tickets/registration; the editor may premiumize only date/calendar `📅` into custom `🎟`; paid ticket text is `🎫 Билеты 💰 1000` without textual `руб.`.

## User / Business Impact

- Public event posts in `@kldevents` showed two calendar-like icons in the date and ticket/registration rows.
- Users could confuse event dates with ticket metadata.
- The previous design depended too much on the post-publication Telethon editor for user-visible semantics; generator fallback must stay correct if the editor/session/Premium fails.

## Detection

- Detected by operator report for `https://t.me/kldevents/1605` on 2026-06-29.
- No automated regression existed for preserving `📅` date lines while premiumizing ticket rows.

## Timeline

- 2026-06-29 08:43–08:45 UTC: backfill edited recent `@kldevents` posts and converted `📅` to `🎟`.
- 2026-06-29: operator reported duplicated/ambiguous icons on `@kldevents/1605`.
- 2026-06-29: pre-repair snippet for `1605` saved under `artifacts/codex/INC-2026-06-29-tg-premium-ticket-calendar-icon/`.
- 2026-06-29: live repair migrated posts `1606…1597` to final product contract: custom date/calendar `🎟`, ticket/registration `🎫`, paid row `1606` as `🎫 Билеты 💰 1000`.

## Root Cause

1. The first implementation did not enforce a semantic boundary between date/calendar icon and ticket icon.
2. Ticket rows used the same visible/custom emoji family as the date/calendar row, so successful editing produced two calendar-like icons.
3. The ticket-price formatting contract was incomplete: it did not remove `руб.` and did not place the ruble/money emoji before the numeric value.
4. The generator fallback contract was not explicit enough: public posts must remain clear even if Telethon editing, the session, or Premium availability fails.

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

- Unit test proving generator fallback keeps date/calendar as `📅` and ticket/registration as `🎫`.
- Unit test proving editor converts only date/calendar `📅` to custom `🎟`.
- Unit test proving ticket prices become `🎫 Билеты 💰 <number>` and textual `руб.` is removed.
- Live dry-run/idempotence on repaired `@kldevents/1605` or equivalent event post.
- Production `/healthz` after deploy and confirmation `ENABLE_TG_PREMIUM_EMOJI_EDITOR=1`.

### Required evidence

- deployed SHA reachable from `origin/main`;
- targeted pytest output;
- live Telegram post verification after repair;
- Fly app status/health evidence.

## Immediate Mitigation

- Repaired `@kldevents` posts `1606…1597`: date/calendar rows use custom `🎟`; registration/ticket rows use distinct `🎫`; paid row `1606` is `🎫 Билеты 💰 1000`.

## Corrective Actions

- Scope `📅` → custom `🎟` replacement to date/calendar rows only.
- Move ticket/registration rows to distinct ordinary `🎫`, generated before any Telethon edit.
- Add ticket-price specific `💰` insertion and `руб.` suffix removal.
- Update skill/docs/tests/changelog to the corrected contract.

## Follow-up Actions

- [ ] Close after final deploy and post-deploy live verification are recorded below.

## Release And Closure Evidence

Pending final deploy for the current corrected contract. Pre-deploy evidence collected locally on 2026-06-29:

- targeted pytest: `27 passed` for `tests/test_tg_premium_emojis.py`, Tretyakov/rock daily-format tests, Telegram event-publish premium-editor tests, and `tests/test_remote_telegram_session.py`.
- `py_compile tg_premium_emojis.py main_part2.py main.py scripts/tg_premium_emoji_editor.py` → passed.
- live `@kldevents` posts `1606…1597` reread with the local fixed editor → `remaining_replacements=0`, date rows use custom `🎟`, ticket rows use `🎫`, `1606` has `🎫 Билеты 💰 1000` and no textual `руб.`.
- live daily `@kenigevents/4210` repaired before deploy: title-level `👉 🖼🖼 Александр Дейнека...` collapsed to ordinary `👉 🖼️ ...`; compact `03.07 🖼🖼 ...` uses stable same-document pair `5188683852096234620,5188683852096234620`; second dry-run `remaining_replacements=0`.

## Prevention

- This record is now a mandatory regression contract for Telegram premium emoji editor changes.
