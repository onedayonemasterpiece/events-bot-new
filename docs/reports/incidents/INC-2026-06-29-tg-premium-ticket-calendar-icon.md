# INC-2026-06-29-tg-premium-ticket-calendar-icon Telegram premium editor changed date calendars into ticket icons

Status: closed
Severity: sev2
Service: Telegram `@kldevents` / premium emoji editor
Opened: 2026-06-29
Closed: 2026-06-29
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

- [x] Closed after final deploy and post-deploy live verification were recorded below.

## Release And Closure Evidence

- deployed code SHA: `d6681191d0485406d085d48178f36da33917226d` (reachable from `origin/main`; docs-only closure commit may be newer).
- deploy path: manual `flyctl deploy -a events-bot-new-wngqia --remote-only` from clean worktree, HEAD equal to `origin/main`.
- Fly image: `deployment-01KW9BRXCHGMAFZ2BH4MHD9JJR`; machine `683961db016e28`, version `1521`, checks passing.
- regression checks:
  - `pytest -q tests/test_tg_premium_emojis.py tests/test_daily_format.py::test_format_event_daily_marks_tretyakov_with_picture_pair tests/test_daily_format.py::test_format_event_daily_does_not_mark_tretyakov_by_title_only tests/test_daily_format.py::test_format_event_daily_inline_replaces_recent_flag_for_tretyakov tests/test_daily_format.py::test_format_event_daily_marks_rock_concert_with_horns_icon tests/test_tg_event_publish.py::test_build_tg_event_announcement_formats_links_hashtags_and_footer tests/test_tg_event_publish.py::test_tg_event_publish_schedules_premium_editor_after_send tests/test_remote_telegram_session.py` → `27 passed`.
  - `py_compile tg_premium_emojis.py main_part2.py main.py scripts/tg_premium_emoji_editor.py` → passed.
  - `git diff --check` → passed.
  - live `@kldevents` posts `1606…1597` reread with the local fixed editor → `remaining_replacements=0`, date rows use custom `🎟`, ticket rows use `🎫`, `1606` has `🎫 Билеты 💰 1000` and no textual `руб.`.
  - live daily `@kenigevents/4210` repaired before deploy: title-level `👉 🖼🖼 Александр Дейнека...` collapsed to ordinary `👉 🖼️ ...`; compact `03.07 🖼🖼 ...` uses stable same-document pair `5188683852096234620,5188683852096234620`; second dry-run `remaining_replacements=0`.
- post-deploy verification:
  - `/healthz` returned `200`, `ready=true`.
  - Production env check: `ENABLE_TG_PREMIUM_EMOJI_EDITOR=1`, dedicated `TG_PREMIUM_EMOJI_AUTH_BUNDLE` present, delay/jitter/between-edits configured as `150` / `45` / `3,12`.
  - Production code smoke: sample generator/editor text becomes `🎟 30 июня 19:00`, `🎫 Билеты 💰 1000`, `🎫 Билеты 💰 1500`, Tretyakov venue line `🖼🖼 Филиал Третьяковской галереи...`, and title-level `👉 🖼🖼 Александр Дейнека` is cleaned to `👉 🖼️ Александр Дейнека`; custom ids include date `5267071016747690521`, money `5305700407874449437`, Tretyakov pair `5188683852096234620,5188683852096234620`.

## Prevention

- This record is now a mandatory regression contract for Telegram premium emoji editor changes.
