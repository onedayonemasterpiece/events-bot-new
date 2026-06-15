# INC-2026-06-15-tg-promo-markdown-leak

Status: monitoring
Severity: sev3
Service: Telegram promo publication (`promo_activity.surface='tg_event_publish'`)
Opened: 2026-06-15
Closed: —
Owners: Codex / events-bot
Related incidents: —
Related docs: `docs/features/tg-publishing/README.md`, `docs/features/promo-campaigns/README.md`, `docs/operations/release-governance.md`

## Summary

The Kantata education promo campaign published `https://t.me/kldevents/565`
with raw Markdown service markers in the full body text: `### О спикере`,
`### Формат и темы`, and `*` bullets were visible to readers. The top Telegram
title was already sent as HTML bold, but Markdown sections inside the promo
body were escaped as plain text instead of being formatted.

## User / Business Impact

- Public `@kldevents` subscribers saw editorial service markup in a promoted
  event post.
- The post remained readable and linked to the event page, but the visual
  quality of a priority promo campaign was degraded.
- The same formatter could repeat the issue for future full-text promo
  activity posts whose `event.description` contains Markdown headings/bullets.

## Detection

- Detected by operator report on 2026-06-15 with public post URL
  `https://t.me/kldevents/565`.
- Production evidence:
  - `promo_exposure#371`, campaign `#7 Кантата · образовательная программа · 13-16 июня`,
    activity `#17 tg_event_publish`, event `#5885`, status `TG_PUBLISHED`,
    created `2026-06-15 09:47:06 UTC`.
  - Public embed HTML confirmed raw `###` and `*` markers in the post body.
  - Fly runtime file mirror was enabled and readable at `/data/runtime_logs`.

## Timeline

- 2026-06-15 09:47 UTC — promo activity published `@kldevents/565`.
- 2026-06-15 — operator reported visible Markdown markers in the post.
- 2026-06-15 — production DB and public Telegram embed confirmed the affected
  campaign/activity/event.

## Root Cause

1. `build_tg_promo_event_publication_message()` selected full
   `event.description` for explicit promo activity posts.
2. The body formatter only stripped HTML tags and then escaped the entire body,
   so Markdown headings and bullet markers were treated as literal public text.
3. The formatter had no regression test for Markdown-rich Smart Update
   descriptions in promo activity posts.

## Contributing Factors

- Explicit promo activity posts intentionally use full event text, unlike the
  ordinary managed Telegram event pipeline that builds a short intro.
- Smart Update descriptions can validly contain Markdown-like section headings
  and bullets for rich event pages.

## Automation Contract

### Treat as regression guard when

- Changing `main_part2.build_tg_promo_event_publication_message`.
- Changing `promo_activity.surface='tg_event_publish'` publication flow.
- Changing Smart Update description formatting that can feed Telegram promo
  activity posts.

### Affected surfaces

- `main_part2.py` Telegram promo activity formatter.
- `promo.py` `tg_event_publish` activity runner (publication path).
- Public Telegram channel `@kldevents`.
- Release path to Fly production.

### Mandatory checks before closure or deploy

- Unit test proving Markdown headings/bullets in promo body do not leak as
  literal `###`, `**`, or `*` markers.
- Existing promo runner regression around `tg_event_publish` still passes.
- Correct the already published post `https://t.me/kldevents/565` or document a
  Telegram API blocker.
- Deploy from a clean branch rooted in `origin/main`; deployed SHA must become
  reachable from `origin/main`.
- Post-deploy smoke: public post no longer contains raw Markdown markers, and
  production health is OK.

### Required evidence

- Test command output.
- Edited post evidence from public Telegram embed or Bot API response.
- Deployed SHA and deploy command/path.
- Confirmation that the fix is reachable from `origin/main`.

## Immediate Mitigation

- Edited `@kldevents/565` via the production bot environment on Fly on
  2026-06-15. The edit preserved the existing public infoblock and details
  button, converted body headings to Telegram HTML bold, and normalized bullets
  to `•`.
- Public embed verification after the edit showed no raw `###` or line-start
  `*` bullets, and confirmed bold heading HTML for `О спикере` /
  `Формат и темы`.

## Corrective Actions

- `main_part2.build_tg_promo_event_publication_message()` now converts full
  promo body Markdown headings to safe Telegram HTML, normalizes bullets, strips
  inline Markdown service markers, and escapes the remaining body text.
- Added unit coverage in `tests/test_tg_event_publish.py` for Markdown-rich
  promo body formatting.

## Follow-up Actions

- [ ] Consider adding a periodic public-post smoke for promo activity captions
      that checks for raw `###` / `**` / markdown bullets in `@kldevents`
      outputs.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: `pytest -q tests/test_tg_event_publish.py::test_build_tg_promo_event_publication_formats_markdown_body tests/test_promo.py::test_tg_event_publish_honors_preferred_ids_by_date tests/test_promo.py::test_promo_vk_runner_schedules_publications_and_repost` — 3 passed pre-deploy
- post-deploy verification: pending

## Prevention

- Added unit test coverage for Markdown-rich promo activity body formatting and
  documented the no-raw-Markdown contract in promo/Telegram publishing docs.
