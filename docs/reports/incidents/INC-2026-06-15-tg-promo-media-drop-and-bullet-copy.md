# INC-2026-06-15-tg-promo-media-drop-and-bullet-copy

Status: open
Severity: sev3
Service: Telegram promo publication (`promo_activity.surface='tg_event_publish'`)
Opened: 2026-06-15
Closed: —
Owners: Codex / events-bot
Related incidents: `INC-2026-06-15-tg-promo-markdown-leak.md`
Related docs: `docs/features/tg-publishing/README.md`, `docs/features/promo-campaigns/README.md`, `docs/operations/release-governance.md`

## Summary

Two explicit Kantata education promo posts in `@kldevents` were published as
text-only messages despite both selected events having stored `event.photo_urls`:

- `https://t.me/kldevents/565` — event `#5885`, promo exposure `#371`, posted
  `2026-06-15 09:47:06 UTC`.
- `https://t.me/kldevents/566` — event `#5900`, promo exposure `#378`, posted
  `2026-06-15 11:31:50 UTC`.

Post `#566` also surfaced a generated Smart Update `### Что важно` bullet-dump
as the primary public copy. Markdown markers were sanitized after the prior
hotfix, but the editorial shape still read like an internal fact list rather
than a crafted promo caption.

## User / Business Impact

- Priority promo campaign posts lost their visual/poster surface in Telegram.
- The text-only output looked less intentional than ordinary media event posts.
- `#566` exposed fallback-like thesis copy instead of a compact generated public
  caption.

## Detection

- Detected by operator report on 2026-06-15 with public post URLs
  `https://t.me/kldevents/565` and `https://t.me/kldevents/566`.
- Public Telegram embed checks confirmed both posts had message text but no
  `tgme_widget_message_photo_wrap` media block.
- Production DB evidence showed both affected events had media at publish time:
  - event `#5885`: `photo_count=4`, `photo_urls` populated.
  - event `#5900`: `photo_count=2`, `photo_urls` populated.
- Runtime file mirror was enabled at `/data/runtime_logs/events-bot.log*` with
  24-hour retention and included scheduler evidence for both promo publishes.

## Timeline

- 2026-06-15 09:47 UTC — promo activity published event `#5885` to
  `@kldevents/565` as text-only because the full promo message exceeded the
  media caption limit.
- 2026-06-15 — Markdown leak in `#565` was fixed by sanitizing Markdown and
  editing the existing text post.
- 2026-06-15 11:31 UTC — event `#5900` published to `@kldevents/566`; the
  Markdown sanitizer worked, but the post remained text-only and the long bullet
  list became public copy.
- 2026-06-15 — operator reported the second media/copy issue.

## Root Cause

1. `publish_tg_promo_event_publication()` only used `sendPhoto` /
   `sendMediaGroup` when `len(message) <= 1024`.
2. Explicit promo activity posts intentionally use the full event body, so
   media-backed events with long descriptions exceeded Telegram's caption limit.
3. The over-limit branch fell back to `sendMessage`, dropping images even though
   `event.photo_urls` were present.
4. The full-body promo path trusted Smart Update descriptions too literally; a
   `### Что важно` section with many generated bullets became the whole public
   text for `#566`.

## Regression Contract

### Treat as regression guard when

- Changing `main_part2.publish_tg_promo_event_publication()`.
- Changing `main_part2.build_tg_promo_event_publication_message()` or media
  caption helpers.
- Changing `promo_activity.surface='tg_event_publish'` runner behavior.

### Must not regress

- If `event.photo_urls` is non-empty, a Telegram promo activity post must not
  silently degrade to text-only only because the full body exceeds Telegram's
  caption limit.
- For overlong promo bodies with media, the publisher must send the photo/album
  with a safe concise caption and `Подробнее` button rather than dropping media.
- Overlong Smart Update bullet sections should not be dumped as the primary
  media caption; caption text should prefer prose-like lines and leave full
  details to the linked Telegraph page.

### Mandatory checks before closure or deploy

- Unit test proving overlong promo text with `photo_urls` sends media, not
  `sendMessage`.
- Unit test proving media captions stay within 1024 chars and do not include a
  long bullet dump.
- Existing Markdown sanitizer regression for promo bodies still passes.
- Post-deploy smoke: production health is OK and public/embed check confirms
  future promo media posts can render as media.
- Decide whether to repair already published `#565` / `#566` by reposting media
  posts, or document that editing a Telegram text post into a media post is not
  possible and reposting would change public URLs.

## Corrective Actions

- Pending: make media mandatory for promo activity posts with `event.photo_urls`.
- Pending: add concise media-caption builder for overlong promo bodies.
- Pending: add regression tests and deploy.

## Release And Closure Evidence

Pending.
