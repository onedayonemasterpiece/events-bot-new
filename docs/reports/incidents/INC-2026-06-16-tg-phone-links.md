# INC-2026-06-16 Telegram phone contacts are not clickable in event posts

Status: open
Severity: sev3
Service: Telegram event publishing (`@kldevents`)
Opened: 2026-06-16
Closed: —
Owners: events-bot maintainers
Related incidents: —
Related docs: `docs/features/tg-publishing/README.md`, `docs/operations/release-governance.md`, `docs/operations/runtime-logs.md`

## Summary

The public Telegram event post `https://t.me/kldevents/589` showed a phone contact as plain text instead of a reliably clickable contact action. The affected event stored `ticket_link='tel:+74012463635'`, but the Telegram event caption formatter converted non-HTTP ticket links to a visible phone string and did not emit an explicit Telegram HTML link/entity. Body phone numbers were also left to Telegram client auto-detection, which is not reliable in bot captions.

## User / Business Impact

- Channel readers could see the registration phone, but could not reliably tap it from the Telegram post.
- Registration friction increased for events where the primary action is a phone call rather than a web ticket/registration URL.
- The defect affects future `@kldevents` event posts with phone-only contacts unless fixed at the publisher boundary.

## Detection

- Reported by user from public post `https://t.me/kldevents/589` on 2026-06-16.
- Production DB inspection confirmed the event row had a phone ticket link:
  - `event.id=6043`
  - `tg_event_post_id=589`
  - `tg_event_post_mode='photo_caption'`
  - `ticket_link='tel:+74012463635'`
  - title: `🎓 Лекция «Янтарная мануфактура: от промышленного гиганта к страницам истории»`

## Timeline

- 2026-06-16: user reports that the phone number in `@kldevents/589` is not active.
- 2026-06-16: production DB row for event `6043` confirms a `tel:` registration link existed before publish.
- 2026-06-16: code audit finds `_tg_event_ticket_line()` rendered non-HTTP ticket links as escaped plain text and `_telegram_event_body_html()` did not linkify phone contacts for Telegram.
- 2026-06-16: hotfix adds normalized `tel:` href generation, Telegram phone linkification outside existing anchors, and Kaliningrad `4012` landline display formatting.

## Root Cause

1. `main_part2._tg_event_ticket_line()` treated phone-only `event.ticket_link` as a display formatting case, not as a Telegram action link; it returned plain escaped text such as `+7 (4012) ...`.
2. Telegram event body formatting relied on Telegram client auto-linking for phone-looking text, which is unreliable in bot messages/captions.
3. `format_tel_link_for_display()` formatted Russian landlines as a generic 3-digit-code number; Kaliningrad `4012` numbers were displayed as `+7 (401) 246-36-35` instead of the more natural `+7 (4012) 46-36-35`.

## Contributing Factors

- Telegram and Telegraph/VK have different link/contact rendering constraints; the shared phone-display helper was not enough for Telegram captions.
- Existing tests covered phone formatting for Telegraph/display, but not Telegram event caption actionability.

## Automation Contract

### Treat as regression guard when

- changing `markup.py` phone/link helpers;
- changing Telegram event caption/body rendering in `main_part2.py`;
- changing ticket/registration link handling for `event.ticket_link`;
- modifying `tg_event_publish` or promo Telegram event publishing surfaces.

### Affected surfaces

- `markup.format_tel_link_for_display()`
- `markup.tel_href_for_phone_value()`
- `markup.linkify_phones_for_telegram_html()`
- `main_part2._telegram_event_body_html()`
- `main_part2._tg_event_ticket_line()`
- `build_tg_event_announcement()` / `build_tg_event_announcement_for_publish()`
- Bot API send/edit calls using Telegram HTML parse mode.

### Mandatory checks before closure or deploy

- Unit tests must prove `tel:+74012463635` is displayed as `+7 (4012) 46-36-35`.
- Unit tests must prove visible phone strings are normalized to `tel:+...` hrefs.
- Unit tests must prove Telegram event announcement ticket lines include an explicit `href="tel:+..."` phone link.
- Unit tests must prove body phone numbers are linkified without touching already existing `<a>` links.
- Production or staging smoke must confirm a current/fixture event caption builds with a clickable phone action.

### Required evidence

- deployed SHA;
- tests/smoke command output;
- production DB evidence for the originally affected event row;
- post-deploy repair or explicit decision not to mutate already published `@kldevents/589`;
- confirmation that the fix commit is reachable from `origin/main`.

## Immediate Mitigation

- No destructive action was taken on the existing post before code fix/deploy.
- Hotfix changes the Telegram publisher boundary so future posts with phone contacts render explicit `tel:` links.

## Corrective Actions

- Added normalized `tel:` href helper for stored `tel:` values and visible Russian phone strings.
- Added Telegram-specific phone linkification that skips existing HTML anchors.
- Changed phone-only ticket/registration rows to render explicit Telegram HTML links.
- Added Kaliningrad `4012` landline display formatting.
- Added regression tests for phone display, normalization, Telegram body linkification, and ticket-line linkification.

## Follow-up Actions

- [ ] After deploy, repair `@kldevents/589` if Bot API edit is safe and can preserve the current media/caption contract.
- [ ] Consider a live Telegram client smoke for phone link rendering across Android/Desktop when the next phone-only event is published.

## Release And Closure Evidence

- deployed SHA: —
- deploy path: —
- regression checks: —
- post-deploy verification: —

## Prevention

- `docs/features/tg-publishing/README.md` now states that phone-only contacts in Telegram event posts must be rendered as explicit `tel:` links.
- The incident record is added as a regression contract for future Telegram event publishing/link handling changes.
