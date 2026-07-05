# INC-2026-07-05 Guide visual digest Telegram phone not clickable

Status: open
Severity: sev3
Service: Guide excursions visual digest Telegram publication
Opened: 2026-07-05
Closed: —
Owners: events-bot maintainer
Related incidents: `INC-2026-06-16-tg-phone-links`
Related docs: `docs/features/guide-excursions-monitoring/README.md`, `docs/operations/incident-management.md`, `docs/operations/release-governance.md`

## Summary

Telegram visual guide digest issue `148` published to `@youwillsee39` as message `240` with the booking phone for `Школа юного альпаковеда` rendered as plain text (`+7 962 255-54-91`) instead of a clickable phone link. The regular event Telegram renderer already has a phone linkifier, but the newer `visual_schedule` guide digest caption path did not use it.

## User / Business Impact

- Readers of the Telegram guide digest could not tap the phone contact directly from the post to start a call.
- The affected visible production post is `https://t.me/youwillsee39/240`; the same issue family also publishes to `@wheretogo39`.
- This is a small but user-visible CTA degradation in a scheduled production digest.

## Detection

- Detected by user report on 2026-07-05: the phone was visible in the Telegram digest but not clickable.
- Public Telegram embed check before fix: `https://t.me/youwillsee39/240?embed=1&mode=tme` contained `+7 962 255-54-91`, but `tel_count=0`.
- Production DB probe identified `guide_digest_issue.id=148`, `family=visual_schedule`, target `tg:@youwillsee39:visual`, message id `240`.

## Timeline

- 2026-07-05 08:30 UTC — production scheduled visual guide digest issue `148` created/published.
- 2026-07-05 08:49 UTC — public embed checked: phone present, no `tel:` entity.
- 2026-07-05 08:50 UTC — root cause localized to `guide_excursions/visual_digest.py::build_visual_digest_telegram_text`.

## Root Cause

1. `build_visual_digest_telegram_text()` treated phone contacts as escaped plain text (`title — +7 ...`).
2. The project-wide Telegram phone linkifier (`markup.linkify_phones_for_telegram_html`) was used by the regular event post renderer, but not by the visual guide digest caption renderer.
3. VK and Telegram visual digest text paths intentionally diverge for URL/shortener behavior, so the missing Telegram-only post-processing was not covered by the existing VK phone regression test.

## Contributing Factors

- The visual digest path is newer than the main Telegram event renderer and did not inherit the phone CTA guard.
- Existing tests covered that VK keeps phone numbers plain, but did not assert that Telegram visual digest phones become clickable.

## Automation Contract

### Treat as regression guard when

- changing `guide_excursions/visual_digest.py` Telegram caption generation;
- changing `markup.linkify_phones_for_telegram_html()` / `tel_href_for_phone_value()`;
- changing guide visual digest publication or caption compacting logic.

### Affected surfaces

- `guide_excursions/visual_digest.py::build_visual_digest_telegram_text`
- `publish_visual_digest_to_telegram(... parse_mode="HTML")`
- `markup.linkify_phones_for_telegram_html`
- production `guide_digest_issue` rows with `family='visual_schedule'`
- Telegram channels configured by `GUIDE_VISUAL_DIGEST_TARGET_CHATS` / `GUIDE_DIGEST_TARGET_CHATS`

### Mandatory checks before closure or deploy

- Targeted regression proving Telegram visual digest phone contacts first render as `<a href="tel:+...">visible number</a>` internally and are converted to Bot API `phone_number` caption entities for sending.
- Regression proving existing title/source links remain `<a href="https://...">title</a>` and are not double-linked.
- Regression or manual check that VK visual digest text keeps phone contacts plain and does not call the VK shortener for phones.
- `python3 -m py_compile guide_excursions/visual_digest.py markup.py`.
- `git diff --check`.
- Post-deploy edit/repair of issue `148` Telegram messages when Bot API permits editing the original captions.
- Public embed verification after repair should show `tel:` for `https://t.me/youwillsee39/240` or record Telegram-side limitation/blocker.

### Required evidence

- commit SHA reachable from `origin/main`;
- deployed Fly SHA/image;
- targeted test/smoke output or explicit local dependency blocker;
- `/healthz` after deploy;
- public Telegram embed or Bot API edit evidence for repaired message `240`.

## Immediate Mitigation

Pending code deploy, no safe user-facing workaround was applied manually because the source caption must preserve existing HTML title links and channel-specific footer links.

## Corrective Actions

- Apply `linkify_phones_for_telegram_html()` to generated Telegram visual digest captions after HTML link construction and compacting, then send captions through explicit Bot API `caption_entities` so phone contacts are `phone_number` entities rather than relying on Telegram HTML `tel:` parsing.
- Add a guide visual digest regression test for phone contacts in Telegram captions.
- Document the Telegram `tel:` contract in the guide excursions monitoring feature doc.

## Follow-up Actions

- [ ] Consider a lightweight scheduled/publication smoke that scans freshly published Telegram visual digests for plain phone contacts without `tel:` entities.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

The incident record is the regression contract for future visual digest caption changes; the unit test must keep Telegram phone contacts clickable while VK keeps phone numbers plain.
