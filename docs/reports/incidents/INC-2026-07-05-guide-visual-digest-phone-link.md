# INC-2026-07-05 Guide visual digest Telegram phone not clickable

Status: mitigated
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
- 2026-07-05 08:54 UTC — initial code fix deployed as `deployment-01KWRQPF8D8MPH787YTARCZ7HK`; `/healthz` ready.
- 2026-07-05 09:01 UTC — follow-up entity send path deployed as `deployment-01KWRR50H87ZTWE0BZE5MYER3N`; `/healthz` ready.
- 2026-07-05 09:06 UTC — final phone-display fallback deployed as `deployment-01KWRRDMH9F1H12NW40BTVMAFT`; `/healthz` ready.
- 2026-07-05 09:06 UTC — existing Telegram messages `@wheretogo39/221` and `@youwillsee39/240` edited in place to visible phone format `+7 (962) 255-54-91`; Bot API returned successful edits. Public embed confirms the new visible phone text, but Telegram public HTML still does not expose `tel:` attributes (`tel_count=0`).
- 2026-07-05 09:10 UTC — user confirmed the phone still was not tappable in Telegram client; root cause expanded from missing code linkification to Telegram client/Bot API caption phone-link limitation.
- 2026-07-05 09:11 UTC — mitigation design changed to guaranteed inline button: `📞 Позвонить: ...` uses an HTTP Bot API button URL pointing at `/call?phone=...`, and the server route redirects/falls back to `tel:+...`.

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

Existing issue `148` Telegram captions were edited in place. Because Telegram Bot API treats equivalent `tel:`/`phone_number` caption entity attempts as not modified or strips them from returned caption entities, the repair also changed the visible number to the more reliably phone-detected Russian format `+7 (962) 255-54-91` while preserving source/title/footer links. After user confirmation that this still was not tappable in Telegram, the mitigation was expanded to an inline `📞 Позвонить` button backed by an HTTPS `/call` redirect to `tel:`.

## Corrective Actions

- Apply `linkify_phones_for_telegram_html()` to generated Telegram visual digest captions after HTML link construction and compacting, then send captions through explicit Bot API `caption_entities` so phone contacts are `phone_number` entities rather than relying on Telegram HTML `tel:` parsing.
- Add a guaranteed Telegram inline call CTA for phone-only visual digest items: `📞 Позвонить: ...` opens an HTTPS `/call?phone=...` route that immediately redirects to `tel:+...` and displays a fallback call link.
- Add a guide visual digest regression test/smoke for phone contacts and call buttons.
- Document the Telegram call-button contract in the guide excursions monitoring feature doc.

## Follow-up Actions

- [ ] Consider a lightweight scheduled/publication smoke that scans freshly published Telegram visual digests for plain phone contacts without `tel:` entities.

## Release And Closure Evidence

- deployed SHA: `dc624ddbf5ae78754c66d9a260610f011c3ab37e` (reachable from `origin/main`)
- deploy path: manual `flyctl deploy -a events-bot-new-wngqia --remote-only` from clean hotfix worktree
- deployed image: `registry.fly.io/events-bot-new-wngqia:deployment-01KWRRDMH9F1H12NW40BTVMAFT`
- regression checks: `python3 -m py_compile guide_excursions/visual_digest.py markup.py`; `git diff --check`; manual helper smoke for Telegram `tel:` internal HTML, Bot API entity conversion, title `text_link` preservation, and VK plain-phone output. Full pytest was blocked locally by missing global `pytest` (`/usr/bin/python3: No module named pytest`).
- post-deploy verification: `/healthz` returned `ok=true`, `ready=true`, `guide_visual_digest=ok`; Bot API edit of `@wheretogo39/221` and `@youwillsee39/240` succeeded with visible `+7 (962) 255-54-91`; public embeds for both messages show the updated phone text. Telegram public embed still reports `tel_count=0`, recorded as Telegram-side rendering limitation rather than code-path absence.

## Prevention

The incident record is the regression contract for future visual digest caption changes; the unit tests and helper smoke keep Telegram phone contacts in a phone-detectable display format with explicit internal `tel:`/entity handling while VK keeps phone numbers plain. Closure should be upgraded from mitigated to closed after a human Telegram-client spot-check confirms tap-to-call on the edited digest.
