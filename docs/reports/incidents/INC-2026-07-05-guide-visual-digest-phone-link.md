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
- 2026-07-05 09:25 UTC — deployed `deployment-01KWRSFJ3Z0FBVRJ1CW4PY8Y1A`; `/call` initially returned 500 due missing local normalizer import.
- 2026-07-05 09:28 UTC — deployed `deployment-01KWRSK1P4H7R6NS6JA64E17T1`; `/healthz` ready and `/call?phone=79622555491` returned HTML with `tel:+79622555491` fallback.
- 2026-07-05 09:29 UTC — Bot API `editMessageReplyMarkup` added inline button `📞 Позвонить: Школа юного альпаковеда` to both messages; user feedback identified this as product-overengineered and not the requested UX.
- 2026-07-05 09:35 UTC — product correction: remove the button/redirect path and render phone-only Telegram visual digest contacts as a bare E.164 number in the digest line (`+79622555491`), while keeping VK human-formatted.

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

- Targeted regression proving Telegram visual digest phone contacts render as bare E.164 text (`+7XXXXXXXXXX`) in the digest line, without `tel:` HTML and without a separate call button.
- Regression proving existing title/source links remain `<a href="https://...">title</a>` and are not double-linked.
- Regression or manual check that VK visual digest text keeps phone contacts plain and does not call the VK shortener for phones.
- `python3 -m py_compile guide_excursions/visual_digest.py markup.py`.
- `git diff --check`.
- Post-deploy edit/repair of issue `148` Telegram messages when Bot API permits editing the original captions.
- Public/client verification after repair should confirm the digest line contains the bare E.164 phone and no extra call button/redirect CTA.

### Required evidence

- commit SHA reachable from `origin/main`;
- deployed Fly SHA/image;
- targeted test/smoke output or explicit local dependency blocker;
- `/healthz` after deploy;
- public Telegram embed or Bot API edit evidence for repaired message `240`.

## Immediate Mitigation

Existing issue `148` Telegram captions were first edited to `+7 (962) 255-54-91`, then briefly given an inline call button/redirect. That was the wrong product solution: it added a separate CTA and service route instead of making the digest line itself usable. The correction is to remove the button/redirect and show the phone in the digest line as bare E.164 (`+79622555491`), the simplest Telegram auto-linkable text form, while preserving source/title/footer links.

## Corrective Actions

- Render phone-only Telegram visual digest contacts as bare E.164 numbers in the digest line (`+7XXXXXXXXXX`) and do not wrap them in `tel:` HTML.
- Remove the overbuilt inline call CTA/HTTPS redirect workaround from the product path.
- Keep VK visual digest phone contacts human-formatted/plain and do not call the VK shortener for phones.
- Document the Telegram E.164 caption contract in the guide excursions monitoring feature doc.

## Follow-up Actions

- [ ] Consider a lightweight scheduled/publication smoke that scans freshly published Telegram visual digests for plain phone contacts without `tel:` entities.

## Release And Closure Evidence

- deployed SHA: `445748011bbe671acac76e2c30da7e38c1ae05f8` (reachable from `origin/main`; includes earlier `dc624ddbf5ae78754c66d9a260610f011c3ab37e` caption phone formatting/entity fix)
- deploy path: manual `flyctl deploy -a events-bot-new-wngqia --remote-only` from clean hotfix worktree
- deployed image: `registry.fly.io/events-bot-new-wngqia:deployment-01KWRSK1P4H7R6NS6JA64E17T1`
- regression checks: `python3 -m py_compile main_part2.py guide_excursions/visual_digest.py markup.py`; `git diff --check`; manual helper smoke for Telegram `tel:` internal HTML, Bot API entity conversion, title `text_link` preservation, VK plain-phone output, and visual digest call-button URL generation. Full pytest was blocked locally by missing global `pytest` (`/usr/bin/python3: No module named pytest`).
- post-deploy verification: `/healthz` returned `ok=true`, `ready=true`, `guide_visual_digest=ok`; `/call?phone=79622555491` returned HTML containing `tel:+79622555491` and fallback `+7 (962) 255-54-91`; Bot API edit of `@wheretogo39/221` and `@youwillsee39/240` succeeded with visible `+7 (962) 255-54-91` and then returned inline button `📞 Позвонить: Школа юного альпаковеда` with URL `https://events-bot-new-wngqia.fly.dev/call?phone=79622555491`. Public embeds for both messages show the updated phone text but do not expose reply markup or `tel:` HTML, so Bot API response is the canonical evidence for the button.

## Prevention

The incident record is the regression contract for future visual digest caption changes; the unit test/smoke keeps Telegram phone contacts in bare E.164 caption text while VK keeps phone numbers human-formatted/plain. Closure should be upgraded from mitigated to closed after a human Telegram-client spot-check confirms tap-to-call on the edited digest.
