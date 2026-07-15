# UI integrator results — typed briefing visual harmony

## Status

- Status: committed and published as an immutable lab.
- Requirement: R05.
- Branch/worktree: `integration/typed-briefing-artist-unusual-20260715` / `/home/dev/projects/events-bot-new-typed-briefing-artist-unusual-20260715-integration`.
- Base: `a9b829d6a865bcf08bc267aa5360103298e461fc`.
- UI implementation/public source: `f7d99384fc7ce308399b5e047ca1ca3ce51737ce`.
- Durable public evidence: `32664fcd64226beedefe198e65f765c960c01587`.

## Delivered

- Restored the complete `announcements-wordmark-ui.svg` in the isolated build and made the build check verify its content.
- Reworked the named-person scene into a flat editorial 4:5 poster composition without a card frame, radius, or shadow.
- Bounded the named-person and weather typography at desktop widths and preserved the visible category/feed entry below the hero.
- Demoted terminal `Показать следующее` to a secondary ghost action so the narrative CTA remains primary.
- Added wordmark, 1366/1440/1920, long-name, crop/grid, overflow, and CTA-hierarchy Playwright gates.
- Published `preview-20260715t2005-briefing-lab-f7d99384` and sent mobile/desktop evidence to Telegram topic `6` in messages `77–82`.

## Verification

- Isolated `build:lab` and `check:lab`: pass; strict six-file allowlist pass.
- Playwright: `11/11` pass.
- Public page and both logo assets: HTTP 200; page robots policy is `noindex,nofollow,noarchive`.
- Gemini 3.1 Pro (High) exact-state sequence: initial `FAIL`, postfix `PASS WITH CONDITIONS`, crop recheck `PUBLISH PASS` with no P0/P1 findings.
- Final Telegram reread after message `82`: no newer incoming comments.

## Residual boundaries

- This is an isolated lab, not a production-homepage rollout or product-desirability validation.
- A human should still judge motion smoothness and perceived jitter on real devices.
- Narrative media remains desktop-only by design in this iteration.
