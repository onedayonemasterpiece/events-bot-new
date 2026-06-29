# INC-2026-06-29-tg-premium-tretyakov-composite-pair Daily Tretyakov premium marker used the wrong emoji pair

Status: mitigated pending final deploy
Severity: sev3
Service: Telegram `@kenigevents` / daily premium emoji editor
Opened: 2026-06-29
Closed: —
Owners: events-bot
Related incidents: `INC-2026-06-29-tg-premium-ticket-calendar-icon.md`
Related docs: `docs/features/tg-premium-emojis-update/README.md`, `.codex/skills/tg-premium-emojis-update/SKILL.md`

## Summary

The daily premium emoji editor used the wrong `lovekenigofficial` document ids for the Tretyakov venue marker. The pack contains a small standalone Tretyakov building thumbnail followed by two adjacent emoji that compose a larger building. The previous fix duplicated the small thumbnail, so the `🖼🖼` marker in the daily announcement showed two small buildings instead of the intended two-part composite.

## User / Business Impact

- The public daily announcement `@kenigevents/4210` showed an incorrect Tretyakov venue marker in the `ДОБАВИЛИ В АНОНС` block.
- The marker remained scoped to the right venue row, but the visual composition was wrong and less recognizable.

## Detection

- Detected by operator screenshot/report on 2026-06-29.
- Existing tests checked only that two custom emoji entities existed, not that the pair used the two-part composite document ids after the small thumbnail.

## Timeline

- 2026-06-29: Tretyakov venue marker was changed from a mixed small/half pair to duplicated small-thumbnail ids.
- 2026-06-29: operator reported the duplicated-thumbnail visual regression.
- 2026-06-29: local pack contact sheet confirmed document ids `5188445640325099838` and `5188470637034758005` are the adjacent two-part composite after small thumbnail `5188683852096234620`.
- 2026-06-29: live `@kenigevents/4210` repaired to the composite ids; second dry-run reported `remaining_replacements=0`.
- 2026-06-29 13:00 UTC: recurrence detected: visible text was correct, but title-level ordinary `🖼️` retained a stale Tretyakov custom entity and the compact `🖼🖼` pair regressed to duplicated small-thumbnail ids.
- 2026-06-29 13:07 UTC: live `@kenigevents/4210` repaired again: title-level `🖼️` has no Tretyakov custom entity; compact row uses ids `5188445640325099838,5188470637034758005`; second dry-run reported `remaining_replacements=0`.

## Root Cause

1. The emoji pack has multiple consecutive `🖼` custom emoji with the same fallback text, but different visual roles.
2. The implementation selected ids by fallback order without distinguishing the standalone thumbnail from the two-part composite.
3. Tests asserted only membership/equality through the default tuple and did not encode the intended visual sequence.
4. The editor did not explicitly clean stale title-level `🖼️` custom entities after the visible title text had already been corrected.

## Contributing Factors

- Telegram custom emoji fallback text hides the difference between the small thumbnail and composite parts in plain text/tests.
- The first live correction focused on avoiding mixed ids and over-corrected to duplicated ids.

## Automation Contract

### Treat as regression guard when

- changing `DEFAULT_TRETYAKOV_EMOJI_DOCUMENT_IDS`;
- changing `TG_PREMIUM_EMOJI_TRETYAKOV_DOCUMENT_IDS` defaults/docs;
- editing daily Tretyakov venue marker behavior in `tg_premium_emojis.py` or `main.py`.

### Affected surfaces

- `tg_premium_emojis.DEFAULT_TRETYAKOV_EMOJI_DOCUMENT_IDS`
- `tg_premium_emojis.apply_daily_free_premium_emojis`
- daily announcements in `@kenigevents`
- `.codex/skills/tg-premium-emojis-update` operational docs

### Mandatory checks before closure or deploy

- Unit test / smoke proving the Tretyakov pair ids are `5188445640325099838,5188470637034758005`.
- Unit test proving title-level ordinary `👉 🖼️ ...` drops stale Tretyakov custom-emoji entities and remains ordinary picture text.
- Live verification on `@kenigevents/4210` or latest daily announcement that the visible `🖼🖼` pair uses these two ids and a second dry-run reports `remaining_replacements=0`.
- Production code smoke after deploy returning the same ids.
- Production `/healthz` after deploy.

### Required evidence

- deployed SHA reachable from `origin/main`;
- targeted pytest/py_compile output;
- Telethon verification of the live daily post;
- Fly app status/health evidence.

## Immediate Mitigation

- Repaired `@kenigevents/4210`: compact row `03.07 🖼🖼 🎹 Концерт Ильи Папояна` now has document ids `5188445640325099838,5188470637034758005`; title row remains ordinary `👉 🖼️ Александр Дейнека...` with no Tretyakov custom entity; second dry-run reported `remaining_replacements=0`.

## Corrective Actions

- Changed default Tretyakov pair ids to `5188445640325099838,5188470637034758005`.
- Updated feature docs, skill notes, and changelog to clarify that the intended pair is the two adjacent composite parts after the small standalone thumbnail.
- Added cleanup for stale title-level `🖼️` custom emoji entities so title pictures cannot retain a Tretyakov premium entity after visible-text cleanup.

## Follow-up Actions

- [ ] Close after final deploy and post-deploy verification are recorded below.

## Release And Closure Evidence

Pending final deploy for recurrence hardening. Previous release evidence remains in git history.

## Prevention

- This record is a regression contract for future Tretyakov premium emoji changes.
