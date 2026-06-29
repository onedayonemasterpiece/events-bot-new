# INC-2026-06-29-tg-premium-rock-emoji-wrong-id Rock premium emoji used the wrong custom symbol

Status: mitigated pending final deploy
Severity: sev3
Service: Telegram `@kldevents` / premium emoji editor
Opened: 2026-06-29
Closed: —
Owners: events-bot
Related incidents: `INC-2026-06-29-tg-premium-ticket-calendar-icon.md`, `INC-2026-06-29-tg-premium-tretyakov-composite-pair.md`
Related docs: `docs/features/tg-premium-emojis-update/README.md`, `.codex/skills/tg-premium-emojis-update/SKILL.md`

## Summary

The Telegram premium emoji editor mapped rock-concert `🤘` to the wrong `lovekenigofficial` custom emoji document id. The selected id `5393556708398225048` renders a neighboring cathedral/heart-style symbol rather than the intended rock/guitarist symbol. Public `@kldevents/1614` therefore showed the wrong premium icon for a rock event.

## User / Business Impact

- Public event posts for rock concerts displayed a misleading non-rock premium symbol.
- The visible fallback remained `🤘`, but Telegram clients rendered the wrong custom image.

## Detection

- Detected by operator report for `https://t.me/kldevents/1614` on 2026-06-29.
- Existing tests did not pin the intended `🤘` document id and did not verify correction of older wrong custom-emoji entities.

## Timeline

- 2026-06-29 11:26 UTC: `@kldevents/1614` published.
- 2026-06-29 11:29 UTC: premium editor changed the title icon to custom emoji id `5393556708398225048`.
- 2026-06-29: pack inspection found two `🤘` fallback custom emoji in `lovekenigofficial`: wrong id `5393556708398225048` and intended rock/guitarist id `5404517529362128309`.
- 2026-06-29: live `@kldevents/1614` repaired to document id `5404517529362128309`; second dry-run reported `remaining_replacements=0`.

## Root Cause

1. The emoji pack contains multiple custom emoji with fallback `🤘`.
2. The implementation selected the first matching fallback id instead of the intended rock/guitarist visual.
3. The single-emoji editor treated any existing custom emoji entity as already done, so changing the configured id would not repair old live posts without an explicit correction path.

## Contributing Factors

- Telegram custom emoji fallback text does not distinguish visually different document ids.
- The previous Tretyakov composite incident had the same failure class, but rock mapping did not yet have a pinned visual/id regression check.

## Automation Contract

### Treat as regression guard when

- changing `DEFAULT_DAILY_SINGLE_EMOJI_DOCUMENT_IDS["🤘"]`;
- changing rock-concert title/category icon replacement rules;
- changing `_find_daily_single_emoji_ops` custom-entity idempotence/correction behavior.

### Affected surfaces

- `tg_premium_emojis.DEFAULT_DAILY_SINGLE_EMOJI_DOCUMENT_IDS`
- `tg_premium_emojis.apply_daily_free_premium_emojis`
- Telegram event posts in `@kldevents`
- automatic post-publication premium editor

### Mandatory checks before closure or deploy

- Unit test proving `🤘` maps to document id `5404517529362128309`.
- Unit test proving an existing wrong custom emoji entity for visible `🤘` is corrected in-place.
- Live verification on `@kldevents/1614` or equivalent rock event post: `🤘` has document id `5404517529362128309` and second dry-run reports `remaining_replacements=0`.
- Production code smoke after deploy returning the same id.
- Production `/healthz` after deploy.

### Required evidence

- deployed SHA reachable from `origin/main`;
- targeted pytest/py_compile output;
- Telethon verification of the live post;
- Fly app status/health evidence.

## Immediate Mitigation

- Repaired `@kldevents/1614` to use `🤘` document id `5404517529362128309`.

## Corrective Actions

- Changed default rock `🤘` document id to `5404517529362128309`.
- Updated the single-emoji editor so visible configured emoji with a wrong custom document id are corrected, not skipped.
- Updated skill, feature docs, changelog, and tests.

## Follow-up Actions

- [ ] Close after final deploy and post-deploy verification are recorded below.

## Release And Closure Evidence

Pending final deploy.

## Prevention

- This record is a regression contract for future rock premium emoji changes.
