# INC-2026-06-29-tg-premium-rock-emoji-wrong-id Rock premium emoji used the wrong custom symbol

Status: closed
Severity: sev3
Service: Telegram `@kldevents` / premium emoji editor
Opened: 2026-06-29
Closed: 2026-06-29
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

- [x] Closed after final deploy and post-deploy verification were recorded below.

## Release And Closure Evidence

- deployed SHA: `52ab7e4177e3d261e687e2cd8a4d7ab996e97c92` (reachable from `origin/main`; docs-only closure commits may be newer).
- deploy path: manual `flyctl deploy -a events-bot-new-wngqia --remote-only` from clean worktree after rebasing on current `origin/main`.
- Fly image: `deployment-01KW9JTTE8055N1WRYRWB6KVFW`; machine `683961db016e28`, version `1528`, checks passing.
- regression checks:
  - `pytest -q tests/test_tg_premium_emojis.py tests/test_daily_format.py::test_format_event_daily_marks_rock_concert_with_horns_icon tests/test_tg_event_publish.py::test_build_tg_event_announcement_formats_links_hashtags_and_footer tests/test_tg_event_publish.py::test_tg_event_publish_schedules_premium_editor_after_send tests/test_remote_telegram_session.py` → `25 passed`.
  - `py_compile tg_premium_emojis.py main.py main_part2.py scripts/tg_premium_emoji_editor.py` → passed.
  - `git diff --check` → passed.
  - live `@kldevents/1614` after repair: first line remains `🤘 Трибьют группы «АРИЯ»`; custom emoji entity at offset `0` has document id `5404517529362128309`; date/money ids remain correct; `remaining_replacements=0`.
- post-deploy verification:
  - `/healthz` returned `200`, `ready=true`.
  - Production env check: `ENABLE_TG_PREMIUM_EMOJI_EDITOR=1`, dedicated `TG_PREMIUM_EMOJI_AUTH_BUNDLE` present.
  - Production code smoke: `DEFAULT_DAILY_SINGLE_EMOJI_DOCUMENT_IDS["🤘"]` is `5404517529362128309`; a sample visible `🤘` with old wrong entity id `5393556708398225048` is corrected to custom id `5404517529362128309`.

## Prevention

- This record is a regression contract for future rock premium emoji changes.
