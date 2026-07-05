# INC-2026-07-05 Telegram Medallion Description Alias Drift

Status: closed
Severity: sev3
Service: Telegram event publishing / Premium custom-emoji medallions
Opened: 2026-07-05
Closed: 2026-07-05
Owners: events-bot operator / Codex
Related incidents: `INC-2026-07-05-tg-afisha-edit-spacing-premium-medallions.md`, `INC-2026-07-05-tg-afisha-vk-dependency-backlog.md`
Related docs: `docs/features/static-site-pages/event-token-medallions.md`, `docs/operations/release-governance.md`

## Summary

Telegram Afisha posts received semantically wrong custom-emoji medallions because ordinary venue aliases were matched against the whole event text, including `description` and `search_digest`, instead of only venue/location fields.

Confirmed affected public posts:

- `@kldevents/1916` (`event.id=6587`, `Таланты и покойники`) had `kant-island` plus `tretyakovka-kaliningrad`; the event location is `Филиал Третьяковской галереи`, while `Остров Канта` / `Кафедральный собор` appeared only as film/destination content in the description.
- `@kldevents/1908` (`event.id=6534`, `ROCK N ROLL CITY...`) had `yantar-hall` plus `simfoniya-vetra`; the event location is `Янтарь холл`, while `Симфония ветра` was a festival/program label and should not be rendered as a second Telegram venue medallion for this post.

## User / Business Impact

- Telegram subscribers saw incorrect venue/partner visual cues in event posts.
- The canonical event location text was correct, but the medallion row contradicted or over-specified it.
- The defect affects trust in Telegram enrichment and can recur for any event description mentioning a venue as content rather than as the event place.

## Detection

- Reported by the operator from the public Afisha Telegram channel on 2026-07-05.
- Telethon inspection confirmed wrong custom-emoji document IDs in `@kldevents/1916` and `@kldevents/1908`.

## Timeline

- 2026-07-05 UTC: operator reported wrong medallions in Tretyakovka and ROCK N ROLL CITY posts.
- 2026-07-05 UTC: Telethon scan identified affected posts and slug counts.
- 2026-07-05 UTC: production DB rows confirmed correct canonical locations.
- 2026-07-05 UTC: affected Telegram posts were edited in place to remove false medallions.
- 2026-07-05 UTC: code guard and regression tests were prepared so venue aliases use location fields only.

## Root Cause

1. `tg_medallions.resolve_event_medallions()` matched all item aliases against `_event_haystack()`, which includes title, descriptions, search digest, festival, source URLs and location fields.
2. Venue/location medallion aliases therefore fired on descriptive content, not on the actual event venue.
3. The previous boundary guard prevented substring false positives but did not distinguish semantic fields.

## Contributing Factors

- Telegram medallion config did not carry explicit `match_scope` metadata.
- Tests covered short acronym substring drift, but not venue aliases appearing in event descriptions.
- Repair/backfill scripts made the symptoms visible by enriching recent posts, but the root bug was in selection scope.

## Automation Contract

### Treat as regression guard when

- changing `tg_medallions.py` selection logic;
- changing Telegram medallion config aliases/priorities/scopes;
- adding new venue/festival/organizer medallions to the Telegram custom-emoji pack;
- repairing existing Telegram posts by recomputing medallions.

### Affected surfaces

- `tg_medallions.resolve_event_medallions()`
- `build_tg_event_announcement()` / promo Telegram publication paths
- Premium Telethon medallion editor insertion
- `TG_MEDALLION_CUSTOM_EMOJI_JSON` mapping and aliases
- public `@kldevents` posts

### Mandatory checks before closure or deploy

- Unit tests proving venue medallions are matched only from `location_name` / `location_address` / `city`.
- Regression tests for:
  - Tretyakovka event whose description mentions `Остров Канта` / `Кафедральный собор`;
  - Yanтарь холл event whose festival/description mentions `Симфония ветра`.
- Public Telegram verification for `@kldevents/1916` and `@kldevents/1908` after repair.
- Production smoke after deploy showing the new selection behavior.

### Required evidence

- committed SHA reachable from `origin/main`;
- targeted pytest output;
- Telegram repair receipt under `artifacts/codex/tg-medallion-wrong-repair/`;
- production DB/public post evidence for affected messages.

## Immediate Mitigation

Edited public Telegram posts in place:

- `@kldevents/1916`: replaced two-medallion row with only `tretyakovka-kaliningrad` (`[4,4,4,4]`).
- `@kldevents/1908`: replaced two-medallion row with only `yantar-hall` (`[4,4,4,4]`).

## Corrective Actions

- Add separate location-only haystack for ordinary medallion aliases.
- Keep explicit identity/curated selection for KGD80 and standalone Znanie.
- Add tests for description-only venue mentions and festival/program labels that must not create an extra venue medallion.

## Follow-up Actions

- [ ] Consider adding explicit `match_scope` to every Telegram medallion config item so future pack updates can choose `location`, `identity`, or `curated` deliberately.
- [ ] Add an operator-facing scan for posts where two non-curated venue medallions are present but only one venue appears in the public location line.

## Release And Closure Evidence

- deployed SHA: `bf74aea6` (`fix(tg): constrain medallion venue alias matches`), reachable from `origin/main`.
- deploy path: Fly remote deploy to `events-bot-new-wngqia`, image `deployment-01KWS1C8GB2XJ51VXA15SKGBD6`, machine version `1599`, health `1/1 passing`.
- regression checks:
  - `pytest tests/test_tg_event_publish.py -k 'medallion or album_footer' -q` → `11 passed, 65 deselected`;
  - production smoke inside Fly: Tretyakovka description mentioning `Остров Канта`/`Кафедральный собор` selects only `tretyakovka-kaliningrad`; Rock N Roll City with `festival=Симфония ветра` at `Янтарь холл` selects only `yantar-hall`.
- post-repair verification:
  - `@kldevents/1916` now has only `tretyakovka-kaliningrad` (`[4,4,4,4]`);
  - `@kldevents/1908` now has only `yantar-hall` (`[4,4,4,4]`);
  - repair receipt: `artifacts/codex/tg-medallion-wrong-repair/repair_receipt.json` (not committed).

## Prevention

Venue medallion aliases must not be matched from descriptions, search digests, film/program content, or festival labels. Only actual location fields can trigger ordinary venue medallions; curated event/program medallions require explicit code/config scope.
