# L0 placement lane result

- Branch: `agent/ui-golden-event-corpus/l0-placement`
- Base / tested Astro SHA: `a2a1acacfbbcfb955fb914548d7f5655f7de27b9`
- Corpus: `ui-reference-events.v1` (`9ca2960fb8d723f95b19c012b50f8abb5e4503eedcf673fe178ed0f4afd46320`)
- Frozen clock: `2026-08-21T09:00:00+02:00`, `Europe/Kaliningrad`
- Result: **PASS** — 23 implemented scenarios, 0 failures, 6 explicit declared gaps.
- Production source mutation: **false**.

## What is proved

The adapter materialized the immutable eight-event corpus into a disposable copy
of the real Astro `site/src`, ran an actual static Astro build, and read the real
component markers from generated HTML. Desktop `ListingEventCard` and mobile
`MobileListingRailRow` orders both matched the independent source-derived L0
placement oracle:

- `/date-2026-08-22/`: `7807, 7906`
- `/zavtra/`: `7807, 7906`
- `/vyhodnye/`: `7807, 7906`
- `/date-2026-10-21/`: `6399`
- `/date-2026-10-17/`: `3132`
- frozen Today and exhibition-only Date routes: no primary listing cards
- all eight canonical event-detail routes: exact `event.detail` ID marker
- Favorites: production `mergeSavedEventRefs` + `joinFutureSavedEvents`, exact
  event `7906`, `event.card`, `surface=favorites;layout=split-actions`

No production renderer, selector, route or ranking code was modified. The harness
only overwrites its copied preview/current and archive JSON files. Home, Popular,
Unusual, Search, personal feed and Related stay `SKIPPED_DECLARED_GAP`; their
required immutable ranking/manifest/query/persona/anchor inputs are absent from v1.

## Commands run

```bash
node --test tests/ui-surface-placement.test.mjs
UI_REFERENCE_CORPUS_ROOT=/home/dev/.codex/worktrees/lovekgd-design-system/golden-event-corpus-v1/catalog/fixtures/ui-reference-events/v1 \
UI_REFERENCE_NODE_MODULES=/home/dev/.codex/worktrees/events-bot-new/golden-event-corpus-v1/site/node_modules \
node --test tests/ui-surface-placement.test.mjs
node scripts/ui_conformance/verify-surface-placement.mjs \
  --corpus-root /home/dev/.codex/worktrees/lovekgd-design-system/golden-event-corpus-v1/catalog/fixtures/ui-reference-events/v1 \
  --site site --harness /tmp/events-l0-placement-receipt \
  --node-modules /home/dev/.codex/worktrees/events-bot-new/golden-event-corpus-v1/site/node_modules \
  --output .codex/lanes/l0-placement/receipt.json
```

Machine-readable evidence: `.codex/lanes/l0-placement/receipt.json`.
