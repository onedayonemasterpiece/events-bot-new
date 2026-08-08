# Current UI Decoder v1 — medallions lane

## Result

Implemented a deterministic AS-IS medallion state model pinned to candidate
`ef7aa62e45c60f7a12da6160f490719c0721ec03`.

The lane intentionally stops before browser execution, normalization, component
equivalence, tokenization, Penpot work, or any Astro/CSS/UI mutation.

## Coverage

- Exact `EventTokenMedallions` inputs and defaults:
  - `layout`: `inline | desktop-slots`, default `inline`;
  - `allowTopSlot`: default `true`;
  - roles: `main | secondary`;
  - kinds: `organizer | source | program | pushkin | badge | pill`;
  - identity categories: `venue_brand | festival_brand | festival | organizer`;
  - resolutions: `resolved | conflicting_source_identity | ambiguous_venue_identity`.
- Exact caps and projections:
  - three resolved identities;
  - six visible tokens;
  - first five plus `free-admission` when free would otherwise overflow;
  - desktop removes pill tokens;
  - top slot contains at most the first eligible Main and only when enabled.
- Exact local-media behavior for WebP source plus PNG fallback, WebP without
  fallback, and non-WebP primary assets.
- Explicit responsive/consumer contexts for `1023/1024`, `1279/1280`,
  `1439/1440`, and desktop height `720/721`.
- Eleven representative production-route plans covering event IDs `2601`,
  `5336`, `6856`, `698`, `6994`, `7040`, `6591`, `6562`, `6990`, `5829`, and
  `5278`.
- Fourteen controlled component specimens; combined bounded plan: 25 rows.
- Component-scoped selectors plus separate cascade owners for the component,
  base event layout, desktop consumer, and mobile production consumer.
- Separate `NOT_MERGED` resource candidates for event detail, listing card,
  mobile listing rail, exhibitions, manual medallion lab, and design-system lab.
- Reconciliation records:
  - exact manifest/test count 28 versus stale prior count 27;
  - documentation geometry versus slot-/consumer-scoped source CSS;
  - manual `/lab/medallions` markup is not equivalent to the actual component.
- Four unresolved records retain capture, route binding, computed geometry, and
  cross-surface equivalence as open evidence work.

## Verification

```text
/opt/venvs/events-bot-modern/bin/pytest -q tests/test_current_ui_decoder_v1_medallions.py
........                                                                 [100%]
8 passed in 1.18s

node --check scripts/current_ui_resource_graph/v1/medallions.mjs
git diff --check
```

Tests cover exact/default axes, valid and invalid values, identity/token/top
caps, free retention, desktop pill filtering, image fallback behavior,
responsive boundaries, deterministic serialization, resource separation, and
the no-normalization/no-false-capture invariants.

## Evidence claim boundary

- Browser captures performed by this lane: **none**.
- Private candidate corpus executed by this lane: **no**.
- Production observation claimed by this lane: **no**.
- Specimen and route rows are marked `planned-not-captured` with proof labels
  ending in `not-observed`.
- All candidate records remain `decision=NOT_MERGED`,
  `recommendation=unresolved`, `normalization_allowed=false`.
