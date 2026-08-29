# Reference fixture scenarios

## Authority and generated bridge

The durable fixture authority is Golden Event Corpus v2 in
`lovekgd-design-system/catalog/fixtures/ui-reference-events/v2/` and the
versioned registry/scenario in
`catalog/fixtures/design-system-reference/v2/`. Astro does not select a new
corpus independently.

The executable bridge is generated, not hand-maintained:

```bash
python3 site/scripts/build-design-system-reference-fixtures-v3.py \
  --ui-sot-root /path/to/lovekgd-design-system
```

It writes:

- `site/src/data/ui-reference-events-v2.json`: full frozen `PreviewEvent`
  payloads, projection, source snapshot and all authority hashes;
- `site/src/data/design-system-reference-fixtures.json`: Astro's small
  scenario/container registry with the same exact pins.

The generator fails on payload, corpus, projection, registry or scenario
drift. Routes select only a scenario ID. A page-local ID array, an edited
payload field, or selecting records from the route's ordinary
`preview-events.json` is forbidden.

Fixture mode is local/preview only and is rejected for `production`,
`secret_candidate` and `secret-candidate` site modes.

## Golden selection contract

Golden Event Corpus v1 remains immutable historical evidence. V2 is an
adjacent current corpus created from the exact production preview exporter at
`events-bot-new@8710e56fa3685f6c30a90cd062d532dce0348cce`, export SHA-256
`7e4ea8f4a6c6273e17d5531ca009b4dfaf184a2328f45832e41b308bfe170032`.

The eight-event general diagnostic corpus is:

`2182, 6711, 7609, 8006, 8200, 7907, 6942, 7020`.

It deliberately covers landscape, square, portrait, 4:5 and 6:7 media;
visual-only and OCR-protected artwork; safe crop and preserved document
framing; single and multi-image payloads; long-title pressure; free and paid
admission. It is not a route taxonomy.

The free-collection factual projection is the smallest five-record subset that
keeps materially different EventCard states while remaining factually free and
active in the September window:

`2182, 6711, 7609, 8006, 8200`.

The repeated green Chernyakhovsk programme posters are an explicit fail-closed
exclusion. Their four asset keys are stored in
`free-collection-september.v1.json`; a future refresh must not silently
reintroduce them. “Take the first five”, reuse the route's current records, or
choose five homogeneous recent posters are invalid selection methods.

The timed events are intentionally early-September because the inspected
production snapshot contained no diverse factually-free timed-event set around
mid-September; the only active free records there were continuing exhibitions.
Replacing the timed pair with those exhibitions would improve date proximity
but destroy diagnostic state diversity. This is a recorded trade-off, not an
accidental first-five choice.

## First executable parity scenario

```bash
PUBLIC_DESIGN_FIXTURE_PROFILE=design-system-reference-v3 \
PUBLIC_UI_SOT_SCENARIO=free-collection-september-desktop-v2 \
PUBLIC_SEARCH_COLLECTION_REFERENCE_DATE=2026-09-01 \
npm --prefix site run build
```

Route: `/podborki/besplatnye-sobytiya/`.

Input fixture order is `2182, 6711, 7609, 8006, 8200`. The production surface
separates timed events from continuing exhibitions, so asserted DOM order is
`8006, 8200 / 2182, 6711, 7609`. Both groups render through
`FreeCollectionSurface` → `OptimizedEventCardGrid` → canonical `EventCard@2`.
The grid must preserve all five cards, equalize height within each row and fill
the available width per row; an optimizer result that silently omits an
incompatible card is a regression.

Acceptance is incomplete until desktop 1280 and mobile 390 are inspected at
native scale in three states: top, after the hero (compact sticky medallion),
and full page. Required evidence includes exact fixture IDs and DOM order, five
rendered canonical cards, zero horizontal overflow, zero console errors, and a
visual comparison with Penpot using the same payloads/assets. Structural
readback is not a substitute for looking at both renderings.

## Superseded evidence

The sets `7030, 7006, 6901, 6996, 6997` and
`7016, 6982, 7018, 6996, 5259` are rejected. They were derived from existing
route/old July data rather than a reviewed current diagnostic corpus. Penpot
boards or screenshots based on them remain historical failure evidence only.
