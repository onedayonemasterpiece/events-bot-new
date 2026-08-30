# Static-site ↔ LoveKGD Design System

Status: `CURRENT_OPERATIONAL_BRIDGE / DRAFT_CONTOUR`

Последняя фактическая сверка: `2026-08-29`.

Это локальная точка входа для Astro UI, SoT UI ↔ Penpot parity, component
lineage и reference fixtures. Полная authority находится в
`onedayonemasterpiece/lovekgd-design-system`; этот репозиторий хранит executable
Astro consumer/projection и runtime evidence.

## 1. Central authority

**SoT UI is the central system.** Its current durable form is versioned
contracts/package identities, tokens, behavior contracts, fixture authority,
bindings and receipts in `lovekgd-design-system`.

```text
owner/product decision
→ SoT UI
  ├─→ Penpot native visual projection/review
  └─→ Astro executable projection/consumer
→ structural + visual parity
→ owner acceptance
→ promotion and production migration
```

- Penpot is not a central system, independent SoT or direct source for Astro.
- Astro is not an independently editable visual authority after promotion.
- A change proposed or reviewed in Penpot returns to SoT UI first.
- The desired propagation direction is `SoT UI → Penpot` and
  `SoT UI → Astro`.
- A direct Penpot → Astro copy or Penpot-only correction is forbidden.
- Before promotion, pinned Astro/runtime remains executable evidence of the
  current AS-IS behavior.

Latest owner correction:
`lovekgd-design-system#53/docs/reviews/owner-text-sot-ui-centrality-correction-20260829.md`
(`REV-CHAT-20260829-01` / `OV-59`).

## 2. Current implementation layers

| Layer | Exact source | Meaning |
|---|---|---|
| Historical DS snapshot | `lovekgd-design-system/main@c6419a62af3d73f53e81d95a518fbe62a4a1c942` | not current owner-review state |
| Source-proven AS-IS baseline | Draft PR `lovekgd-design-system#52@b86bab3e91511b3d4bd7d953b22bceb847f02a51` | 17 archetypes / 34 cases; no acceptance/promotion |
| Active SoT/owner-review contour | Draft PR `lovekgd-design-system#53`, branch `fix/penpot-owner-comments-20260826` | current contracts, Penpot readbacks, review routing |
| Component Golden Corpus pilot | Draft PR `lovekgd-design-system#42@7a26772828a5d74a9683c08e7e6774ff15ac61a5` | 8-event identity PASS; visual FAIL |
| Published Astro AS-IS | `events-bot-new/main@8710e56fa3685f6c30a90cd062d532dce0348cce` | executable fact before promotion |
| Active Astro/UI candidate | this Draft PR `#596`, branch `fix/audio-audit-ui-20260828` | bounded candidate; not production |

Fresh-read current heads of PR `#53` and `#596` before work.

## 3. Correct reading of the latest owner voice

The full transcript says that Source of Truth is the center and Penpot is the
instrument that displays component and archetype states. A previous derived
summary falsely converted Penpot's review role into a “central point” thesis.
That interpretation is superseded.

Correct requirements:

- one SoT-governed hierarchy without hidden duplicate implementations;
- component, composed-group and archetype parity from the same SoT version;
- Penpot library masters/state catalogs on bounded pages;
- linked component instances inside archetypes;
- exact fixture identity in every comparison;
- visual and instrumental review before closure.

## 4. Fixture authority and current gap

Target:

```text
one canonical SoT UI fixture authority
→ typed records
→ named scenarios/subsets
→ same IDs and hashes in Astro and Penpot per case
```

Current factual split:

- 8-event component-certification corpus;
- disjoint 5-event archetype-core pool;
- both support bounded tests, but one cross-level Golden Corpus authority is not
  proven.

Status: `SOT_FIXTURE_AUTHORITY_UNIFICATION_OPEN`.

Different entity pools and scenario subsets are allowed. Parallel unlinked event
authorities are not a finished target.

Executable bridge in this repo:

- `site/src/data/design-system-reference-fixtures.json` — generated ID-only
  projection, not an editable fixture authority;
- `site/src/data/designSystemReferenceFixtures.ts` — runtime validation;
- `reference-fixture-scenarios.md` — exact scenario rules and the open
  unification gate;
- tests reject fixture mode in production/secret-candidate builds.

## 5. Current candidate work

### `ListingDiscoveryRail@6`

This Draft PR introduces shared `plane` / `floating-island` surface axes:

- Date and Popular keep the plane presentation;
- Weekend uses a transparent content-sized Floating Island;
- candidate production callers explicitly use `version={6}`;
- v5 remains deprecated comparison until sign-off;
- source-contract/regression and browser computed-style checks pass.

This is a bounded candidate, not universal Floating Island promotion.

### Other owner-review corrections

Event Detail motion/keyboard/continuation, packed rows/crop, FestivalCard
centralization and Penpot lineage receipts are owned by current
`lovekgd-design-system#53` contracts. Do not fork them here as a second norm.

## 6. Agent route

1. Read this bridge and `reference-fixture-scenarios.md`.
2. Fresh-read `lovekgd-design-system#53`, then open:
   - `docs/static-site-design-system-current-state.md`;
   - `docs/reviews/index.md`;
   - `docs/ui-source-of-truth-roundtrip.md`;
   - `docs/ui-reference-fixture-registry.md`;
   - affected contract and newest receipt.
3. Update the SoT owner first.
4. Update Astro and Penpot projections from the same version.
5. Prove structural readback and focused visual parity.
6. Keep owner acceptance, promotion and release as separate gates.

## Forbidden claims

Until gates close, do not claim:

- Penpot is central or directly controls Astro;
- automatic bidirectional Penpot ↔ Astro authority;
- the 8-event and 5-event sets are already one proven Golden Corpus;
- the design system is completely accepted/promoted;
- visual similarity proves lineage;
- Draft PR `#596` is production;
- green tests or `validate()=[]` equal owner acceptance.
