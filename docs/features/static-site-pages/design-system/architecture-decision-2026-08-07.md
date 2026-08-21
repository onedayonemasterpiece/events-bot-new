# Historical architecture decision redirect — 2026-08-07

> Status: superseded. Do not use as current UI authority or release workflow.

The 2026-08-07 decision kept the canonical component implementation permanently
inside `events-bot-new` and treated `lovekgd-design-system` only as a screenshot
projection/review layer. That was the accepted boundary for prototype 003, but it
was superseded by the per-family authority model, native Resource Graph,
versioned Component Contract/package, and explicit owner round trip.

Current authority:

1. `onedayonemasterpiece/lovekgd-design-system` →
   `docs/ui-source-of-truth-roundtrip.md`;
2. `onedayonemasterpiece/lovekgd-design-system` →
   `docs/component-contract-authority.md`;
3. `onedayonemasterpiece/lovekgd-design-system` →
   `docs/normalization/design-system-family-lifecycle.md`;
4. this repository → `docs/features/static-site-pages/design-system/README.md`
   for consumer, preview, migration, release, and runtime obligations.

Before a family is promoted, exact `events-bot-new` Astro/runtime remains the
source of fact for current AS-IS implementation. After promotion, the versioned
design-system package is canonical and this repository is its pinned consumer.
Penpot is the native visual implementation/review surface of the same contract,
not an independently editable implementation.

The historical decision and prototype evidence remain available in Git history
and `lovekgd-design-system/docs/legacy-experiments.md`.
