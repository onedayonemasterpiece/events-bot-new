# Historical Penpot review flow redirect

> Status: superseded, historical prototype 003 instructions.
> Do not use this document for current Resource Graph or production UI work.

The former plugin-003 AS-IS mirror flow predated the current component authority,
native Resource Graph, family lifecycle, and two-stage owner acceptance gates.
Its statement that `events-bot-new` Git was always the canonical UI source is
valid only as historical pre-promotion AS-IS implementation fact and is not the
current cross-repository operating contract.

Use:

1. `onedayonemasterpiece/lovekgd-design-system` →
   `docs/ui-source-of-truth-roundtrip.md` — canonical round trip;
2. `onedayonemasterpiece/lovekgd-design-system` →
   `docs/component-contract-authority.md` and
   `docs/normalization/design-system-family-lifecycle.md` — authority and gates;
3. this repository → `docs/features/static-site-pages/design-system/README.md`
   and `.codex/skills/static-site-design-system/SKILL.md` — consumer,
   preview, migration, and release obligations.

Historical prototype details remain available in Git history and in the
design-system repository's `docs/legacy-experiments.md`; they are evidence, not
current instructions.
