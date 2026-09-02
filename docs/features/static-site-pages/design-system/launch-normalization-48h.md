# UI normalization launch — Astro implementation route

Status: `ACTIVE_PROGRAMME_CANDIDATE`  
Coordination: `onedayonemasterpiece/events-bot-new#621`  
Implementation branch: `integration/ui-normalization-launch-20260902`  
Base: `61f7a6af5f5e82515dcd42c93dd02748297112bc`

Canonical detailed programme and thin S:

```text
repository: onedayonemasterpiece/lovekgd-design-system
branch: integration/launch-normalized-sot-penpot-20260902
paths:
  docs/launch-normalization/README.md
  docs/launch-normalization/STATUS.md
  docs/launch-normalization/CONSULTANT-K0.md
  contracts/launch-normalized-ui.v1.yaml
```

## Launch authority

For this two-day programme, this repository owns the executable normalized UI:

- `site/src/styles/design-system.css` — foundations and semantic tokens;
- `site/src/components/design-system/` — shared primitives;
- `site/src/components/` — product components and component families;
- `site/src/layouts/` and `site/src/pages/` — route compositions;
- `/lab/design-system/` and launch lab routes — owner/browser review;
- existing local, production, secret-candidate and Kaggle build paths.

`lovekgd-design-system` stores stable IDs, variant/state decisions, Golden
Corpus bindings, Penpot placement and visual status. It does not hold a second
independently edited Astro implementation during the launch.

Penpot is the native visual projection and parity surface. Browser design
review is not blocked by Penpot availability.

## Normalization, not current-code copying

Current Astro is `A0`: useful and largely implemented, but historically
fragmented. Each family must be normalized before it becomes the target:

```text
bounded consumer/drift census
→ component/variant/state/composition decision
→ central foundations and family implementation
→ migrate current consumers
→ Golden browser review
→ thin S update
→ native Penpot projection
→ V0 visual verdict
```

Do not recreate the design system from scratch. Reuse the current runtime
catalogue, current checks, decoder/synthesis donors, PR #37/#42/#43/#52 and
healthy old-Penpot assets/anatomy.

## Golden review before real-data review

The first owner-visible data plane is a fixed Golden Corpus with a frozen
Kaliningrad clock:

- one single-date listing family on Friday;
- the same family on Saturday;
- the same family on Sunday;
- one weekend/two-day page reusing the Saturday/Sunday occurrences;
- target event density `5 / 6 / 5`;
- one free-collection page derived from the same event corpus.

Preferred lab routes:

```text
/lab/launch/
/lab/launch/date-friday/
/lab/launch/date-saturday/
/lab/launch/date-sunday/
/lab/launch/weekend/
/lab/launch/free-collection/
/lab/design-system/
```

Equivalent stable routes are allowed, but exact clickable review URLs are
posted to issue #621. Production events are reviewed only after the Golden
surfaces are usable.

## Work ownership

- `N0`: docs, Golden Corpus/lab, integration and release;
- `F0`: foundations, primitives, icons and brand;
- `M0`: MediaFrame and component/card families;
- `A0`: shell, listings and route archetypes;
- `V0`: independent browser/Penpot visual review;
- `K0`: detailed consultant and prompt author;
- `R0`: Codex implementation worktrees and sole Penpot writer.

The owner is not the message bus. Meaningful results, review links and real
blockers are posted to issue #621.

## Existing build routes

The launch reuses the existing commands and implementation rather than creating
another release system:

```text
npm run build
npm run build:preview
npm run build:production
npm run build:secret-candidate
npm run check:design-system
npm run check:preview
npm run check:production
npm run check:browser-release
npm run check:secret-candidate
```

The release lane must reproduce a current real-data build early, repair the
first actual blocker at its component owner and then run the existing Kaggle
secret-candidate path on the integrated normalized SHA.

## Explicit exclusions

Do not introduce into the critical path:

- a new component package/repository;
- another decoder or global audit;
- a new orchestration generation;
- per-candidate provider/lease cryptography;
- mandatory MAT→QA→INTEGRATE→PUBLISH approval chains;
- bespoke Penpot runner frameworks for each family;
- page/root/instance micro-checkpoints;
- full old-Penpot reconstruction;
- owner-operated result forwarding.

A meaningful checkpoint is an owner-visible browser page, Penpot family/page or
real-data review candidate, not a commit or empty canvas.
