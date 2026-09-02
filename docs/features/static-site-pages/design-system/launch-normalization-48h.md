# UI normalization launch — Astro implementation route

Status: `ACTIVE`  
Coordination: `onedayonemasterpiece/events-bot-new#621`  
Implementation branch: `integration/ui-normalization-launch-20260902`  
Base: `61f7a6af5f5e82515dcd42c93dd02748297112bc`

Canonical programme and thin S:

```text
repository: onedayonemasterpiece/lovekgd-design-system
branch: integration/launch-normalized-sot-penpot-20260902
paths:
  docs/launch-normalization/README.md
  docs/launch-normalization/STATUS.md
  docs/launch-normalization/CONSULTANT-K0.md
  contracts/launch-normalized-ui.v1.yaml
```

## Current implementation facts

This repository already contains the executable UI, semantic foundations,
shared components, actual route compositions, preview hub and release tooling.
The launch work normalizes historical drift; it does not create a new design
system from scratch.

Actual date/weekend semantics:

```text
/segodnya/                  current build date
/zavtra/                    next date
/date-YYYY-MM-DD/           arbitrary date
/vyhodnye/                  active/nearest weekend
/vyhodnye/YYYY-MM-DD/       selected available weekend range
```

The first three use the shared `DateListingSurface`; weekend routes use the
distinct `WeekendListingSurface`. Intentional page composition differences are
preserved while shared foundations, cards, media, controls and icons are
normalized.

## Owner review uses fresh real data

Do not create `/lab/launch/*` routes and do not require the owner to review
components in isolation.

The existing preview build exposes one owner entry point:

```text
/<buildId>/__preview/
```

The release/data lane must:

1. export a fresh bounded production-event snapshot through the existing
   exporter;
2. build the existing preview profile on the normalization integration branch;
3. publish the exact clickable `/<buildId>/__preview/` URL to issue #621;
4. rebuild the same preview after each meaningful normalization wave.

The owner opens actual product routes from that hub and checks visual sanity.
The existing `/lab/design-system/` remains unchanged as an internal automated
regression harness only; it is not an owner checkpoint.

## Internal Golden A=S=P

Golden fixtures exist only to make Astro↔Penpot comparisons deterministic.
With a frozen Friday clock, use the actual route implementations:

```text
/segodnya/                            Friday
/zavtra/                              Saturday
/date-YYYY-MM-DD/                     Sunday
/vyhodnye/                            Saturday + Sunday
/podborki/besplatnye-sobytiya/        free subset
```

Target event density is `5 / 6 / 5`, minimum `4 / 5 / 4`. Golden selection is
not an owner-review prerequisite. It supplies identical text, images, states and
dates to Astro and Penpot after a family is normalized.

## Normalization proof

Each family must have:

- one central Astro implementation;
- explicit component/variant/state/composition decision;
- all launch route consumers migrated;
- no page-local visual copy or forbidden internal override;
- central foundation, SVG, icon-size and MediaFrame roles;
- real-data preview rebuilt without visible regression;
- thin S binding to exact sources and consumers;
- native Penpot master/variants and linked route instances;
- internal Golden V0 verdict.

Exactly four semantic icon size roles are required. Concrete dimensions live in
central tokens/utilities; components choose a role and do not hard-code local
icon width/height.

## Existing build and release routes

Reuse, do not replace:

```text
site/scripts/export-production-preview-data.py
npm run build:preview
npm run build:production
npm run build:secret-candidate
npm run check:design-system
npm run check:preview
npm run check:production
npm run check:browser-release
npm run check:secret-candidate
scripts/run_static_site_builder_kaggle.py
```

The current checked-in production catalogue is historical; the programme starts
by producing a fresh snapshot and current preview. Known browser/media failures,
including image containment, are repaired at the shared MediaFrame/component
owner rather than by weakening release checks.

## Work ownership

- `N0`: documentation, fresh real-data export/build, internal Golden corpus,
  integration and release;
- `F0`: foundations, primitives, exactly four icon-size roles, SVG and brand;
- `M0`: MediaFrame and component/card families;
- `A0`: shell, listings and actual route archetypes;
- `V0`: real-data visual sanity and internal Golden Astro/Penpot review;
- `K0`: detailed consultant and prompt author;
- `R0`: Codex implementation worktrees and sole Penpot writer.

The owner is not the message bus. Meaningful results, review links and real
blockers are posted to issue #621.

## Explicit exclusions

Do not introduce:

- owner-facing lab pages or owner Golden-corpus review;
- a new component package/repository;
- another decoder/global audit;
- a new orchestration generation;
- per-candidate provider/lease cryptography;
- mandatory MAT→QA→INTEGRATE→PUBLISH chains;
- bespoke Penpot runner frameworks per family;
- page/root/instance micro-checkpoints;
- full old-Penpot reconstruction;
- owner-operated result forwarding.

A meaningful checkpoint is a fresh real-data preview, a compact normalization
report, a native Penpot master with linked route instances or a release
candidate—not a commit, isolated specimen or empty canvas.