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

## Launch authority

This repository owns the executable normalized UI:

- `site/src/styles/design-system.css` — foundations and semantic tokens;
- `site/src/components/design-system/` — shared primitives;
- `site/src/components/` — product component families;
- `site/src/layouts/` and `site/src/pages/` — actual route compositions;
- existing local, preview, production, secret-candidate and Kaggle build paths.

`lovekgd-design-system` stores only stable IDs, variant/state decisions, Golden
Corpus and route bindings, Penpot placement and visual status. It does not hold
a second independently edited Astro implementation during this launch.

## Owner review is route review

Do not create `/lab/launch/*` pages. Do not require the owner to review shared
components on separate laboratory pages.

Golden fixtures are injected into the actual route templates:

```text
/date-{FRIDAY}/
/date-{SATURDAY}/
/date-{SUNDAY}/
/vyhodnye/{SATURDAY}/
/podborki/besplatnye-sobytiya/
```

The exact URLs may be served from an immutable preview host or build prefix, but
the rendered layouts and components must be the production route
implementations. Issue #621 publishes all clickable links.

The existing `/lab/design-system/` remains an optional internal regression
harness only. It is not an owner checkpoint, is not expanded in this programme
and cannot prove that actual routes reuse one component family.

## Component identity and reuse

For each normalized family:

1. one central Astro implementation owns the visible component;
2. all launch routes use that implementation, not page-local copies;
3. debug/browser evidence exposes `family_id`, version/state and fixture without
   changing layout;
4. source checks reject forbidden local overrides and deprecated consumers;
5. thin S binds the family to exact source paths and actual route consumers;
6. Penpot route boards use linked instances of one native master/variant family;
7. V0 verifies the actual route DOM/bounds against the matching Penpot board.

This is how a medallion, card or control is proved to be the same component on
multiple pages. Visual similarity alone is insufficient.

## Normalization, not copying current drift

Current Astro is `A0`: useful and substantially implemented, but historically
fragmented.

```text
bounded family/consumer census
→ component / variant / state / composition decision
→ normalize central foundations and family implementation
→ migrate actual route consumers
→ review actual Golden routes
→ update thin S
→ create native Penpot master and linked route instances
→ V0 verdict
```

Reuse the current runtime code and checks plus PR #37/#42/#43/#52 and healthy
old-Penpot assets/anatomy. Do not reconstruct from scratch.

## Golden review before real-data review

Use a fixed `Europe/Kaliningrad` clock and three consecutive dates:

- Friday single-date page: target 5 events, minimum 4;
- Saturday single-date page: target 6, minimum 5;
- Sunday single-date page: target 5, minimum 4;
- weekend page reusing exactly the Saturday/Sunday occurrences;
- free collection derived from the same corpus.

The corpus covers materially different media, title/address length, time,
admission, calendar and cancellation/reschedule cases. After owner acceptance,
the same route implementations are built from current production events.

## Work ownership

- `N0`: documentation, Golden Corpus, integration and release;
- `F0`: foundations, primitives, icons and brand;
- `M0`: MediaFrame and component/card families;
- `A0`: shell, listings and route archetypes;
- `V0`: actual-route browser/Penpot visual review;
- `K0`: detailed consultant and prompt author;
- `R0`: Codex implementation worktrees and sole Penpot writer.

The owner is not the message bus. Meaningful results, review links and real
blockers are posted to issue #621.

## Existing build routes

Reuse the existing build/release system:

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

The release lane reproduces a current real-data build early, repairs the first
actual blocker at its component owner and then runs the existing Kaggle
secret-candidate path on the integrated normalized SHA.

## Explicit exclusions

Do not add:

- owner-facing lab pages;
- a new component package/repository;
- another decoder or global audit;
- a new orchestration generation;
- per-candidate provider/lease cryptography;
- mandatory MAT→QA→INTEGRATE→PUBLISH chains;
- bespoke Penpot runner frameworks per family;
- page/root/instance micro-checkpoints;
- full old-Penpot reconstruction;
- owner-operated result forwarding.

A meaningful checkpoint is an owner-visible actual route, linked Penpot route
board or real-data candidate—not a commit, isolated lab specimen or empty
canvas.
