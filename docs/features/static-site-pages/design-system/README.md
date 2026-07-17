# KenigEvents static-site design system

> **Status:** desktop-v14 parity and runtime enforcement implemented on the integration branch; immutable public preview and product/design release sign-off remain pending.
> **Runtime catalog:** `/lab/design-system/` in every checked preview build.
> **Source of truth:** `site/src/styles/design-system.css` + `site/src/components/design-system/`.

This directory is the normative home for reusable visual rules of the KenigEvents static site. Dated research and lab pages remain historical evidence. Production templates must consume the shared tokens and runtime components documented here instead of copying geometry into a page-local stylesheet.

## What is implemented

- one semantic token layer for color, type, spacing, radii, elevation, motion, touch size and content width;
- shared primitive Astro components: `Button`, `Badge`, `Field`, `StatePanel`, `Skeleton`, plus the candidate `CopyAction` clipboard affordance;
- compatibility aliases for established product components, so the system can be adopted without a risky all-at-once rewrite;
- shared token/action adoption by both `EventLayout` pages and the standalone root landing page;
- a built Astro catalog at `/lab/design-system/`, linked from `/__preview/`;
- the accepted production desktop runtime: `DesktopEventPage@14`, `DesktopEventActionPanel@2`, poster/preview compositions and the complete desktop scenario lab;
- the real `AuthorizedEventSearch@2` form with anonymous, ready, progress, skeleton, results, empty, error and quota fixtures;
- real product fixtures for `EventHero`, `EventFacts`, `EventTokenMedallions`, `EventCard` and `EventListItem`; legacy `EventCtaPanel`/standalone `EventMediaRail` are visibly deprecated for desktop-v14;
- visible default, hover, focus, pressed, loading, disabled, empty, error, stale/degraded, success and consent-free states;
- automated source/contrast/state checks in `site/scripts/check-design-system.mjs` and generated-page assertions in `site/scripts/check-preview.mjs`.

The catalog is not a Figma-only representation or a parallel Storybook implementation. It renders the same Astro components and global CSS used by static pages. A visual approval therefore describes runtime code, not a disconnected mock.

## Open and inspect

Local development:

```bash
cd site
npm ci
npm run dev
# http://127.0.0.1:4321/lab/design-system/
```

Checked preview build:

```bash
cd site
npm run check:design-system
PREVIEW_BUILD_ID=preview-<id> npm run build:preview
PREVIEW_BUILD_ID=preview-<id> npm run check:preview
```

In a published preview the route is `<preview-base>/lab/design-system/`. The exact immutable preview URL and SHA are recorded in release evidence; product/design approval must never refer only to “latest”.

## Architecture

### Foundations

`site/src/styles/design-system.css` owns the `--ke-*` tokens. Components use semantic roles (`brand`, `surface`, `danger`, `muted`) rather than raw palette names or page-specific values.

Token families:

| Family | Prefix / examples | Contract |
|---|---|---|
| Color | `--ke-color-*` | Text/background pairs pass WCAG AA for normal text where used together. |
| Type | `--ke-font-*`, `--ke-line-*` | System stack, 16 px body baseline, explicit display/heading/body/label scale. |
| Spacing | `--ke-space-*` | 4 px base scale; no new arbitrary component spacing without review. |
| Shape | `--ke-radius-*` | Small/medium/large/xlarge/pill roles. |
| Elevation | `--ke-shadow-*` | Surface/card/overlay levels; random shadows are not allowed. |
| Motion | `--ke-duration-*`, `--ke-ease-*` | 160–220 ms interaction transitions; reduced-motion fallback is mandatory. |
| Interaction | `--ke-control-min` | Minimum interactive target is 44 px. |
| Layout | `--ke-content-max` | Shared content-width authority. |

Legacy aliases such as `--primary`, `--surface` and `--radius-md` remain temporarily in the same file. They are a migration boundary, not a second token system. When an existing component is touched materially, move it to `--ke-*` roles rather than adding another alias.

### Primitive components

| Component | Runtime path | Required states | Status |
|---|---|---|---|
| Button | `site/src/components/design-system/Button.astro` | default, hover, focus, pressed, selected, loading, disabled; primary/secondary/quiet/inverse/danger | approved |
| CopyAction | `site/src/components/design-system/CopyAction.astro` | secondary/inverse; default, success, error; Clipboard API and deterministic fallback | candidate |
| Badge | `site/src/components/design-system/Badge.astro` | neutral, brand, accent, success, warning, danger | approved |
| Field | `site/src/components/design-system/Field.astro` | default, hover, focus, filled, error, disabled | approved |
| Skeleton | `site/src/components/design-system/Skeleton.astro` | event-card, event-list, reduced motion | approved |
| StatePanel | `site/src/components/design-system/StatePanel.astro` | empty, error, stale/degraded, success, informational | approved v2; loading moved to Skeleton v1 |

### Product components

Product components remain in `site/src/components/` while they are shared only by this product. “In the design system” means they are registered, rendered from their real source in the catalog, documented, checked and cannot be locally forked. Moving every file into a nested folder provides no consistency benefit by itself.

The machine-readable authority is `site/src/data/design-system-registry.json`. It records the accepted runtime base, source, version, states and production consumers. The generated catalog table is a human-readable projection verified against this manifest.

Current registry highlights:

- `EventHero` — approved default; composition families live in `/lab/hero/`;
- `EventFacts` + `EventTokenMedallions` — approved quick-decision group;
- `DesktopEventActionPanel@2` — approved graphite CTA with paid-price, paid-unknown, registration, free/calendar, free/registration, phone copy, external source, sold-out and unavailable states; responsive normal/compact/stacked plus attached/released behavior is covered by the desktop lab;
- `DesktopEventPage@14` — approved production desktop composition. The catalog embeds the real OCR companion-arrival and low-resolution split routes and links the complete 14-scenario lab;
- `EventCtaPanel@1` and standalone `EventMediaRail@1` — deprecated desktop proxies with zero desktop-v14 production consumers, replaced by `DesktopEventActionPanel@2` and `DesktopEventPage@14` respectively;
- `EventCard` — `split-actions` is release baseline; `overlay-controls` is deprecated and retained only for regression/lab comparison;
- `EventListItem` — approved compact listing row;
- `CalendarLink`, `EventMediaRail`, `Icon`, `SocialIcon` and `AnnouncementsLockup` — approved supporting components with visible catalog examples;
- `InterestClubCard` — feature-gated product component;
- `AuthorizedEventSearch@2` — approved shared search surface. Its primary action keeps the existing progress fill while the content area uses `Skeleton@1` until the first vector/final cards arrive; pagination preserves existing cards and marks only “Показать ещё” busy;
- `ListingPersonalFilter` and `PersonalFeedSlot` — conditional client enhancements. Catalog fixtures are explicitly marked and ignored by hydration; the catalog must never override runtime `[hidden]` CSS.

### Loading contract

`Skeleton@1` is the only content-loading pattern for event results and personal feed slots. A progress fill may remain inside a submit action to communicate stage/percentage, but it does not replace card-shaped skeletons. Spinner panels and a permanently visible “Подбираем события” status are prohibited. Search shows skeletons only for a new first page and removes them as soon as vector or final cards render; “load more” keeps the existing cards.

A registry row in `/lab/design-system/#registry` links status, runtime source and coverage. Status vocabulary is `experimental`, `candidate`, `approved`, `deprecated`.

### Icon-only copy action

`CopyAction` is the single candidate contract for copying an already visible value such as a phone number. The value remains ordinary selectable text; the adjacent action shows the conventional copy icon without persistent helper copy. Its accessible name states exactly what will be copied, and the fixed icon box changes from `copy` to `check` on success. A hidden polite live region announces success or an actionable failure, while a visible `!` marker ensures the error state is not communicated by colour alone. Neither state changes the control geometry.

Use `secondary` on light surfaces and `inverse` on a dark or brand surface. Both variants inherit the `44px` minimum target, focus ring and motion rules from `Button`. Product components must consume `CopyAction` rather than adding another Clipboard API listener or a page-local copy glyph. The component remains `candidate` until its immutable preview and real product integration receive owner sign-off.

## Component versions and mandatory migration

Every registered component has an explicit integer version (`v1`, `v2`, ...). A material change to public API, geometry, hierarchy, visual language or interaction behavior never silently rewrites an approved version: it creates `vN+1`.

Version transition contract:

1. render the old and new runtime versions side by side in `/lab/design-system/` with the same representative content and state matrix;
2. mark the old row `deprecated` and set a visible `replaced by <component> vN+1` relation;
3. inventory all production consumers and move them to the new version in the same delivery;
4. if an atomic move is unsafe, use a feature flag and record consumers, owner, deadline and rollback; undocumented mixed versions block release;
5. declare migration complete only after source search plus preview/E2E evidence show zero production callers of the old version;
6. retain the deprecated version only for catalog/regression comparison, then delete it when sign-off and rollback window are closed.

Non-contract accessibility/browser corrections may stay on the same version. Any change in component API, layout geometry, information hierarchy or interaction semantics requires a new version. `EventCard overlay-controls` is recorded as `v1`; `split-actions` is `v2`, and production consumers are required to use `v2`.

## Brand architecture

The visible service name is **«Полюбить Калининград Анонсы»**:

- `Полюбить Калининград` is the umbrella endorsement;
- `Анонсы` is the service wordmark with one deliberately expanded `о`;
- both tiers form one lockup and must not be stretched, reordered or replaced independently;
- the hanging-tag silhouette is the common desktop, mobile and favicon mnemonic; the favicon uses the lower-set wide `о`, not the rejected empty tag or old `ПК` mark.

Canonical brand documents:

- [Brand lockups and lettering](brand-lockups.md);
- [Favicon and small mark](favicon.md);
- [Event-page product and desktop composition](../event-page-product-design.md).

## Component promotion lifecycle

1. **Experiment:** build in `/lab/<topic>/`; assign owner, use case and reject criteria. Do not ship page-local copies as production primitives.
2. **Candidate:** add every relevant state, narrow/mobile/desktop examples, keyboard behavior, accessible names and reduced-motion/no-JS behavior.
3. **Approval:** product/design owner approves an immutable preview build id and git SHA; deviations are written down.
4. **Version:** assign `v1` to a new component or `vN+1` to a material redesign; keep the prior version visible beside it.
5. **Promotion:** move reusable geometry/behavior into `site/src/components/design-system/` or register the shared product component; replace local copies and migrate every production consumer.
6. **Contract:** update this registry, version/migration assertions in `check-design-system.mjs` and `check-preview.mjs`, canonical feature docs, release docs and `CHANGELOG.md`.
7. **Deprecation:** mark the old version visibly in the catalog, name its replacement and delete it after zero-caller and regression evidence.

A component is not approved merely because it appears on one production page. Conversely, an approved component may not be changed silently inside a page stylesheet.

## Acceptance matrix

Every approved component change is reviewed at:

- viewport widths `320`, `360`, `390`, `430`, `768`, `1024`, `1366`, `1440`, `1920` as applicable;
- default, hover, keyboard focus, pressed/selected, loading, disabled, empty/error/degraded states;
- keyboard-only navigation and screen-reader names/state;
- normal and long Russian content, missing media and broken/slow optional backend;
- `prefers-reduced-motion: reduce` and no-JS for core static content;
- no horizontal page overflow; intentional table overflow must be labelled and keyboard-focusable;
- minimum 44×44 px interaction targets and AA contrast.

Automated checks are necessary but do not replace real Safari iOS/Chrome Android review for native share, calendar and OS handoff.

## Governance

1. New pages compose the registered system; they do not invent a second button, badge, card radius or feedback pattern.
2. Raw colors are prohibited in shared primitive component files. The graphite desktop surface consumes `--ke-color-graphite-*` and `--ke-shadow-graphite`; add/change a semantic token once in `design-system.css`.
3. A new state is incomplete until it is visible in `/lab/design-system/` and asserted by the contract checks.
4. Historical labs cannot silently redefine the approved baseline.
5. Product/design sign-off records exact branch, SHA and immutable preview URL in [the release UI contract](../release-ui-contract.md) and [the release readiness plan](../../../reports/static-personal-announcements-release-readiness-2026-07-11.md).
6. Any production UI behavior change updates canonical docs and `CHANGELOG.md` in the same commit.
7. A material redesign increments the component version and includes complete consumer migration; untracked coexistence of old and new versions is a release blocker.
