# KenigEvents static-site design system

> **Status:** canonical foundation and component catalog implemented; product/design release sign-off remains pending.
> **Runtime catalog:** `/lab/design-system/` in every checked preview build.
> **Source of truth:** `site/src/styles/design-system.css` + `site/src/components/design-system/`.

This directory is the normative home for reusable visual rules of the KenigEvents static site. Dated research and lab pages remain historical evidence. Production templates must consume the shared tokens and runtime components documented here instead of copying geometry into a page-local stylesheet.

## What is implemented

- one semantic token layer for color, type, spacing, radii, elevation, motion, touch size and content width;
- shared primitive Astro components: `Button`, `Badge`, `Field`, `StatePanel`;
- compatibility aliases for established product components, so the system can be adopted without a risky all-at-once rewrite;
- shared token/action adoption by both `EventLayout` pages and the standalone root landing page;
- a built Astro catalog at `/lab/design-system/`, linked from `/__preview/`;
- real product fixtures for `EventHero`, `EventFacts`, `EventTokenMedallions`, `EventCtaPanel`, `EventCard` and `EventListItem`;
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
| Button | `site/src/components/design-system/Button.astro` | default, hover, focus, pressed, selected, loading, disabled; primary/secondary/quiet/danger | approved |
| Badge | `site/src/components/design-system/Badge.astro` | neutral, brand, accent, success, warning, danger | approved |
| Field | `site/src/components/design-system/Field.astro` | default, hover, focus, filled, error, disabled | approved |
| StatePanel | `site/src/components/design-system/StatePanel.astro` | empty, loading, error, stale/degraded, success, informational | approved |

### Product components

Product components remain in `site/src/components/` while they are shared only by this product. “In the design system” means they are registered, rendered from their real source in the catalog, documented, checked and cannot be locally forked. Moving every file into a nested folder provides no consistency benefit by itself.

Current registry:

- `EventHero` — approved default; composition families live in `/lab/hero/`;
- `EventFacts` + `EventTokenMedallions` — approved quick-decision group;
- `EventCtaPanel` — approved paid/free/sold-out state family;
- `EventCard` — `split-actions` is release baseline; `overlay-controls` is deprecated and retained only for regression/lab comparison;
- `EventListItem` — approved compact listing row;
- `CalendarLink`, `EventMediaRail`, `Icon`, `SocialIcon` and `AnnouncementsLockup` — approved supporting components with visible catalog examples;
- `InterestClubCard` — feature-gated product component;
- `ListingPersonalFilter`, `PersonalFeedSlot` and `AuthorizedEventSearch` — conditional client-enhancement components; the catalog exposes their static/forced state and links to the dedicated auth/search surface, while live network/auth states remain in feature E2E.

A registry row in `/lab/design-system/#registry` links status, runtime source and coverage. Status vocabulary is `experimental`, `candidate`, `approved`, `deprecated`.

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
4. **Promotion:** move reusable geometry/behavior into `site/src/components/design-system/` or register the shared product component; replace local copies.
5. **Contract:** update this registry, `check-design-system.mjs`, `check-preview.mjs`, canonical feature docs, release docs and `CHANGELOG.md`.
6. **Deprecation:** mark the old variant visibly in the catalog, name its replacement and delete it after callers and regression evidence are migrated.

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
2. Raw colors are prohibited in shared primitive component files. Add/change a semantic token once in `design-system.css`.
3. A new state is incomplete until it is visible in `/lab/design-system/` and asserted by the contract checks.
4. Historical labs cannot silently redefine the approved baseline.
5. Product/design sign-off records exact branch, SHA and immutable preview URL in [the release UI contract](../release-ui-contract.md) and [the release readiness plan](../../../reports/static-personal-announcements-release-readiness-2026-07-11.md).
6. Any production UI behavior change updates canonical docs and `CHANGELOG.md` in the same commit.
