# KenigEvents static-site design system

> **Status:** canonical foundation and component catalog implemented; product/design release sign-off remains pending.
> **Runtime catalog:** `/lab/design-system/` in every checked preview build.
> **Source of truth:** `site/src/styles/design-system.css` + `site/src/components/design-system/`.

This directory is the normative home for reusable visual rules of the KenigEvents static site. Dated research and lab pages remain historical evidence. Production templates must consume the shared tokens and runtime components documented here instead of copying geometry into a page-local stylesheet.

## What is implemented

- one semantic token layer for color, type, spacing, radii, elevation, motion, touch size and content width, including the global `--ke-content-wide-max` authority for wide desktop surfaces;
- shared primitive Astro components: `Button`, `Badge`, `Field`, `StatePanel`, plus the candidate `CopyAction` clipboard affordance;
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
| Layout | `--ke-content-max`, `--ke-content-wide-max`, `--ke-listing-*` | Shared normal/wide content width and date-listing geometry; page-local width systems are forbidden. |

Legacy aliases such as `--primary`, `--surface` and `--radius-md` remain temporarily in the same file. They are a migration boundary, not a second token system. When an existing component is touched materially, move it to `--ke-*` roles rather than adding another alias.

### Primitive components

| Component | Runtime path | Required states | Status |
|---|---|---|---|
| Button | `site/src/components/design-system/Button.astro` | default, hover, focus, pressed, selected, loading, disabled; primary/secondary/quiet/inverse/danger | approved |
| CopyAction | `site/src/components/design-system/CopyAction.astro` | secondary/inverse; default, success, error; Clipboard API and deterministic fallback | candidate |
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
- `ListingPageHeader v1`, `ListingControls v3`, `ListingTimeNav v1`, `ExactTimeTimeline v1`, `WeekendEditorialTimeline v1` and `ListingEventCard v2` — candidate shared date-listing family. `ListingControls v3` keeps all cities directly visible and adds a non-colour-only selected marker; `WeekendEditorialTimeline` replaces the deprecated centre-rail matrix with one time-left union axis and two continuous day lanes; `ListingEventCard v2` uses responsive derivatives, intrinsic geometry and measured row-end title expansion. `ListingControls v1/v2`, `WeekendTimeMatrix v1` and `ListingEventCard v1` remain registry-only deprecated records;
- `ListingPersonalFilter v2` — candidate full-list-first wording and behavior for date listings; `v1` is deprecated, retained in the catalog only for migration comparison and blocked from production callers;
- `PersonalFeedSlot` and `AuthorizedEventSearch` — conditional client-enhancement components; the catalog exposes their static/forced state and links to the dedicated auth/search surface, while live network/auth states remain in feature E2E.

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

## Event-detail component contracts

- **Transport A/B/C:** `site/src/components/transport/*` is the only canonical
  implementation. It shares bus/walk/pin/car icons but deliberately keeps the
  accepted arm-specific route, last-mile and return-warning copy documented in
  [event-transport-schedule.md](../event-transport-schedule.md). The retained
  secret-candidate specimen renders the same responsive component at desktop
  and mobile widths; query forcing must select the visible arm on both.
- **Desktop CTA:** geometry follows the resolved media family, not a viewport
  guess. Split portrait/OCR pages keep admission, primary action and
  `calendar-share-like` utilities in one compact row; Editorial wide-photo
  pages keep the accepted three-row hierarchy with utilities on the bottom.
  At `1536×864` (FHD at 125%) all controls must align and remain inside the
  card. Run `STATIC_SITE_REVIEW_BASE_URL=… npm --prefix site run
  check:desktop-cta-geometry`; the command targets retained expiry-proof Split
  and Editorial candidate fixtures rather than elapsed event URLs.
- **Service footer:** `Понравились ` + the canonical inline `Анонсы` wordmark +
  `? Поделитесь` is one accessible prompt. Do not redraw the expanded `о` with
  CSS or revert to the ambiguous `Поделиться афишей` copy.
- **Frozen accepted specimens:** time-bounded production events used for design
  acceptance (`4783`, `5374`, `6551`, `6815`) are also stored in
  `desktop-event-examples.json`. Production eligibility may expire, but the
  design-system route and regression tests must not disappear or break a later
  full-catalog build.

## Governance

1. New pages compose the registered system; they do not invent a second button, badge, card radius or feedback pattern.
2. Raw colors are prohibited in shared primitive component files. Add/change a semantic token once in `design-system.css`.
3. A new state is incomplete until it is visible in `/lab/design-system/` and asserted by the contract checks.
4. Historical labs cannot silently redefine the approved baseline.
5. Product/design sign-off records exact branch, SHA and immutable preview URL in [the release UI contract](../release-ui-contract.md) and [the release readiness plan](../../../reports/static-personal-announcements-release-readiness-2026-07-11.md).
6. Any production UI behavior change updates canonical docs and `CHANGELOG.md` in the same commit.
7. A material redesign increments the component version and includes complete consumer migration; untracked coexistence of old and new versions is a release blocker.
