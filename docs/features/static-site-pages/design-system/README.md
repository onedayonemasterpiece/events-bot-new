# KenigEvents static-site design system

> **Status:** canonical. The shared announcement lockups and wide-`о` favicon
> are approved for production as of 2026-07-14.

This directory is the normative home for reusable visual rules of the
KenigEvents static site. Dated research and lab pages remain historical
evidence; production templates must use the shared assets and components
documented here rather than copying their geometry.

## Brand architecture

The visible service name is **«Полюбить Калининград Анонсы»**:

- `Полюбить Калининград` is the umbrella endorsement;
- `Анонсы` is the service wordmark with one deliberately expanded `о`;
- both tiers form one lockup and must not be stretched, reordered or replaced
  independently;
- the hanging-tag silhouette is the common desktop, mobile and favicon
  mnemonic; the favicon uses the lower-set wide `о`, not the rejected empty
  tag or old `ПК` mark.

## Canonical documents

- [Brand lockups and lettering](brand-lockups.md) — production component,
  geometry, responsive variants and accessibility.
- [Favicon and small mark](favicon.md) — final transparent tag + wide-`о`
  asset and small-size rules.
- [Event-page product and desktop composition](../event-page-product-design.md)
  — how the lockup participates in listing and event-detail contexts.

## Core tokens

| Token | Value | Meaning |
|---|---|---|
| Brand tag | `#98401f` | Default solid tag fill. |
| Brand tag hover | `#893719` | Pointer hover only; not an active-section signal. |
| Reversed lettering | `#ffffff` | One-colour lockup and favicon glyph. |
| Tag bottom radius | `12px` | Desktop and mobile large tags. |
| Tag top radius | `0` | The tag appears attached to the top rail. |
| Desktop tag | `240×88px` | Stable from `1024px` upward. |
| Mobile tag | `128×96px` + safe area | Mobile drawer handle below `760px`. |
| Favicon artboard | `64×64` | Transparent SVG with a `52px`-wide tag. |

No gradient, inset tile, outline, texture or decorative lettering animation is
part of the approved identity. A contextual mobile shadow is allowed because
the handle overlays photography; the desktop tag remains shadowless.

## Event-detail component contracts

- **Transport A/B/C:** `site/src/components/transport/*` is the only canonical
  implementation. It shares bus/walk/pin/car icons but deliberately keeps the
  accepted arm-specific route, last-mile and return-warning copy documented in
  [event-transport-schedule.md](../event-transport-schedule.md). The retained
  secret-candidate specimen renders the same responsive component at desktop
  and mobile widths; query forcing must select the visible arm on both.
- **Desktop CTA:** admission/primary action and the bottom
  `calendar-share-like` row keep the same three-row hierarchy for ticket,
  telephone and informational variants. At `1536×864` (FHD at 125%) the three
  bottom controls must align, remain inside the card and never move beside the
  primary action. Run `STATIC_SITE_REVIEW_BASE_URL=… npm --prefix site run
  check:desktop-cta-geometry` for browser geometry
  acceptance.
- **Service footer:** `Понравились ` + the canonical inline `Анонсы` wordmark +
  `? Поделитесь` is one accessible prompt. Do not redraw the expanded `о` with
  CSS or revert to the ambiguous `Поделиться афишей` copy.
- **Frozen accepted specimens:** time-bounded production events used for design
  acceptance (`4783`, `5374`, `6551`, `6815`) are also stored in
  `desktop-event-examples.json`. Production eligibility may expire, but the
  design-system route and regression tests must not disappear or break a later
  full-catalog build.

## Governance

1. Change brand geometry only in the shared runtime assets/components.
2. Check desktop at `1024/1280/1536/1920`, mobile at `320/360/390/430`, and
   the favicon at `16/32/64px`.
3. Preserve visible focus, truthful `aria-current`, a functional home-link
   label and reduced-motion behavior.
4. Update these documents, `site/scripts/check-preview.mjs` and
   `CHANGELOG.md` with any production behavior change.
