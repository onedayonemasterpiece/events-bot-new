# KenigEvents static-site design system

> **Status:** canonical. Brand lockup R3/R4 is approved for the static site as of 2026-07-14.
>
> **Visual QA lab:** `/lab/design-system/`.

This directory is the normative home for reusable visual rules of the KenigEvents static site. Historical research and choice labs remain in dated feature documents, but production code and future layouts must follow the contracts here.

## Brand architecture

The visible service name is **«Полюбить Калининград Анонсы»**:

- `Полюбить Калининград` is the umbrella endorsement;
- `Анонсы` is the service wordmark with one deliberately expanded `о`;
- both tiers form one lockup and must not be independently stretched, centered or reordered;
- the tag silhouette is the common desktop, mobile and favicon mnemonic.

## Canonical documents

- [Brand lockups and lettering](brand-lockups.md) — geometry, type, responsive variants and implementation.
- [Favicon and small mark](favicon.md) — the transparent tag + wide-`о` mark and small-size rules.
- [Wide-`о` construction record](../announcements-lettering-wide-o-2026-07-14.md) — source audit and optical drawing history.
- [Desktop header decision](../desktop-header-concepts-2026-07-14.md) — navigation balance and current-state semantics.
- [Mobile drawer behavior](../event-hero-lab-2026-06-27.md) — interaction contract of the tag as the drawer handle.

## Core tokens

| Token | Value | Meaning |
|---|---|---|
| Brand tag | `#98401f` | Default solid tag fill. |
| Brand tag hover | `#893719` | Pointer hover only; not an active-section signal. |
| Reversed lettering | `#ffffff` / inherited `currentColor` | One-colour lockup and favicon glyph. |
| Tag bottom radius | `12px` | Desktop and mobile large tags. |
| Tag top radius | `0` | The tag must appear attached to the top rail. |
| Desktop tag | `240×88px` | Stable from `1024px` upward. |
| Mobile tag | `128×96px` + safe-area top | Optically taller drawer handle below `760px`. |
| Favicon artboard | `64×64` | Transparent SVG with a `52px`-wide tag silhouette. |

No gradient, inset card, outline, texture or decorative animation is part of the approved identity. A contextual mobile shadow is allowed because the handle overlays photography; the desktop tag remains shadowless.

## Governance

1. Change brand geometry only in the shared runtime assets/components, not by copying paths into page templates.
2. Check desktop at `1024/1280/1536/1920`, mobile at `320/360/390/430`, and favicon at `16/32/64px`.
3. Preserve visible focus, truthful `aria-current`, a functional home-link label and reduced-motion behavior.
4. Update this directory, the affected feature document, `site/scripts/check-preview.mjs` and `CHANGELOG.md` together.

## R4 review evidence

`Gemini 3.1 Pro (High)` reviewed the real `320/360/390/430px` mobile renders and favicon size board through Antigravity `agy` on 2026-07-14. Its first pass rejected the overly dominant two-line endorsement and the favicon's subpixel side clear space. R4 then reduced the endorsement to `8/9px`, weight `600`, opacity `0.9`, increased the inter-tier gap/bottom clear space, and changed the favicon wide-`о` to `6px` source side space with an optically lifted `y=28` centre. The second rendered acceptance returned **ACCEPT**, with no P0 or measurable P1.

Ignored raw evidence: `artifacts/codex/brand-system-mobile-favicon-20260714/gemini-r4-review.md`, `gemini-r4-acceptance-review.md`, screenshots and browser metrics.
