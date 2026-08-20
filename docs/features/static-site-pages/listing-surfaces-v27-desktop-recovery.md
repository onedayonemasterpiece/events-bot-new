# Listing surfaces V27 — desktop shell recovery

**Scope:** shared static-site shell plus desktop `Сегодня`, `Завтра`,
`Выходные` and `Популярное`. Mobile Popular V26 behavior is a preserved
regression contract, not a redesign target.

## Product contract

Very tall, explicitly classified OCR media now shares the bounded-document
framing already used by compact related cards. A known source below width/height
`4/5` may spend at most 20% crop while retaining at least 80% vertically to gain useful row width;
unclassified/error media remains natural `contain`. The card exposes treatment,
fit, crop and reason data attributes so browser geometry can verify this without
an event-specific rule. The exact 906×1280 acceptance oracle resolves to
ratio `0.884765625`, `coverCrop=0.2` and `verticalRetention=0.8`.

- The real site header remains visible at the top of desktop listing pages.
- The discovery rail sticks immediately below the 57px global header; neither
  layer may cover the other or listing content.
- Every listing route consumes the same global design-system bundle. A page
  must never rely on a child component to import that bundle transitively.
- Today, Tomorrow and Weekend retain the accepted V18 listing composition;
  Popular retains the accepted V22 desktop composition and V26 phone context.

## Root-cause correction

V22 carried both the global design-system import and sticky header definition
inside `EventLayout.astro`. A later divergent integration branch replaced that
layout with a version that had neither contract. Popular hid the first loss
because its route imported the stylesheet explicitly; the date routes did not,
so a later component-tree change exposed them as unstyled HTML. V27 restores
both contracts at the actual common layout boundary.

## Regression gates

`check-preview.mjs` opens the built HTML contract for all four routes, resolves
their emitted Astro stylesheets and requires the listing shell, discovery rail
and listing-card selectors in the compiled CSS. It also requires the shared
layout to keep the sticky 57px header at `z-index: 60`.

The older broad `check-design-system.mjs` is not silently re-enabled here: it
contains independent stale assertions against newer shared primitives. The V27
gate therefore checks the affected built output directly rather than allowing
unrelated checker debt to hide this regression again.

Browser acceptance covers `1366×768`, `1536×864` and `1920×1080` at the top and
after scrolling, plus the V26 Popular mobile matrix at `360/390/430px`.
The executable desktop gate is
`site/scripts/check-listing-desktop-geometry-playwright.sh` (`ADD-V12-13`).
It also focuses the skip link before scrolling and requires it to remain above
the restored sticky header rather than becoming a keyboard-only hidden control.

## Published review build

The immutable V27 candidate is available under
`https://kenigevents.ru/preview-20260720-date-listings-v27-desktop-recovery/`.
Its four public routes passed the same executable desktop matrix after deploy;
the review links were delivered to Telegram forum topic `122` as message `411`.
