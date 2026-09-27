# Home owner-review contract — 2026-09-27

This note is the current owner-review contract for the public home surface. It
supersedes the accidental R8 composition; it does not redefine unrelated page
families.

## HeroTalk

- Event-linked HeroTalk scenes are image-led. A clickable event phrase must use
  the mosaic media treatment when an eligible visual asset exists.
- Text-only HeroTalk is reserved for service/onboarding copy such as greetings
  or a generic next-step message; it is not a reason to remove the image from a
  normal event-linked scene.
- The same rule applies to the page-end HeroTalk.
- The mosaic renderer may use the current catalogue focal point when the older
  geometry-classifier receipt is absent. Explicit OCR/document/unsafe media
  remains ineligible.

## One floating navigation island

Home has one navigation island, not an in-flow menu plus a second sticky copy.

1. The island first appears in normal flow immediately below HeroTalk.
2. After its original anchor passes the viewport top, that exact DOM element
   becomes fixed near the top edge.
3. The global desktop header navigation is hidden on Home while this contract
   is active; the brand tag remains.
4. The island contains the complete Home navigation set used for browsing:
   Сегодня, Завтра, Выходные, Бесплатно, Выставки, Фестивали, Популярное,
   Необычное and, when enabled, Клубы по интересам.
5. Mobile keeps normal in-flow discovery controls and the existing bottom-nav
   contract; no second desktop island is synthesized.

## Geometry / overflow / brand

- HeroTalk is a full-width block in normal document flow; it must not rely on a
  100vw negative-margin trick from inside the constrained page shell.
- Owner acceptance requires no document-level horizontal overflow.
- The review build uses the current announcements favicon master with a
  versioned URL so an old browser-cached icon cannot masquerade as current UI.

## Review-build boundary

An owner-review link opens the normal preview root
`/preview-<id>/`. Technical QA pages such as `__preview`, `lab` and
`_review` are not published by the owner-review publisher. Reviewers browse
the actual site routes directly.

Owner evidence: voice review `voice-20260926-200723-dab0343f` plus the
2026-09-27 screenshot review in the KenigEvents workstream.
