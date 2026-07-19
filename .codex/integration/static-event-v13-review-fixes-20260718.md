# Static event v13 integration report — 2026-07-18

Base: `origin/main@fb5a35dd`
Branch: `integration/static-event-v13-review-fixes-20260718`

## Integrated requirements

- R01: crop-safe classified `unknown_visual` can route to Editorial; unsafe
  uncertain media remains Split. Event 6592 synthetic regression plus built
  production fixture.
- R02–R04: Split CTA admits `comfortable -> icons -> stacked`, checks the
  primary label geometry, keeps 6793/6811 inline, and promotes the calendar to
  the only primary calendar action for free one-day no-link events (6901/6959).
- R05–R06: closed desktop hero Left/Right navigation and terminal gallery CTA
  pointer/touch navigation; focus and reduced-motion guards.
- R07–R08: static and dynamic continuation use the shared row packer and
  six-card diversity/fatigue policy. Explicit rejected vector items are not
  eligible; missing verification is not described as verified similarity.
- R09: read-only redacted 24h diagnostics CLI with channel/inventory/orphan
  reporting.
- R10: primary-source archive/noindex policy and one-build/two-artifact design
  are documented. Stable-root publication and archive registry remain a
  separate gated implementation; this change does not promote root.

## Local gate

- Astro preview: 382 pages, success.
- Node: 33/33 focused media/CTA/gallery/continuation/runtime/feed checks.
- Python: 33/33 diagnostics/release/handoff checks.
- Playwright 1536×864:
  - Split phone, registration and free calendar-primary fixtures passed;
  - actual 6592 = Editorial, 6793/6811 = compact icon Split, 6901 = `В календарь`;
  - 5755/6408/4783 closed-hero arrows passed;
  - desktop and 390px mobile terminal CTA navigation passed;
  - 3934 dynamic continuation: 6 unique cards; each three-card row has equal
    380px card widths and 472.5px media heights.
- `git diff --check`: clean.

## Release boundary

No public claim is valid until the merged source SHA produces a fresh automatic
secret candidate and the same public Playwright checks pass there. Secret bearer
tokens are not recorded in git.
