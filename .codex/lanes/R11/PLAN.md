# R11 execution matrix

Base: `956683719543667f4042f3b81a88b1b5b7605ef8` (accepted R10 noindex prototype).
Production is out of scope.

| ID | Requirement | Lane | Dependencies | Acceptance |
|---|---|---|---|---|
| R11-01 | One-time consent before first negative swipe; future marks immediate and undoable | LISTING-UX | accepted R10 rail gesture | first confirm persists; cancel does not; later negative swipe skips modal |
| R11-02 | Sticky desktop date shelf retains current date at left | LISTING-UX | date listing header/shelf | date appears only in compact stuck state; filters remain usable |
| R11-03 | Accepted row-equalizing, no-field crop in lower card grids | CARD-CROP | canonical crop contract | full non-final rows; equal card/media height; no contain fields for non-OCR |
| R11-04 | Read and incorporate owner screenshots/comments from Telegram | SEARCH-MAP + integrator | E2E human session only | evidence ids and inspected screenshots recorded |
| R11-05 | Popular excludes ended events; exhibitions remain through end date | LIFECYCLE | event lifecycle fields | ordinary past excluded; active exhibition retained through end_date |
| R11-06 | Compact occurrence labels group repeated month | OCCURRENCES | mutual explicit family contract unchanged | `24, 25 июл, 27 сен`; accepted existing exact labels remain |
| R11-07 | Today de-emphasizes already passed occurrences | LISTING-UX | runtime clock | class/style updates without rebuild; future cards unaffected |
| R11-08 | Search avatar identifies session; real mobile Search works | SEARCH-AUTH | Supabase/Yandex contracts | meaningful avatar or email/name initial; live query succeeds or exact external blocker evidenced |

## Lane map

- `MAP-LISTING` read-only: R11-01/R11-02/R11-07; locate accepted donor and current regressions.
- `MAP-LIFECYCLE` read-only: R11-05/R11-06; data semantics and tests.
- `MAP-CROP` read-only: R11-03; shared accepted pack/crop algorithm.
- `MAP-SEARCH` read-only: R11-04/R11-08; Telegram evidence plus public reproduction.
- `WORK-LISTING` writes only listing/date/today UI and focused tests.
- `WORK-LIFECYCLE` writes lifecycle/occurrence libraries and focused tests.
- `WORK-CROP` writes shared card layout/crop surface and focused tests.
- `WORK-SEARCH` writes Search/auth UI/runtime and focused tests.
- Integrator alone owns canonical docs, `CHANGELOG.md`, generated preview, deployment, Telegram handoff, and conflict resolution.

Workers must not revert concurrent work and must end with a committed branch plus `.codex/lanes/<lane>/RESULTS.md`.
