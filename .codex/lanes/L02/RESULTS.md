# Lane L02 Results — Search initial state and shared controls

## Lane contract

- Lane: `L02`
- Requirement: `R03`
- Branch: `agent/static-unified-corrections/search`
- Base SHA: `5c2db86811c34355a1894748b87af73fdb5b19e3`
- Head: resolve `agent/static-unified-corrections/search` after this lane commit.
- Owned implementation scope: `AuthorizedEventSearch.astro`,
  `EventLayout.astro`, focused Search tests, and this result record.

## Outcome

- The Search skeleton now has an unconditional initial `hidden` attribute,
  regardless of whether public Supabase configuration exists.
- The disabled noindex specimen no longer exposes a fake loading state or the
  “Образец состояния загрузки результатов” label.
- The existing runtime request path remains authoritative: a validated
  first-page search calls `setSearchLoading(true, { showSkeleton: !append })`,
  which reveals the card skeleton, and final/error cleanup hides it again.
- The accepted standalone textarea, label, form rule, dark full-width submit
  and terracotta in-button progress fill are now shared viewport-independent
  styles. Desktop and mobile no longer fork these controls.
- Yandex/Supabase auth, session restore, validation, NDJSON progress, request
  epochs, result rendering and pagination semantics were not changed.

## INC-2026-07-02 regression control

- Incident status remains `open`.
- Affected lane surfaces: Search initial/loading DOM and Search CSS.
- Preserved target behavior: no skeleton before a query; a card-shaped
  shimmer appears during the active first-page runtime request and remains
  until final cards/error cleanup.
- This lane verified the local DOM/runtime visibility contract and compiled
  page. Production URL auth/search journeys, backend audit rows and deployed
  SHA evidence remain integration/release gates and were not claimed here.

## Tests and evidence

```bash
cd site
node --test tests/search-learning.test.mjs tests/search-initial-state.test.mjs
# 13/13 PASS

npm run build
# PASS: 311 pages; /poisk/index.html generated

git diff --check
# PASS
```

Generated `/poisk/index.html` inspection:

- `data-search-skeletons hidden aria-hidden="true"` is present.
- The removed specimen label is absent.

Local Chromium computed-style smoke against the generated page:

| Viewport | Skeleton | Textarea | Submit |
|---|---|---|---|
| 1280×900 | `hidden`, `display:none` | `82px`, 2px bottom rule, 0 radius, 21px font | full width, `50px`, 8px radius, `rgb(34,26,20)` |
| 390×844 | `hidden`, `display:none` | `82px`, 2px bottom rule, 0 radius, 21px font | full width, `50px`, 8px radius, `rgb(34,26,20)` |

The focused runtime test executes the real `setSkeletonLoading` function body
with DOM-like doubles and verifies reveal on `true`, re-hide on `false`, and
that no second script path writes `skeletons.hidden`.

## Files changed

- `site/src/components/AuthorizedEventSearch.astro`
- `site/src/layouts/EventLayout.astro`
- `site/tests/search-learning.test.mjs`
- `site/tests/search-initial-state.test.mjs`
- `.codex/lanes/L02/RESULTS.md`

No shared generated-output gate, canonical documentation, `CHANGELOG.md`,
auth/backend implementation, or other lane-owned file was edited.

## Integration notes

- Cherry-pick this lane commit onto the correction integration branch.
- After merge, the integrator should run the full preview gate and the live
  INC-2026-07-02 mobile auth/search matrix on the immutable review prefix.
