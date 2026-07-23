# Lane L01 Results

## Status
committed

## Requirement IDs
- R01

## Branch
`agent/static-unified-corrections/exhibitions`

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/static-unified-corrections-exhibitions`

## Base SHA
`5c2db86811c34355a1894748b87af73fdb5b19e3`

## Head SHA
See final lane handoff; committed `RESULTS.md` cannot self-reference the commit that contains it.

## Files changed
- `site/src/pages/vystavki/index.astro`
- `site/src/components/ExhibitionsPersonalSurface.astro`
- `site/src/lib/exhibitionsPersonal.ts`
- `site/tests/exhibitions-public-dynamic.test.mjs`
- `.codex/lanes/L01/RESULTS.md`

## Commands run
- `node --experimental-strip-types --test tests/exhibitions-public-dynamic.test.mjs`
- `npm run test:occurrences`
- `npm run build`
- generated `/vystavki/` row/canonical probe
- `node scripts/check-exhibitions-personal-prototype.mjs`
- `git diff --check`
- forbidden-file scope audit with `git status --short --ignored`

## Tests / verification
- PASS: focused dynamic public-exhibition projection tests — 4/4.
- PASS: occurrence-family regression tests — 10/10.
- PASS: Astro build — 311 pages, including `/vystavki/`.
- PASS: built `/vystavki/` contains five unique current real exhibition rows, two dynamic new rows, the public canonical URL, and no donor-only specimen.
- PASS: accepted donor CSS and inline interaction script are byte-identical in `ExhibitionsPersonalSurface.astro`; the shared `ExhibitionPrototypeRow.astro` remains the row renderer.
- PASS: source contract proves `/vystavki/` obtains candidates through `getOngoingExhibitionEvents()` before dynamic projection, retaining its date selection and occurrence collapse.
- PASS: projection fails closed on inactive, invalid-date, expired and non-exhibition rows, then removes duplicate IDs and exact normalized exhibition-title repeats.
- PASS: `git diff --check` and writable-file scope audit.
- ENVIRONMENT NOTE: the first build attempt could not resolve `astro/config` because this isolated worktree had no `node_modules`. A same-revision dependency tree was symlinked from the integration worktree; two subsequent full builds passed.
- OUT-OF-SCOPE CHECKER NOTE: `check-exhibitions-personal-prototype.mjs` still reports four lab-fixture/header-count failures against the unchanged `/lab/exhibitions-personal/` donor and current preview data. L01 is forbidden to edit that donor/checker; the focused public-route parity/projection tests and builds pass.

## Risks
- The public review route intentionally keeps the donor's localStorage key and interaction behavior byte-for-byte. Any later state-key migration must be product-approved and tested as an interaction change.
- `check-preview.mjs` still asserts the retired `listing-stack` exhibition presentation. The integration owner must update that shared generated-output gate after cherry-picking L01, as required by lane ownership.
- The route-neutral surface was extracted without modifying the donor route because the donor page is outside L01's writable scope. The donor remains the presentation/interaction source of truth; a later owner may switch it to the shared component after confirming byte parity.

## Merge notes
- Cherry-pick the single L01 commit.
- Do not deploy this branch or build from the production root.
- No preview JSON, shared check scripts, docs or `CHANGELOG.md` were changed.
- `INC-2026-07-02-exhibition-duplicates-static-site` is a regression guard, not closed by this review-surface lane.
- No push performed.
