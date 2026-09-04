# Product typography normalization — independent implementation window

Work independently in `onedayonemasterpiece/events-bot-new`. Do not delegate to Codex/DevCoveer or create another orchestration layer. Perform the work yourself with Git, source inspection, tests and local browser tooling.

## Product outcome

Make the typography of **real user-facing static pages** behave as one design system: a small semantic hierarchy for page/section/card headings, body, metadata and labels, with intentional responsive mobile/desktop variants. This is broader than finding byte-identical copies. A role change in the canonical Git/Astro SoT must propagate to every registered production consumer.

Exclude `/lab/**`, `/__preview/**`, 404/500 and internal catalog/demo pages from product acceptance. Do not redesign palette or typography. Do not materialize Penpot. Do not create a new component package or build service.

## Source and isolation

1. Fresh-read issue `onedayonemasterpiece/events-bot-new#621` from comment `5542976599` and newer meaningful results.
2. Fetch current `origin/agent/static-site-single-kaggle-contract`; this is the sole executable Astro trunk. Record the exact base SHA only after fetching.
3. Create one short-lived branch/worktree named `work/ui-normalization-product-typography-20260904`. Never write to historical `integration/ui-normalization-launch-20260902`, `r0/*`, lab-only or specialist branches.
4. Reuse the existing foundations, Astro-family registry, token-impact graph and product-route graph. Do not introduce a second SoT.

## Execute, not merely audit

1. Census only component/style sources reachable from the current product route graph.
2. Separate legitimate semantic/responsive variants from local arbitrary forks. Rank by route reach and visual frequency.
3. Select one coherent, reversible, highest-reach batch that can be finished and tested in this run. Prefer an existing semantic token/type owner; add a token only if no correct role exists and document why.
4. Replace local typography literals/aliases with semantic roles without changing copy, markup hierarchy, layout behavior or palette. Preserve deliberate mobile/desktop differences as explicit variants of the same system.
5. Add fail-closed source/consumer regression tests proving the affected real consumers use the shared owner and that lab files cannot satisfy the gate.
6. Regenerate only existing generated SoT graphs if their checker requires it.
7. Update the canonical normalization document and `CHANGELOG.md`.
8. Render one affected product route locally at 390×844 and 1440×900 using the existing focused runner. Inspect computed font family, size, weight and line-height for the affected roles; save concise evidence under `artifacts/codex/` but do not commit artifacts.
9. Run relevant focused tests plus:
   - `npm --prefix site run check:design-system-production-surfaces`
   - `npm --prefix site run check:astro-family-sot`
   - token-impact checker/generator according to current repository commands
   - `git diff --check`
10. Commit and push the branch. Publish one factual `[RESULT]` comment to #621 with base/head SHA, exact production routes and components changed, tests/exits, local computed-style evidence, exclusions and remaining next gap.

Do not run a full Kaggle build; R0 integrates compatible batches and owns publication. Do not stop at a census: if no factual blocker exists, deliver source + tests + docs + commit.
