# token_impact_sot results

- Lane: `token_impact_sot`
- Base SHA: `5fddf16c42f8d92a33493684a9e7342c83fe3db3`
- Implementation head SHA: `c00fdbbb9cbad74585c28c111393c60dfe977703`
- Receipt note: this file is committed immediately after the implementation
  commit, so the implementation SHA above is the tested feature head.

## Delivered

- Machine-readable token-authority policy registry and deterministic generated
  CSS custom-property census/reverse impact graph.
- Read-only token → components → registered Astro families → production routes
  query.
- Fail-closed validation for stale token/route graphs, conflicting global
  `--ke-` owners, undefined non-fallback consumers, stale exemptions, and
  undocumented alias cycles.
- Focused behavior tests for success, owner conflict, undefined token, alias
  cycle, and stale generated graph.

## Evidence / commands

```text
node --test site/scripts/token-impact-graph.behavior.test.mjs
# PASS: 4/4
node site/scripts/check-astro-family-sot.mjs
# PASS: 9 families; generated source graph current
node site/scripts/check-token-impact-sot.mjs
# PASS: 2506 tokens, 2788 definitions, 3367 consumers, 704 alias edges
node site/scripts/generate-token-impact-graph.mjs --impact --ke-color-action-primary
# PASS: returns component paths, registered families, and production routes

git diff --check
# PASS
```

## Changed files

- `CHANGELOG.md`
- `docs/features/static-site-pages/design-system/README.md`
- `site/scripts/check-token-impact-sot.mjs`
- `site/scripts/generate-token-impact-graph.mjs`
- `site/scripts/token-impact-graph.behavior.test.mjs`
- `site/src/design-system/token-authority-registry.v1.json`
- `site/src/design-system/token-impact.generated.v1.json`

## Risks / integration notes

- The graph is generated from the exact checked-in Astro family graph and its
  registry; integrate this lane after any concurrent Astro-family graph update,
  then regenerate both graphs in that order.
- No token value, component styling, route source, Button code, family parser,
  route runner, or icon component was changed.
- One pre-existing legacy shell variable is explicitly registered as an
  unresolved integration boundary; the checker rejects a stale exception once a
  definition is added or its non-fallback consumer disappears.
