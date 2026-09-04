# Product icon/action composites normalization — independent implementation window

Work independently in `onedayonemasterpiece/events-bot-new`. Do not delegate to Codex/DevCoveer. Execute source changes, tests and evidence yourself.

## Product outcome

Across all **real user-facing static pages**, one semantic icon must have one glyph source and use a very small canonical size set. Repeated constructions such as icon + count, icon + optional label, like, share, save or calendar action are component families with one behavior/state/accessibility contract wherever they occur. This is not limited to eliminating literal copies.

Exclude `/lab/**`, `/__preview/**`, 404/500 and internal demos from product acceptance. Do not redesign icons, palette or action semantics. Penpot is optional and out of scope. Do not create another design-system package.

## Source and isolation

1. Fresh-read issue #621 from comment `5542976599` and newer meaningful results.
2. Fetch the current `origin/agent/static-site-single-kaggle-contract` and freeze its exact base SHA.
3. Work in one short-lived branch/worktree `work/ui-normalization-product-icon-composites-20260904`.
4. Reuse `Icon.astro`, `SocialIcon.astro`, `SemanticIcon.astro`, existing icon roles/tokens, the Astro-family registry and product-route graph.
5. A separate active lane owns `SiteFooter.astro`, `ServiceShareAction.astro` and `PwaInstallAction.astro`; do not edit those files in this batch.

## Execute a coherent product batch

1. Census product-reachable uses of share/like/save/calendar and icon+count/icon+optional-label patterns in `EventCard`, listing cards/rails, event detail and other real route surfaces.
2. Prove which are the same semantic interaction and which are intentionally different. Preserve all URLs, share payloads, counters, client hydration hooks, telemetry, focus behavior and 44px target contracts.
3. Select the highest-reach same-semantic cluster. Consolidate it onto an existing owner or one narrowly scoped shared Astro component. Do not build an abstract universal button and do not change action policy.
4. Make glyph identity flow through the canonical icon renderer. Map visual sizes to no more than the existing small size vocabulary; do not add near-duplicate pixel sizes.
5. Add regression tests for:
   - one glyph/source identity;
   - one shared composite owner for the selected pattern;
   - icon-only and icon+label/count states;
   - accessible name, keyboard/focus behavior and ≥44px interactive target;
   - server-rendered and client-created/hydrated consumers where applicable;
   - product reachability, excluding labs.
6. Update existing Astro/token graphs if required, canonical normalization docs and `CHANGELOG.md`.
7. Use the existing focused runner for one affected route at 390×844 and 1440×900. Record DOM identity, computed icon box size and target geometry under `artifacts/codex/`; do not commit artifacts.
8. Run focused card/listing/event-detail/icon tests plus production-surface, Astro-family, iconography and token-impact checkers and `git diff --check`.
9. Commit and push. Publish a factual `[RESULT]` to #621 with base/head, exact consumers/routes, preserved behavior, tests/exits, browser evidence and remaining exceptions.

Do not run a full Kaggle build. R0 owns intake and publication. A source census alone is not completion unless it proves there is no product-reachable duplication.
