# Current UI Resource Graph v0

## Purpose and boundary

`scripts/current_ui_resource_graph/decode.mjs` reconstructs a deterministic,
machine-readable inventory of the UI that exists in the pinned Astro source and
the exact immutable candidate artifact. It is an observation decoder, not a
design-system generator.

The decoder does **not** merge, normalize or defragment implementations; create
component contracts, tokens, variants or patterns; mutate Penpot; or change the
Astro source/CSS/runtime. Every fragmentation and candidate-graph record keeps
`decision: NOT_MERGED` and `recommendation: unresolved`.

## Two independent identity planes

The default identity is pinned and fail-closed:

1. `latest_checked_kaggle_candidate` is the current durable candidate:
   source `ef7aa62e45c60f7a12da6160f490719c0721ec03`, build
   `production-secret-20260808T144842-5472a382`, run
   `static-site:production-secret-20260808T144842-5472a382:acbd40ef5203`,
   snapshot `snapshot-20260808T124842-4786ac53bc`, candidate manifest SHA-256
   `d615f6e447dc8c6ae3b876bf4a99123d1c85afee55276c26645f020b26074322`,
   Astro `6.4.8`, Node `22.12.0`. Its checked manifest contains 1,266 HTML
   documents, 3,021 pages, 3,323 files and 686,610,720 bytes. This is an
   immutable noindex review candidate, **not** the production root; root
   promotion is disabled.
2. `current_root_prelaunch` is the independently published public root:
   source `5a9d804438377f65fe4b26bd7019e73626529864`, release
   `prelaunch-main-31263560430-1`, Actions run `31263560430`, artifact
   `9023507736`, published `2026-08-08T15:09:02Z`, runtime
   `https://kenigevents.ru/`.

Neither plane is inferred from the other. The decoder verifies the requested
Git commit. A different checkout is accepted only when Git proves the selected
source subtree has the exact same tree object; otherwise decoding stops. A
non-Git fixture requires an explicit exact tree hash.

## Extraction contract

The source pass uses the parser versions installed by the pinned `site`
lockfile: `@astrojs/compiler` for Astro structure, `es-module-lexer` for module
imports and PostCSS for declarations/at-rule context. Approximate fields carry
lower confidence or `unknown`; a filename/class similarity is never promoted to
a component identity.

Every HTML file listed by the exact candidate manifest is fetched and parsed
one at a time. The graph retains hashes, counts and bounded structural facts,
not full HTML or base64 bodies. Routes are represented by hashes/redacted route
identifiers. Source/template evidence is clustered before runtime structure,
state and media facts. Mixed dynamic names such as `date-[date].astro` remain
`/date-:date/` rather than disappearing or becoming a literal route.

Styles distinguish PostCSS-parsed source cascade/literal usage (including
media/at-rule context) from computed inconsistency; a computed result that was
not measured is `unknown`. The core independent observation viewports are
`390x844` and `1728x900`; optional evidence viewports are `430x932`,
`768x1024` and `1280x800`. Mobile and desktop observations are never described
as responsive variants.

Required snapshot files are:

- `manifest.json`, `receipt.json`, `summary.md`;
- `source-components.jsonl`, `observed-ui-families.jsonl`,
  `runtime-observations.jsonl`, `page-families.jsonl`;
- `desktop-mobile-analysis.jsonl`, `style-observations.jsonl`,
  `fragmentation-report.jsonl`, `candidate-component-graph.jsonl`;
- `unresolved-questions.md`, `coverage-report.md`,
  `screenshots-index.jsonl`.

JSONL is stable-sorted and written incrementally. Deterministic snapshot time,
per-file SHA-256 receipts, per-route input limits and a total output byte budget
make reruns comparable. A partial/failed `receipt.json` is written before risky
work. The candidate URL is accepted only through a configured environment
variable or file, is never placed on the command line, and its bearer prefix is
redacted from errors and artifacts.

## Coverage hypotheses

`coverage-report.md` explicitly classifies the supplied hypotheses as `FOUND`,
`MISSING`, `DISCOVERED` or `AMBIGUOUS`, with separate exact source IDs and
runtime route hashes:

- Home, Event Detail, Day Listing, Weekend Listing, Search, Popular,
  Collections, Festivals, Interest Clubs, Partners, Favorites, For Me/personal
  feed and Exhibitions;
- Editorial Collections and Legal documents (absence remains `MISSING`; footer
  prose or future documentation is not a route);
- cross-cutting Hero-talk and Hero-talk page-end.

The pinned source contains `HomeHeroTalk.astro` with the home consumer and
`data-home-hero-talk`; `FOUND` still requires the exact candidate DOM marker.
No separate page-end Hero-talk component/consumer was found. The unrelated
`StandardOnboardingPlacementContext` inert `page_end` slot is not Hero-talk and
must not satisfy that hypothesis. Exhibitions, For Me, Clubs and both Hero-talk
hypotheses are mandatory named rows so they cannot silently disappear.
Experimental archaeology and old branches are not Current graph evidence.

## Running

The canonical unattended entry point is
`.github/workflows/current-ui-resource-graph.yml`. It materializes the exact
candidate SHA in a detached worktree, installs the pinned site dependencies and
Playwright Chromium, reads the bearer candidate base from repository secret
`CURRENT_UI_GRAPH_CANDIDATE_BASE_URL`, validates all required files/invariants,
and uploads a complete or partial artifact with `if: always()`.

A bounded local fixture run supplies `--runtime-root`, `--runtime-manifest`, an
exact `--source-tree-hash`, and `--verify-production-identity false`. This mode
exists for deterministic tests only and cannot be presented as the current
candidate graph.

The only next-step wording emitted by the summary is:

> Proceed to normalization workshop
