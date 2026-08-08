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
   manifest `generated_at` `2026-08-08T13:32:56.163Z`, inventory tree
   `0aad3919fccd996a5d32bcc760af8ee9b72249742c9db53196b009759bd0e7f4`,
   embedded production manifest/tree
   `baa0f29da3205ac81ddd4804bf6ff8e22b4585abb58d7d378e8dd87b9d395e45` /
   `47df3798686dfbdde43589ba6a6498effd82f6fd091de6883a4899b7b4e57769`,
   Astro `6.4.8`, Node `22.12.0`. Its checked
   `static_secret_candidate_manifest_v1` inventory contains 1,266 HTML
   documents, 3,021 pages, 3,323 unique safe relative keys and 686,610,720
   bytes. All eight required checks (`astro_build`, `browser_visual`,
   `candidate_contract`, `catalog_parity`, `no_referrer`, `noindex`,
   `prefix_containment`, `root_isolation`) must be `ok`. This is an immutable
   noindex review candidate, **not** the production root; root promotion is
   disabled.
2. `current_root_prelaunch` is the independently published public root:
   source `5a9d804438377f65fe4b26bd7019e73626529864`, release
   `prelaunch-main-31263560430-1`, Actions run `31263560430`, artifact
   `9023507736`, published `2026-08-08T15:09:02Z`, runtime
   `https://kenigevents.ru/`. The separately fetched public root HTML must hash
   exactly to
   `1c31504d10d9ec66c7fa84ad52c94e6019a741f0ee01826f219578963e0ea21e`.

Neither plane is inferred from the other. The workflow materializes both Git
commits into separate detached worktrees, and the decoder inventories and maps
each source/runtime plane independently. A different checkout is accepted only
when Git proves the selected source subtree has the exact same tree object;
otherwise decoding stops. A non-Git fixture requires a separate explicit exact
tree hash for each source plane. Observed-family aggregation groups identical
logical source paths across planes without counting the second plane as another
implementation; content drift at that path is recorded separately.

## Extraction contract

The source pass uses the parser versions installed by the pinned `site`
lockfile: `@astrojs/compiler` for Astro structure, `es-module-lexer` for module
imports and PostCSS for declarations/at-rule context. Approximate fields carry
lower confidence or `unknown`; a filename/class similarity is never promoted to
a component identity.

Every HTML file listed by the exact candidate manifest is fetched and parsed
one at a time. Keys must be unique, relative and traversal/control-character
safe; remote URL construction encodes every path segment and proves base-path
containment. The graph retains hashes, counts and bounded structural facts, not
full HTML or base64 bodies. Routes are represented by hashes/redacted route
identifiers while the safe manifest-relative key remains mapping evidence.
Source/template evidence and the deterministic transitive import graph are
mapped per identity plane before runtime structure, state and media facts.
Mixed dynamic names such as `date-[date].astro` remain `/date-:date/` rather
than disappearing or becoming a literal route. Event Detail, Day, Weekend and
Popular keep distinct page-family IDs; none is clustered into another.

Styles distinguish PostCSS-parsed inline and standalone CSS literal usage
(including media/at-rule context and conservative selector/property semantic
cohorts) from computed inconsistency; a computed result that was not measured
is `unknown`. Fragmentation evidence reports separate source-AST, exact mapped
runtime, source-style and composition channels rather than treating a name as
proof. The core independent observation viewports are `390x844` and
`1728x900`; optional evidence viewports are `430x932`, `768x1024` and
`1280x800`. Mobile/desktop comparison emits separate page-family and
observed-UI-family records using computed regions, visibility, display, order,
geometry and semantic-cohort style facts. UI-family evidence is explicitly
scoped to mapped host pages when no synthetic runtime wrapper exists, and is
never described as a responsive variant.

### Desktop event presentation resources

Event Detail is one route family but not one desktop presentation. The decoder
must preserve two independent resource formats from the exact runtime markers
and their source branches:

- `event-format.desktop.editorial-landscape`: the wide landscape/photo
  `DesktopEventPage` editorial branch, with the CTA in the editorial side
  column using the stacked action layout;
- `event-format.desktop.split-portrait-poster`: the portrait/poster split
  branch, with sticky media and content columns and the CTA rendered inline in
  the content flow;
- `event-format.desktop.split-portrait-visual`: the narrower resolver state
  backed specifically by `split-portrait-or-square-visual` or
  `split-low-resolution-portrait-viewer`, so the vertical-media evidence is not
  inferred from pixels or a filename;
- `event-format.desktop.no-image-fallback`: the split fallback path selected by
  `split-no-image-fallback`, retaining the typed or generic fallback in place of
  event media.

The names describe observed presentation families, not normalized variants.
The split resolver may also admit square, document or resolution-constrained
media, so `split` must not be reduced to a filename/orientation guess. Exact
runtime evidence comes from `data-desktop-family`, `data-presentation-reason`,
`data-action-family` and `data-action-layout`; every event HTML route is scanned
for them. Browser selection reserves an event-detail specimen for each observed
desktop family, an explicit portrait-visual state, a no-image specimen, and a
large-poster-plus-small-photo-preview specimen after all page-family
representatives, before generic outliers. The
canonical workflow therefore uses a bounded budget of 23 pages / 46 core
viewport screenshots rather than relying on an accidental structural outlier.
Its total serialized evidence budget is explicitly capped at 90 MiB; the
workflow validates both the declared cap and that the final pre-manifest byte
count remains below it.

Media inside `DesktopEventPage` is also recorded without inventing Astro
component boundaries: the primary large frame, split small-photo rail,
editorial small hero-photo rail, large contain-fit poster companion, and small
remaining-photo companion previews are separate
`internal_conditional_resource` records. `DesktopEventActionPanel` remains a
real source component with two `component_family_variant` records for
side/stacked editorial and inline split placement. All records live in
`event-presentation-formats.jsonl` and remain `NOT_MERGED / unresolved`.

This is deliberately more precise than the broad `family.media-treatments` and
`family.event-actions` fragmentation cohorts. It fixes what exists and where it
was observed; it does not yet decide a common component API, token mapping or
future visual style.

For the pinned 1,096 Event Detail documents, the exact structural scan observes
96 editorial/stacked-CTA routes and 1,000 split/inline-CTA routes. Within split,
69 routes carry an explicit portrait/square or low-resolution portrait reason
and 63 carry `split-no-image-fallback`. Media markers identify 217 split small
photo rails, 29 editorial small hero rails, 11 large editorial poster
companions, and 8 of those with adjacent small companion previews. These counts
are snapshot facts, not a proposed target distribution.

### Component, color and typography evidence

`source-components.jsonl` is the complete parser-backed inventory of tracked
`.astro`, `.ts`, `.js` and `.mjs` files inside each pinned `site/src` plane
(tests/specs excluded). It is not a claim that every file is production
reachable: lab, source-only and unused records are intentionally retained.
Likewise, the 20 heuristic UI families are review cohorts, not an exhaustive
component taxonomy. A source record that has not entered a heuristic family is
still present and must not disappear from a later reachability and
normalization workshop.

Colors and typography are extracted into `style-observations.jsonl` at two
levels: PostCSS declaration/literal evidence with selector and at-rule context,
and bounded computed browser cohorts on captured host pages. Color evidence
includes properties such as `color`, `background`, `background-color`,
`border-color`, `fill` and `stroke`; typography evidence includes
`font-family`, `font-size`, `font-weight`, `line-height`, `letter-spacing`, font
shorthand and related text properties. The snapshot summary reports style
inconsistencies separately from total observations.

These are raw, plane-scoped observations and divergence candidates—not named
palette resources, typography resources, semantic tokens or confirmed
conflicts. Shared source can appear once per identity plane. Normalized aliases,
semantic roles and conflict resolutions are explicitly deferred.

The browser budget chooses the modal structural representative of every page
family before considering any outlier. Remaining outlier slots are allocated
round-robin across families, and a budget smaller than the family count emits
explicit uncaptured rows. Each browser context is created with the declared
Playwright viewport and is rejected if the runtime viewport differs, so mobile
and desktop screenshots cannot silently fall back to the default dimensions.
The browser fixture freezes `Date` and pseudorandom selection to the pinned
snapshot/route, waits for network idle, fonts and visible image decode, disables
motion, and requires five identical animation-frame layout fingerprints before
collecting computed evidence or pixels. Near-viewport media settlement is
bounded and fails closed instead of allowing an unresolved decode promise to
stall the artifact job. Following Playwright's screenshot-assertion contract,
capture also requires two consecutive screenshot buffers to match before the
image is accepted; volatile pixels fail closed. Raw browser rasters remain
explicitly noncanonical visual evidence because independent Chromium sessions
can differ in a sparse set of antialiased/media pixels even after structure and
computed styles are identical. `screenshots-index.jsonl` therefore stores a
stable 64-bit perceptual dHash rather than a raw-byte hash; canonical graph
shards and receipts use a fixed, size-checked screenshot budget. Cross-run
acceptance requires equal perceptual dHash while retaining both raw specimens
for human visual QA, following Playwright's tolerance-based rather than
raw-byte visual-comparison model.
Manifest, HTML and browser navigation retries are bounded to three attempts;
retry configuration itself is capped.

Required snapshot files are:

- `manifest.json`, `receipt.json`, `summary.md`;
- `source-components.jsonl`, `observed-ui-families.jsonl`,
  `runtime-observations.jsonl`, `page-families.jsonl`,
  `event-presentation-formats.jsonl`;
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
`data-home-hero-talk`; runtime Hero-talk evidence is marker-only, so the home
route by itself cannot satisfy it. `FOUND` still requires both the source and
the exact candidate DOM marker.
No separate page-end Hero-talk component/consumer was found. The unrelated
`StandardOnboardingPlacementContext` inert `page_end` slot is not Hero-talk and
must not satisfy that hypothesis. Exhibitions, For Me, Clubs and both Hero-talk
hypotheses are mandatory named rows so they cannot silently disappear.
Experimental archaeology and old branches are not Current graph evidence.

## Running

The canonical unattended entry point is
`.github/workflows/current-ui-resource-graph.yml`. Dispatch fields enter shell
steps only through quoted environment variables and must equal the immutable
snapshot allowlist before they can form a path or Git/decoder argument; no raw
dispatch expression is interpolated into shell. Every manual-dispatch input
declares the required GitHub Actions `type: string` schema field, so GitHub can
validate and schedule the workflow rather than rejecting it before job
creation. Every default is also an explicitly quoted YAML string; this prevents
timestamp-like identity values from being normalized into a different display
form before the exact allowlist gate sees them. Job-level environment paths use the allowed `github.workspace`
context rather than the step-only `runner` context. The dependency-install step
resolves its working directory from that same `CANDIDATE_WORKTREE` environment
value, so worktree creation and consumption cannot silently use different temp
roots. The workflow materializes the
exact candidate and public-root SHAs in separate detached worktrees, installs
the candidate-pinned site dependencies and Playwright Chromium, and reads the
bearer candidate base only from repository secret
`CURRENT_UI_GRAPH_CANDIDATE_BASE_URL`. It validates all required files,
identity planes, no-merge invariants, final phrase and redaction before marking
the graph valid. If post-decode validation fails, an apparently `complete`
decoder receipt is rewritten to `failed` with
`reason: workflow_validation_failed`; the always-uploaded artifact therefore
never claims false completion.

A bounded local fixture run supplies `--runtime-root`, `--runtime-manifest`,
`--root-runtime-file`, separate exact `--source-tree-hash` /
`--root-source-tree-hash` values, and `--verify-production-identity false`.
This mode exists for deterministic tests only and cannot be presented as the
current candidate graph.

The only next-step wording emitted by the summary is:

> Proceed to normalization workshop
