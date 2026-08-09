# Current UI Decoder v1 (with Resource Graph v0 compatibility)

## Purpose and boundary

`scripts/current_ui_resource_graph/decode.mjs` reconstructs a deterministic,
machine-readable inventory of the UI that exists in the pinned Astro source and
the exact immutable candidate artifact. It is an observation decoder, not a
design-system generator.

Version 1 retains every v0 file for existing consumers and additionally emits
the compact handoff tree under
`catalog/component-decoder/<snapshot-id>/`. A successful decoder process is
not automatically a design-system handoff: `receipt.json` independently says
`handoff_status: GO|NO_GO`, and the canonical workflow accepts only `GO` with no
open gate. A `complete / partial / NO_GO` receipt means the evidence snapshot
was written successfully but is not ready for defragmentation.

The decoder does **not** merge, normalize or defragment implementations; create
normative TO-BE component contracts, tokens, variants or patterns; mutate
Penpot; or change the Astro source/CSS/runtime. It does create explicitly
non-normative `candidate/as-is` evidence contracts. Every fragmentation and candidate-graph record keeps
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
   Canonical reruns download this HTML from pinned Actions artifact
   `9023507736`, verify both artifact ZIP and HTML hashes, and do not depend on
   the mutable live-root response.

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
imports, `@babel/parser` for bounded TypeScript/JavaScript state facts, and
PostCSS for declarations/at-rule context. Inline Astro `<script>` imports enter
the same dependency graph as frontmatter imports; this is required for the
`ClubCatalogKeyboard.astro` → `clubCatalogNavigation.mjs` consumer edge.
Approximate fields carry
lower confidence or `unknown`; a filename/class similarity is never promoted to
a component identity.

The state-aware layer records, without retaining source text:

- `Props` optionality, literal unions and destructuring defaults;
- bounded conditional/switch/logical branches, derived state and feature flags;
- `data-*`, `hidden`, `open`, `expanded` and `disabled` markers;
- media crop/fallback declarations, `@media` and `@container` queries;
- local `style` / `class:list` override sites.

String literals are bounded. URL-, credential- and bearer-shaped values are
replaced with SHA-256/length records. Parse failures carry only a reason hash
and block the relevant state evidence gate.

## Logical component classification (107/107)

The canonical union contains 107 logical component paths: 106 paths shared by
the candidate and public-root source planes plus the root-only
`PrelaunchPage.astro`. Every path receives one closed disposition and one
closed reachability value, with a written basis and per-plane bindings.

Disposition counts are fixed for the pinned snapshot: 51 `production-ui`, 20
`composition-layout`, 20 `lab-only`, 4 `experiment-only`, 1 `support-data`, 8
`nonvisual`, 3 `dead-unreachable`, and 0 `needs-verification`. Reachability is
one of `production-observed`, `production-reachable-not-observed`,
`controlled-specimen-only`, `lab-only`, `experiment-off`, `source-only`, or
`dead-or-unreachable`.

The labels are AS-IS evidence, not target architecture. In particular:

- all Focus prototype components remain `lab-only`;
- the three timetable treatments and their experiment wrapper remain
  `experiment-only / experiment-off`, never production variants;
- `PopularCategoryFilter`, `WeekendTimeMatrix`, and `MobileSearchBottomNav`
  are `dead-unreachable` in the pinned AS-IS baseline. For Mobile Search the
  decoder also retains a mismatch against the older surface contract: the
  exhaustive pinned consumer/runtime scan found no binding, but this is not a
  deletion or normalization decision.

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
(including source file/line, bounded selector, pseudo-state, custom-property
dependency edges, media/at-rule context and conservative selector/property semantic
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
capture takes a bounded sequence of up to eight frames until two consecutive
frames match through the pinned Playwright `pixelmatch` comparator. It uses
Playwright's documented YIQ perceptual threshold `0.2` with zero pixels allowed
beyond that threshold; it does not require byte equality or exact equality of a
coarse dHash. Both raw SHA-256 values, diagnostic dHashes, the attempt count and
whether the accepted buffers were byte-identical remain recorded. Volatile
perceptual content still fails closed after the bounded attempts. Raw browser rasters remain
explicitly noncanonical visual evidence because independent Chromium sessions
can differ in a sparse set of antialiased/media pixels even after structure and
computed styles are identical. `screenshots-index.jsonl` therefore stores raw
hashes, diagnostic dHashes and the comparator contract; canonical graph shards
and receipts use a fixed, size-checked screenshot budget. Cross-run acceptance
is the hash-bound human visual review rather than equality of encoded bytes or
dHash, following Playwright's tolerance-based visual-comparison model.

Generic component evidence excludes `[data-desktop-clean-event]`: that marker
is the complete event-page surface, not a component boundary. Event layouts are
captured through page verification, while actual component roots such as the
desktop action panel, media frame, transport blocks and medallion layout remain
eligible for element capture.

The same comparator contract applies to controlled component PNGs and exact
real-route element PNGs. Their byte-equality and dHash fields are diagnostics,
not acceptance gates. Each locator is scrolled once, must hold stable geometry
for five animation frames, and then receives the same bounded consecutive-frame
comparison. Exact-route pages additionally wait for network idle and fonts and
disable motion before element capture.

Exact-route component selection never substitutes a complete responsive page
surface for transport or medallion evidence. Transport verification captures
only the rail, bus or KAUP component roots. The desktop medallion consumer
deliberately gives its logical layout root `display: contents`, so browser
evidence is taken from the independently rendered `top` and `inline` slot
sections that own actual visual boxes; the mobile consumer yields its visible
`inline` section. The visibility predicate stays in the Playwright locator so
each screenshot attempt re-resolves the currently visible responsive element,
rather than retaining a positional locator that can move to a hidden sibling.
Capture failures retain the exact route/context/component label in the failed
receipt.

An absence-only real-route binding (for example event `2601`, whose exact
resolver emits no medallion root) is complete only after every declared absent
selector is measured at count zero. The decoder does not demand a visible root
after that proof and does not fabricate an empty component or screenshot.

Manifest, HTML and browser navigation retries are bounded to three attempts;
retry configuration itself is capped.

In addition to page rasters, v1 captures a bounded component-scoped evidence
set. Each record has an element raster, safe DOM attribute summary, geometry,
computed typography/color/layout, CSS custom properties, accessibility and
focus/expanded/hidden state, loaded font face/weight/status, viewport context
and bounded matched CSSOM rule provenance. Where a compiled rule cannot be
mapped to one source line, that limitation is explicit rather than replaced by
a guessed override owner. It never
contains full HTML or raw navigation/media URLs. Evidence labels distinguish
the exact candidate, public root and controlled specimens; candidate presence
cannot be relabelled as production observation. Component breakpoint contexts
are explicitly registered at widths 390, 420, 540, 700, 720 and 1728; an
uncaptured context stays a plan requirement rather than an inferred result.

Required snapshot files are:

- `manifest.json`, `receipt.json`, `summary.md`;
- `source-components.jsonl`, `observed-ui-families.jsonl`,
  `runtime-observations.jsonl`, `page-families.jsonl`,
  `event-presentation-formats.jsonl`;
- `desktop-mobile-analysis.jsonl`, `style-observations.jsonl`,
  `fragmentation-report.jsonl`, `candidate-component-graph.jsonl`;
- `unresolved-questions.md`, `coverage-report.md`,
  `screenshots-index.jsonl`.

The additional compact v1 handoff tree is:

```text
catalog/component-decoder/<snapshot-id>/
├── manifest.json
├── receipt.json
├── summary.md
├── artifact-index.json
├── source-files.jsonl
├── source-bindings.jsonl
├── component-families.jsonl
├── components/*.json
├── composition-edges.jsonl
├── consumers.jsonl
├── route-families.jsonl
├── page-state-signatures.jsonl
├── specimen-plan.jsonl
├── specimen-observations.jsonl
├── page-verification.jsonl
├── candidate-contracts/*.contract.json
├── mismatches.jsonl
├── unresolved.jsonl
├── conformance-capsules/**
└── penpot-materialization-candidates.json
```

Heavy page/element screenshots and component evidence stay in the Actions
artifact and are referenced by `artifact-index.json`; the compact tree is the
Git handoff. Candidate contracts are explicitly AS-IS and non-normative.
`penpot-materialization-candidates.json` remains `not-materialized` in this
phase.

### Controlled specimens and human review

The canonical capture run materializes a disposable Astro 6.4.8 harness from a
reflink/copy of the exact candidate `site/src`; it never edits the pinned tree.
The bounded registry has 21 controlled cases and 22 capture steps, plus 26
exact real-route bindings across 48 rendered viewport contexts. Rail, Kaup and medallions receive real
`PreviewEvent` fixtures and render the real component definitions. The bus
`no-groups` branch has no valid exact-data fixture and is therefore recorded as
`source-model-only`, not fabricated through data-module replacement.

The Event Presentation and Media Heavy capsules also receive isolated evidence
from the real `EventHero.astro` and `EventMediaRail.astro` definitions. The hero
uses exact event `5336`; the media rail uses all ten exact assets of event
`2781` with `maxVisible=3`, preserving three small previews plus the `+7 фото`
overflow action. These captures do not invent a component boundary for the
large poster companion or desktop split/editorial shells inside
`DesktopEventPage.astro`: those remain internal conditional resources verified
on exact real pages.

The exact secret candidate is a non-production `tail` research build for the
Amber system. Its `/artefakty/` binding therefore verifies the active collection
and its mobile `/vyhodnye/` binding verifies the rendered rail collectible; the
desktop rail is intentionally omitted because that consumer is hidden above
720px. These observations stay candidate-only with
`production_state_claimed:false`. The separately modelled production contract
continues to hard-block the rail and render only the unavailable collection
shell; the candidate capture is never relabelled as production evidence.

Every controlled isolated-specimen capture retains two byte-stable PNG buffers,
SHA-256 and dHash,
bounded DOM/ARIA/computed/cascade/font/geometry evidence, focus/open/hidden
state, media facts, console/network counts, source paths and capsule IDs. The
workflow intentionally finishes this first stage as a partial `NO_GO` pending
human visual review.

`v1/review-materialize.mjs` is the separate deterministic second stage. It
accepts a human review ledger only after every raster under `screenshots/` and
`component-screenshots/` has been inspected and hash-bound. The ledger must
cover all six capsules, every controlled observation, every raster-backed page
verification, the Actions run/artifact/digest/expiry, and permanent-storage
URI/version/digest. Only then may it emit the narrow verdict
`GO_FOR_FAMILY_SCOPED_DEFRAGMENTATION`; all component/capsule decisions remain
`NOT_MERGED`.
For a reviewed `GO`, each capsule must contain non-empty local
`specimen-observation-refs.jsonl` and `real-page-verification-refs.jsonl` files;
their IDs must resolve to the global shards and must cover every evidence ID in
the capsule review. Capsule IDs are canonicalized across the serialized
`capsule.<id>` and `<id>` forms. Empty, dangling or prefix-mismatched local
indexes fail closed. Full-page evidence explicitly selected in the human
review ledger is also copied into that capsule's local reference index even
when the capture-time page record has no `capsule_ids`; the human selection is
therefore hash-bound instead of silently discarded.
Every page record with a screenshot path is treated as raster-backed even when
the capture-time row omits a duplicate SHA field; the materializer hashes the
actual file, requires its ledger review, and writes an explicit reviewed page
status.

The review ledger and reviewed capsule overlay are authoritative for completed
visual reconciliation. Capture-time `pending` fields can remain inside
non-promoted candidate contracts when that individual contract still lacks a
dedicated state or consumer specimen; they do not become accepted contracts.
Materialized specimen/page rows themselves are rewritten to explicit reviewed
trace statuses. A `GO` never turns a candidate contract into normative design
system authority.

JSONL is stable-sorted and written incrementally. Deterministic snapshot time,
per-file SHA-256 receipts, per-route input limits and a total output byte budget
make reruns comparable. A partial/failed `receipt.json` is written before risky
work. The candidate URL is accepted only through a configured environment
variable or file, is never placed on the command line, and its bearer prefix is
redacted from errors and artifacts.
Root-relative candidate bearer paths are also sensitive: CSS custom-property,
matched-rule and accessibility strings containing `/_review/` are replaced by
bounded length/SHA-256 records before either real-route JSONL is written. The
recursive final gate independently scans every output byte for both the full
candidate base and its opaque path segment and fails closed on either.

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

## Evidence-completion gate and STOP

The v1 handoff remains `NO_GO` unless all 107 paths are classified, state facts
are present, controlled specimens and component evidence are bound, candidate
AS-IS contracts exist, at least six reconciliation capsules exist, and a
source → specimen → representative real-page trace is complete. Missing
evidence is listed as a blocker; it is never converted into a guessed contract.
The deep validator recomputes compact hashes and rejects `GO` without reviewed
specimen traces, six reviewed capsules, durable Actions metadata, permanent
evidence provenance, and a hash-bound human review ledger.

`labs-preview-special` is an intentional desktop-only lab surface excluded from
the production baseline. Editorial Collections, Legal pages and page-end
Hero-talk are absent AS-IS future requirements and are not synthesized.

Even `GO` authorizes only the immutable evidence handoff. It does **not**
authorize merge, split, normalization, tokenization, Penpot materialization, or
Astro/CSS mutation. Those decisions start only in a separately approved
defragmentation phase.

## Completed reviewed handoff

The two-stage capture and review cycle is complete for snapshot
`decoder-v1-snapshot-20260808T124842-4786ac53bc`:

- canonical capture run:
  [`31293484656`](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31293484656),
  capture decoder `961cd3506f5dc538097299b67c975b4fa117e5c9` and review
  materializer `25d82f59f891b9d64861cd15b787c5c0f86fd129`;
- Actions artifact `9032355884`, digest
  `sha256:add07915b6b70da2a7d825e64e08a91da6d8eb28657d70a0009d087bc0f952b2`;
- permanent heavy evidence:
  [GitHub Release r2](https://github.com/onedayonemasterpiece/events-bot-new/releases/tag/current-ui-decoder-v1-snapshot-20260808T124842-4786ac53bc-r2),
  asset `507100453`, digest
  `sha256:a6ad9244b3ead55424f303fc15efbd988c07a507843bdf9728626e2850335e9c`;
- human review ledger SHA-256
  `88eeaf712a8d7534d53ffabaa0ab98c6eaa54f3e8dea54c2086d8d5c69f7165b`;
- 46 page rasters and 111 component rasters, **157/157 manually reviewed**;
- 22 controlled observations and 135 raster-backed page records reviewed;
- reviewed compact manifest SHA-256
  `f7740f7f533c3f0cda5d4d0b8ebe98b565d7f521368b96462daecbd26522d5cc`;
- official compact handoff merged through
  [`lovekgd-design-system#27`](https://github.com/onedayonemasterpiece/lovekgd-design-system/pull/27)
  at commit
  [`6e179f6edcf1a9232d56020719a8a7897009f9bd`](https://github.com/onedayonemasterpiece/lovekgd-design-system/commit/6e179f6edcf1a9232d56020719a8a7897009f9bd),
  tree `e77fc2457fadfdffb46ed2d90304ebb91e89a715`;
- design-system validation on merged `main`:
  [snapshot run 31294881909](https://github.com/onedayonemasterpiece/lovekgd-design-system/actions/runs/31294881909)
  and
  [contract run 31294881910](https://github.com/onedayonemasterpiece/lovekgd-design-system/actions/runs/31294881910),
  both successful.

The reviewed compact receipt is `complete`, with `handoff_status: GO` and
`GO_FOR_FAMILY_SCOPED_DEFRAGMENTATION`. It contains 107 logical component
dispositions, 12 candidate AS-IS contracts and six reviewed reconciliation
capsules. Candidate contracts remain `candidate-as-is-not-accepted`; every
component/capsule decision remains `NOT_MERGED`, and
`penpot-materialization-candidates.json` remains `not-materialized`.

Manual review visibly confirmed both desktop Event Detail anatomies
(editorial/landscape and split/portrait-poster), their separate stacked versus
inline CTA placements, the large poster/primary companion and smaller remaining
photo previews including a real `EventMediaRail` three-preview/`+7 фото`
specimen, rail/bus/KAUP transport blocks, medallions, and the distinct Amber and
Focus Egg systems. The isolated real-definition `EventHero` is retained as
`consumer-exists-only` because its layout depends on consumer CSS. The requested
route-binding IDs `7052`, `7301`, `7048` and `7186` remain
`explicit-unreachable` for component-scoped exact-route evidence; page-family
screenshots do not promote those bindings to captured component evidence.

The machine-readable final receipt and complete review report live at
`.codex/integration/current-ui-decoder-v1/`. This closes decoding only. It does
not start defragmentation or normalization automatically.

## Behavioral Decoder & Experiment Archaeology v1.1 (append-only, in progress)

`scripts/current_ui_resource_graph/behavioral-decode.mjs` is an append-only evidence
pass over the immutable v1 source pin. It writes a *new*, separately validated
`behavioral-supplement-v1.1/` directory and never rewrites the reviewed v1
manifest, receipt, components, candidate contracts, capsules or visual ledger.
Its final manifest, artifact index and artifact receipt use the same portable
sibling path to the immutable Decoder v1 snapshot; runner-local checkout paths
are forbidden in the cross-repository handoff.

The supplement records source-qualified media/loading/geometry/positioning,
rails, overlays/selection state machines, breakpoint `-1/0/+1` probes, bounded
CTA/variant history and the `transport_timetable_layout` treatment registry.
Generic commit-subject history is pinned only to `ef7aa62` ancestry and remains
semantically unclassified; explicit PR/run/non-ancestor branch evidence is a
curated supplement with its own scope, never a claim of exhaustive coverage of
every mutable remote ref, tag, release or artifact. The
requirements-aware pass additionally writes a line/commit/PR provenance
ledger, a complete consumer-scoped media matrix (including `4:5`, `5:4`,
`3:2`, intrinsic `2:3`, `1:1` and extra ratios), and an explicit dynamic-region
loading/skeleton/fallback matrix. It preserves reviewed conflicts instead of
inventing a global crop, upscaling, offline, retry or skeleton contract. It always
marks source-only and planned evidence explicitly; `dHash` remains a diagnostic
only and is not a human-review substitute. The three transport arms
`departure_board_v1`, `route_strips_v1` and `next_departure_queue_v1` have
separate planned evidence packets and remain `NOT_MERGED`, unaccepted until
receipt-backed archaeology and capture/review are available.

Requirements archaeology deliberately uses two separately pinned planes. UI
implementation is still decoded only from exact source
`ef7aa62e45c60f7a12da6160f490719c0721ec03`; the requirements ledger is read
from the decoder commit so it can include later accepted source maps without
pretending they existed at `ef7aa62`. The required corpus now explicitly
includes the page-archetype source map, schedule user requirements, rail and
multimodal directory, image/EventCard/Event Detail desktop and mobile records,
listing V14/V15 through V28 supersession chain, mobile shell, canonical Search
plus its replaced source brief, owner personalization requirements, target and
implementation contracts, Personal Feed architecture, and loading/skeleton
documents. Extraction is deterministic and bounded to 256 matched lines per
document. A historical, proposal or mixed/conflict document cannot be promoted
to current authority by a line-level keyword; retained rules are reconciled in
explicit curated records. Missing required documents and missing pinned dynamic
region sources stay fail-closed evidence. In particular, the pinned dynamic
source is
`site/src/components/listings/PopularPersonalizedRow.astro`, not the obsolete
non-`listings` path.

`transport_timetable_layout` is decoded from the hash-checked `ef7aa62` source
family (`transportExperiment.ts`, client/controller, Astro consumer and both
build entry points), rather than from placeholder registry prose. The record
preserves modes `off`, `qa`, `focus_group`, `live`; the three 3333/3333/3334
bucket arms; browser-subject `SHA-256` first-u32-big-endian assignment;
1–20-departure/future-after-10-minute-reserve eligibility; consent, automation,
QA and valid-exposure telemetry gates; the five qualified action names; and the
three-cell SRM threshold. Production build is forced `off`, QA/focus-group are
secret-candidate modes, and `live` has no approved pinned build path. The source
defines a qualified-action predicate but the pinned click-ingest path does not
call it, while the SRM evaluator has tests but no runtime consumer; both are
recorded as bounded archaeology findings, not silently accepted contracts. No
winner/acceptance receipt is present, so every row remains
`experiment-unresolved`, `NOT_MERGED`, and forbidden from normalization.

The only permitted completed supplement status is
`READY_FOR_PROJECT_NORMALIZATION_SYNTHESIS`. It does not authorize component
merges, normalisation, semantic tokens, production Astro/CSS/JS changes,
Penpot mutation, experiment winner selection or an automatic next stage.

### Two-stage behavioral evidence gate

The capture implementation is intentionally split from acceptance:

1. `.github/workflows/current-ui-behavioral-decoder-v1-1.yml` checks out the
   exact source SHA `ef7aa62e45c60f7a12da6160f490719c0721ec03`, validates the
   immutable Decoder v1 tree and manifest, builds a disposable reflink/copy
   harness without copying the installed `node_modules` tree, links the single
   pinned dependency installation, and captures the corrected closed 67-packet matrix. The Actions receipt must
   remain `CAPTURE_COMPLETE_NO_GO_PENDING_REVIEW`.
2. The review materializer may emit
   `READY_FOR_PROJECT_NORMALIZATION_SYNTHESIS` only after all 124 raster files
   have been opened and reviewed individually at full resolution, every review
   is bound one-to-one to the observation path and SHA-256, the independent
   audit passes, and both the Actions artifact and permanent Release asset have
   complete immutable provenance. Those review/provenance gates are necessary
   but not sufficient: any packet or per-probe reconciliation row with
   `blocks_ready:true` keeps the final receipt
   `EVIDENCE_COLLECTION_INCOMPLETE`.

The corrected registry has 57 executable packets and ten exact blockers. Blocked states
are recorded as blockers rather than replaced by fake wrappers: source-only
PopularCategoryFilter, the unconsumed ListingPersonalFilter v1, unavailable
zero/one-item mobile rail fixtures, the PersonalFeed unavailable branch that
the pinned real corpus cannot reach honestly, hidden auth/personalization
runtime modes, an intentionally unchanged discovery error fallback, and the
native rail `End`/`Home` accessibility gap. Chromium leaves the exact focusable
horizontal rail at `scrollLeft=0`; this blocks readiness instead of being
replaced by a programmatic scroll. The unreconciled per-probe runtime evidence
for the complete 293-row breakpoint/container matrix also has
`blocks_ready:true`. Carrying every exact probe ID on that blocker is a
fail-closed reconciliation receipt, not a claim that a family screenshot
captured 293 transitions.

Captured evidence remains plane-qualified as controlled exact-source runtime.
The short-viewport Reference4 packet scrolls the actual
`.mobile-discovery-menu__panel` by its observed 56px range; the fixed-height
inner list is not misreported as a scroll surface. It includes mobile
menu/disclosure/focus-return behavior, rail edges and the
exact observed absence of a working native `End`/`Home` alternative,
sticky/fixed collisions, separate Editorial stacked
and Split inline CTA anatomy, all three unresolved transport treatments,
consumer-specific media behavior (including separate real `4:5` EventCard and
`5:4` rail/search evidence), the large primary/poster plus smaller remaining
photo previews, and Search/Favorites/PersonalFeed loading and recovery flows.
The bounded dynamic closure now references every one of the 13 source matrix
regions: Search, Favorites, PersonalFeed, discovery rerank, home local rerank,
the popular personalized shelf, Weather, Exhibitions gallery, EventCard,
ListingEventCard and mobile-listing media, auth runtime and personalization
runtime. Each is bound to an executable packet or an exact explicit blocker;
a source test is never relabelled as runtime capture. Materialization joins the
source `dynamic-region.<id>` namespace to the packet's compact `<id>` form
explicitly; an unbound row is `coverage-missing`, never an implicit family hit.
Semantic transition checks include bounded standard descendant
`aria-checked`, `aria-pressed`, `aria-expanded`, `aria-selected`, `aria-busy`,
`open`, and `hidden` state, so equal-length radio/state replacements still
produce an evidence-bearing transition without retaining full HTML.
Font settling is probed and recorded for at most four seconds before each
packet raster. If it times out, Playwright's internal unbounded second font
wait is disabled and the actually rendered fallback is captured with
`font_settle.status=timed-out-continued`; it is not relabelled as a loaded-font
render.

The home cold-start/rerank root contains 30 lazy `EventCard` images. Its
disposable capture page therefore promotes those images to eager loading and
requires a bounded load/error plus decode settlement before each of the two
element rasters. The observation retains the image counts and test-only
instrumentation mode. This closes runner-dependent progressive lazy loading
without editing pinned source, masking image pixels, or relaxing perceptual
comparison.

Transport disclosure packets use real exact treatment components with a
controlled `PreviewEvent` fixture assembled only from distinct trips returned
by the exact Kaup resolver across bounded start-time variants. This yields six
real resolver rows for each treatment, makes closed/open geometry observably
different, preserves `NOT_MERGED`, and selects no winner. The weekend sticky
packet targets the visible desktop implementation at `1280×800`; hidden
attached mobile/desktop duplicates are rejected. Interaction phases require a
resolved visible target (or an explicit not-applicable contract), a real
DOM/geometry/scroll/focus delta, and nonzero geometry where declared. Plans and
observations carry reachability plus dynamic, breakpoint and coverage refs.

Capture is bounded and diagnosable: each plan logs start/completion/failure and
elapsed time; font settle is capped at four seconds, controlled routes at 20
seconds, and screenshots at 30 seconds. Deferred media routes hold image
requests only, so the previous `media-broken` deadlock cannot also hold fonts.
OCR/photo packets retain their exact event/classification provenance. The
time-nav popover clipping and mobile-menu post-close bottom-nav disappearance
remain recorded visual conflicts rather than silently repaired behavior.
The capture harness freezes clock, timezone and randomness and uses only
test-local network fixtures; it does not contact Supabase or mutate the pinned
`site/src` tree.

Deep validation re-hashes every compact output, checks the receipt-to-manifest
chain, verifies plan/phase/observation/action-packet referential integrity, and
binds each indexed heavy raster to its observation checksum. An outer manifest
rewrite therefore cannot conceal a corrupted compact entry. The retained final
raster always has an exact SHA-256; repeated-frame stability follows the pinned
Playwright pixel/perceptual comparator rather than requiring byte-identical PNG
encoding. Neither the file hash nor the comparator result is accepted as a
substitute for the mandatory human visual review.
