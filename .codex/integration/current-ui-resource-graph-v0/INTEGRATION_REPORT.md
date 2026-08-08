# Current UI Resource Graph v0 integration report

## Scope and boundary

This integration reconstructs the current Astro UI as evidence. It does not
modify `site/src`, CSS, runtime UI, Kaggle publishing, Penpot, tokens, variants,
patterns, component contracts, or normalization/defragmentation decisions.
Every candidate in the graph remains `NOT_MERGED` / `unresolved`.

## Exact evidence planes

- Latest checked immutable Kaggle candidate source:
  `ef7aa62e45c60f7a12da6160f490719c0721ec03`.
- Candidate build:
  `production-secret-20260808T144842-5472a382`.
- Candidate run:
  `static-site:production-secret-20260808T144842-5472a382:acbd40ef5203`.
- Candidate snapshot: `snapshot-20260808T124842-4786ac53bc`.
- Candidate manifest SHA-256:
  `d615f6e447dc8c6ae3b876bf4a99123d1c85afee55276c26645f020b26074322`.
- Candidate Astro / Node: `6.4.8` / `22.12.0`.
- Separate current public-root prelaunch source:
  `5a9d804438377f65fe4b26bd7019e73626529864`.
- Public-root release: `prelaunch-main-31263560430-1`; exact observed root
  HTML SHA-256:
  `1c31504d10d9ec66c7fa84ad52c94e6019a741f0ee01826f219578963e0ea21e`.

The public root is not described as the full production corpus. Its runtime and
source evidence remain a separate plane from the checked Kaggle candidate.

## Local full-run acceptance

The ignored local snapshot
`artifacts/current-ui-resource-graph/snapshot-20260808T124842-4786ac53bc-final-local-v13`
completed with manifest SHA-256
`9e369a1c0e98bf20a8b954b239d56a1c7a69850f4ec9de08b2e65be9d3fd9458`.

- runtime observations: 1,267 (1,266 candidate HTML routes + public root);
- source records: 492 (244 candidate + 248 public-root source plane);
- page families: 19 (non-Astro data/manifest/robots/sitemap endpoints excluded);
- observed/candidate UI families: 18 / 20;
- source and computed style observations: 34,985;
- detected style inconsistencies: 809;
- fragmentation/candidate graph rows: 20 / 20;
- logical fragmentation candidates: 16; identical cross-plane source paths do
  not create a second implementation;
- desktop/mobile records: 19 page-family + 20 observed-UI-family scopes;
- screenshot index rows: 41 (40 captured, 1 explicit source-only uncaptured);
- screenshots: exact `390x844` and `1728x900`, stable perceptual dHash indexed;
- all manifest output hashes verified;
- candidate bearer URL byte-scan: absent from every artifact file;
- receipt: `complete`, deterministic budgeted output bytes: 76,353,200.

Two complete browser-enabled reruns produced byte-identical canonical graph
shards, summary, screenshot index, manifest and receipt. Raw Chromium JPEGs are
explicitly noncanonical visual specimens: each accepted image is stable across
two consecutive buffers, while cross-run acceptance uses equal 64-bit
perceptual dHash. This avoids misrepresenting sparse raster/media differences as
resource-graph changes.

Coverage reconciliation reports 14 `FOUND`, 3 `MISSING`, 5 `DISCOVERED`
and no `AMBIGUOUS` hypotheses. In particular, Exhibitions, For Me/personal
feed, Interest Clubs and marker-backed Hero-talk are found. Editorial
Collections, Legal documents and Hero-talk page-end remain explicitly missing;
no synthetic archetype was created. Artifacts, closed-poster, focus-group,
labs/preview-special and unusual pages were discovered outside the supplied
hypothesis list.

## Validation

```text
node --check scripts/current_ui_resource_graph/decode.mjs        PASS
node --check scripts/current_ui_resource_graph/graph-lib.mjs     PASS
workflow YAML parse                                              PASS
git diff --check                                                 PASS
pytest -q tests/test_current_ui_resource_graph.py                21 passed
full exact source/runtime/browser scan                            PASS
manifest shard hash/byte verification                            PASS
screenshot physical dimension/perceptual fingerprint             PASS (40)
canonical full-rerun byte comparison                              PASS
secret redaction byte scan                                       PASS
manual screenshot review: local + Actions, all 40 specimens     PASS WITH OBSERVATION
  production representatives/outliers: no blank/error/404 or wrong viewport
  labs-preview-special mobile: shell-only versus full desktop event; recorded
  as viewport conflict, not normalized or treated as a responsive variant
```

## Delivery evidence

The decoder landed on the default branch and completed in GitHub Actions:

- successful run: [`31274711696`](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31274711696),
  source `68774e001818adf1e56284396293626872ee2fef`;
- artifact ID `9026814725`, name
  `current-ui-resource-graph-snapshot-20260808T124842-4786ac53bc`;
- artifact digest
  `sha256:7072a4ceb746ebd20733103bcac312bbb090fd5c40a545f3d8c17c94e01306bd`,
  compressed size 8,272,118 bytes, retained through 2026-09-07;
- downloaded receipt `complete`, manifest SHA-256
  `9e369a1c0e98bf20a8b954b239d56a1c7a69850f4ec9de08b2e65be9d3fd9458`;
- all 14 canonical files are byte-identical to final local v13, every declared
  output hash/byte count verifies, and the bearer/redaction scan passes;
- all 40 downloaded screenshots have their indexed physical size (20 at
  `390x844`, 20 at `1728x900`). All four Actions contact sheets were reviewed
  manually, followed by original-size review of the independently changed raw
  event-detail, Exhibitions, Home and lab specimens. No new blank/error/404 or
  viewport problem was found; the known lab mobile shell-only conflict remains
  unresolved evidence.

Earlier failed runs uploaded honest failed receipts while exposing workflow
schema/path defects; they are not accepted graph artifacts. The successful run
above supersedes them and closes the durable-artifact delivery gate.

## Event-presentation format follow-up

The follow-up merged in PR
[`#400`](https://github.com/onedayonemasterpiece/events-bot-new/pull/400) and
the bounded-evidence correction in PR
[`#402`](https://github.com/onedayonemasterpiece/events-bot-new/pull/402).
It adds `event-presentation-formats.jsonl` without changing `site/src`, Astro,
CSS, UI, Penpot, tokens, contracts or normalization decisions.

The exact 1,096 Event Detail documents now retain these structural facts:

- desktop family and CTA layout: 96 editorial/stacked versus 1,000
  split/inline;
- explicit split portrait state: 69 (57 portrait-or-square plus 12
  low-resolution portrait viewer);
- no-image split fallback: 63;
- primary large media frame: 1,096;
- split small-photo rail: 217; editorial small hero rail: 29;
- large editorial poster companion: 11; large poster plus adjacent small photo
  previews: 8.

Eleven format records preserve layout, CTA and media resources. CTA entries
map to real `DesktopEventActionPanel` family variants. The media frame, rails,
poster companion and small previews remain honestly classified as
`internal_conditional_resource` boundaries inside `DesktopEventPage`; the
decoder does not invent new Astro components. Every record remains
`NOT_MERGED / unresolved`.

The accepted follow-up artifact is:

- successful Actions run
  [`31278123911`](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31278123911),
  head `63e411b390489d2a4bbe509622335d00f303416b`;
- artifact ID `9027776108`, digest
  `sha256:2ebb59d1a14ec4134c192c86aad8a8295f77577990398543cfef38f570f75b6c`,
  compressed size 8,897,580 bytes;
- receipt `complete`, manifest SHA-256
  `8f7de76208e2505aae39ebcfa258d55ef2a59bc76fbb7bf584ae99a05cec383c`;
- 15 required canonical files; 83,577,401 receipt output bytes under the
  explicit 90 MiB cap;
- 47 screenshot-index rows: 46 captures and one explicit uncaptured
  source-only row; physical dimensions are 23 at `390x844` and 23 at
  `1728x900`;
- all declared output hashes and byte counts verify; the candidate bearer and
  authorization scan is clean.

Human review covered all 46 captures through labelled contact sheets and the
five new Event Detail pairs at original size. It visibly confirmed the split
poster/content layout with inline CTA, editorial landscape hero with separate
stacked CTA, explicit portrait state, no-image fallback, and the editorial
companion board containing one large poster plus smaller remaining-photo
previews. No production-family representative showed a 404, browser error,
wrong viewport or wholly blank main content. The known lab-only mobile shell
versus full desktop observation remains unresolved.

Source inventory and conflict language remain deliberately conservative. The
artifact contains 213 plane-qualified component rows covering 107 logical
component paths; the heuristic UI-family cohorts cover 80 paths while 27
remain present but unclassified. There are 16 fragmentation candidates and 809
style divergence candidates, but no component/token conflict has been
normalized or confirmed as an equivalence decision. Color and typography
remain raw plane-scoped PostCSS/computed observations, not palette or type
tokens.
