# Golden Review Preview v1 — N0 acceptance and transition contract

Status: `SOURCE_READY_RUNTIME_UNVERIFIED`  
Coordination: `onedayonemasterpiece/events-bot-new#621`  
Owner: `N0`  
Contract: `kenigevents.launch-normalized-ui.v1@1.9.0`

## 1. Scope

This contract closes the source-owned part of PM0 checklist items 4 and 6–10.
It does not claim a build, deployment, browser verdict, `ASTRO_NORMALIZATION_PASS`,
thin S, Penpot parity or release acceptance.

The Golden Review Preview is a deterministic data mode of the existing Astro
product. It is not a second site, a component lab or an owner-facing replacement
for the fresh-real-data preview.

```text
same Astro routes
+ same canonical roots and consumers
+ frozen Golden data/clock/assets
= deterministic internal A=S=P review surface
```

Forbidden:

- a `/golden/` or `/lab/golden/` implementation;
- copied EventCard, ListingEventCard, MediaFrame, AdaptiveEventCardGrid or route
  compositions;
- mutation of the frozen first real-preview transaction;
- use of Golden data as evidence for PM0 checklist item 5, which still requires
  the real production-form/Kaggle generation path;
- publication to the stable site root or any non-versioned prefix.

## 2. Source lineage

The N0 source branch starts from the accepted successor candidate:

```text
r0/ui-normalization-current-candidate-20260903
0d73428dfafff2fd5450b74fd68e7bb40e92d2c5
```

N0 branch:

```text
work/ui-normalization-n0-checklist-20260903
```

The branch must be integrated only into a successor candidate. It must never be
folded into the already frozen first-preview transaction:

```text
repo SHA: d5ff87bcb7a2b2051ad956ef9e7e2733a1ae62c3
build ID: preview-ui-normalized-d5ff87bc-fresh-20260903-v1
```

## 3. Golden corpus

Canonical source:

```text
site/scripts/golden-review-corpus.v1.json
schema: kenigevents.golden-review-corpus.v1
corpus: golden-review-20260903-v1
```

Frozen civil clock:

```text
timezone: Europe/Kaliningrad
current Friday: 2027-06-04
Saturday:       2027-06-05
Sunday:         2027-06-06
reference ISO:  2027-06-04T06:00:00.000Z
```

Density and route reuse:

```text
Friday /segodnya/:             5 events (minimum 4)
Saturday /zavtra/:             6 events (minimum 5)
Sunday /date-2027-06-06/:      5 events (minimum 4)
Weekend /vyhodnye/:           11 exact Saturday/Sunday occurrences
Dated Weekend /vyhodnye/2027-06-05/:
                              the same exact 11 occurrences
Free collection:               6 exact active free/registration specimens
```

The corpus reserves IDs `970001..970099`; v1 uses 16 unique IDs, slugs and
`golden:n0:<id>` source identities. Existing identities and pinned assets are
append-only and immutable from `2026-09-03T12:00:00Z` through
`2026-09-17T12:00:00Z`.

The stress matrix includes:

- cover, contain, multiple media, no media, semantic error, stale and pending;
- long title, long venue and long description;
- free, ticket, registration, phone and source-only admission;
- single-day and cross-midnight calendar cases;
- rescheduled and cancelled lifecycle cases;
- Pushkin-card and untimed cases.

Pinned image and SVG identities are recorded by public path and Git blob SHA;
the reviewed landscape asset also carries its byte SHA-256.

## 4. Data isolation

`build-golden-preview.mjs` temporarily overlays the corpus on the current
`preview-events.json` data and invokes the ordinary `build-preview.mjs`.
Historical real-event canaries are retained so existing build/check contracts
remain executable, while the frozen 2027 target dates contain only Golden
specimens.

The builder:

1. creates an exclusive data lock;
2. records the original real-data SHA-256;
3. materializes and writes the temporary Golden overlay;
4. runs the ordinary Astro preview builder with the frozen date/reference;
5. restores the original file in `finally`;
6. verifies byte-identical restoration;
7. writes corpus and machine-readable evidence under the versioned build root.

A failed build is not allowed to turn Golden data into the repository baseline.
A stale lock is a fail-closed diagnostic requiring R0 inspection, not permission
to delete or overwrite source blindly.

## 5. Build metadata

The ordinary `preview-build.json` now distinguishes:

```json
{
  "dataMode": "real | golden",
  "goldenCorpusId": "golden-review-20260903-v1 | null",
  "goldenCorpusDigest": "<sha256> | null"
}
```

Real preview remains the default and requires no Golden variables. Golden mode
fails closed unless both corpus ID and SHA-256 are supplied by the isolated
builder.

## 6. R0 executable gate

After the frozen real-preview transaction is completed or rejected with exact
evidence, R0 may integrate this N0 branch into the accepted successor candidate.
No semantic redesign or corpus editing is delegated to R0.

Required source checks on the integrated successor SHA:

```bash
cd site
npm run test:golden-preview-contract
npm run check:design-system-production-surfaces
npm run check:design-system-iconography
```

Freeze the Golden transaction only after the integration head is known:

```bash
GOLDEN_SHA="$(git rev-parse HEAD)"
GOLDEN_BUILD_ID="preview-golden-${GOLDEN_SHA:0:8}-20270604-v1"
```

The local commands below are diagnostic only. They do not create publishable
evidence and must never upload `site/dist` directly:

```bash
PREVIEW_BUILD_ID="$GOLDEN_BUILD_ID" npm run build:golden-preview
PREVIEW_BUILD_ID="$GOLDEN_BUILD_ID" npm run check:golden-preview
```

`check:preview` and `check:unified-prototype` remain fresh-real-data successor
gates: their current-catalog recommendation, transport and mutual-occurrence
specimens are intentionally not reclassified as Golden evidence. Golden uses
its own full-route, density, media, lifecycle, action and restoration gate.

The reproducible publishable transaction is owned by `events-bot-new` and uses
the same Kaggle runner, checked artifact handoff and Object Storage publisher as
real preview. Only `--preview-data-mode` and the immutable build ID differ:

```bash
python scripts/run_static_site_builder_kaggle.py \
  --profile preview \
  --preview-data-mode golden \
  --page-class all \
  --repo-sha "$GOLDEN_SHA" \
  --build-id "$GOLDEN_BUILD_ID" \
  --download-output \
  --publish-preview
```

The runner requires an explicit `preview-golden-*` ID, executes
`build:golden-preview`, all Golden/full-preview gates, validates the returned
archive and publishes create-only below exactly:

```text
s3://<bucket>/<GOLDEN_BUILD_ID>/
```

## 7. N0 acceptance

N0 accepts the Golden preview only from one R0 result containing all of:

- exact integrated repo SHA and ancestry;
- exact corpus ID and SHA-256;
- exact command exits for both source tests and all build/check commands;
- evidence that real `preview-events.json` SHA-256 before and after matches;
- exact 5/6/5 route membership and exact 11-event Weekend reuse;
- pinned asset Git-blob/byte verification;
- immutable-prefix dry-run and actual deploy evidence;
- public 2xx readback of:
  - `/<GOLDEN_BUILD_ID>/__preview/`;
  - `/<GOLDEN_BUILD_ID>/preview-build.json`;
  - `/<GOLDEN_BUILD_ID>/golden-review-corpus.v1.json`;
  - `/<GOLDEN_BUILD_ID>/data/golden/evidence.json`;
- required MIME/asset readback;
- confirmation that the stable root and frozen first-preview prefix were not
  mutated.

Any mismatch rejects the Golden prefix. A failed Golden gate never authorizes a
stable-root deployment or a fallback to another corpus/date/SHA.

## 8. Product transition

The accepted order is:

```text
frozen fresh-real preview reaches public 2xx
→ V0 audits that exact real preview
→ R0 integrates accepted successor role outputs, including this N0 source branch
→ successor real preview passes local gates and reaches public 2xx
→ Golden preview is built from the exact same successor SHA
→ V0 audits real and Golden route/viewport matrices
→ owning roles close critical DRIFT
→ N0 issues ASTRO_NORMALIZATION_PASS only on factual V0 evidence
→ thin S / Penpot / release candidate
```

The Golden build may be materialized while V0 audits the successor real preview,
but it cannot substitute for that real-data audit and cannot itself authorize
`ASTRO_NORMALIZATION_PASS`.

## 9. V0 trigger

The Golden trigger is the exact public URL plus matching `preview-build.json`
and Golden evidence recorded in issue #621. V0 resolves all event-detail routes
from the published corpus and personally performs DOM/computed-style review.
R0 local smoke is not the V0 verdict.

Required V0 facts include:

- exact Date and Weekend root identities and states;
- exact route membership and chronology;
- same Saturday/Sunday occurrence identities on both Weekend routes;
- MediaFrame cover/contain/fallback/error behavior;
- AdaptiveEventCardGrid final-line occupancy and overflow;
- long-copy containment;
- admission and calendar states;
- lifecycle presentation for rescheduled/cancelled specimens;
- pinned image/SVG identity and no duplicate wrapper/style owner.

## 10. Checklist classification before runtime

```text
4  Golden build/check/deploy:       PARTIAL — source ready, runtime/public 2xx pending
6  frozen clock:                    SOURCE PASS, built-output pending
7  5/6/5 density:                   SOURCE PASS, built-output pending
8  Weekend exact occurrence reuse: SOURCE PASS, built-output pending
9  stress cells:                    SOURCE PASS, browser verdict pending
10 identity/stability lock:         SOURCE PASS, runtime asset readback pending
```

No item above becomes `DONE` merely because this branch exists.
