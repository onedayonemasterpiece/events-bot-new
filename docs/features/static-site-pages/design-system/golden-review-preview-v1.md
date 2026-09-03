# Golden Review Preview v1 — N0 acceptance and transition contract

Status: `GOLDEN_KAGGLE_ACCEPTED_V0_PENDING`  
Coordination: `onedayonemasterpiece/events-bot-new#621`  
Owner: `N0`  
Contract: `kenigevents.launch-normalized-ui.v1@1.9.0`

## 1. Product boundary

Golden is a deterministic data mode of the one existing Astro product and the
one existing `events-bot-new` Kaggle `StaticSiteBuilder`. It is not a second
site, component lab, exporter, publisher or owner-facing replacement for a
fresh-real-data Review Preview.

```text
same Astro routes/components
+ same Kaggle runner, checked artifact and Object Storage publisher
+ preview-data-mode=golden
+ frozen corpus/clock/assets
= reproducible Golden Review Preview
```

A full or published Golden build may never use a local deployment wrapper.
`my-data-hub` remains only the MCP operation facade and may not duplicate the
exporter, page-class selector, builder, publisher or retention implementation.

## 2. Accepted immutable Golden transaction

N0 accepts the exact transaction reported in issue comment `5527249164`:

```yaml
URL: https://kenigevents.ru/preview-golden-84504f30-20270604-v1/__preview/
HTTP: 200
build_id: preview-golden-84504f30-20270604-v1
repo_sha: 84504f30eebc334deba46e94365601c3d572c5c0
data_mode: golden
page_classes: [all]
golden_corpus_id: golden-review-20260903-v1
golden_corpus_sha256: f90561691baa6744804b6cba36b0a8580eac8e9607dcdb97483511ad32e7ce00
Kaggle_dataset: zigomaro/static-site-builder-input-20260903141925-94f0f2
Kaggle_kernel: zigomaro/kenigevents-static-site-builder
Kaggle_status: COMPLETE
```

Accepted ancestry for that transaction:

```text
0d73428dfafff2fd5450b74fd68e7bb40e92d2c5
→ e7e8802675103a04c207564f8dccd88a8c0c65de
→ e3f6f9dcd5f0103145389556f3e8fbc9b1e59b7b
→ c124ca2a810f100122230b7f51ec1221f7e74be4
→ 84504f30eebc334deba46e94365601c3d572c5c0
```

The result records 1,450 create-only published objects, independent SHA/MIME
readback, `root_mutation=false`, `stable_ics_mutation=false`, 36/36 HTTP-200
document cases and zero document-level horizontal-overflow cases. The
build-prefixed asset origin also repaired the previously missing transport
asset. These facts are accepted as Golden pipeline and corpus evidence.

A newer source branch head does not inherit this runtime result. Every later
candidate requires its own exact Kaggle operation, artifact and public manifest.

## 3. Frozen corpus contract

Canonical source:

```text
site/scripts/golden-review-corpus.v1.json
schema: kenigevents.golden-review-corpus.v1
corpus: golden-review-20260903-v1
```

Frozen civil clock:

```text
timezone: Europe/Kaliningrad
Friday/current date: 2027-06-04
Saturday:            2027-06-05
Sunday:              2027-06-06
reference ISO:       2027-06-04T06:00:00.000Z
```

Published density is exactly `5/6/5`. Current and dated Weekend routes reuse the
same exact 11 Saturday/Sunday occurrences. The corpus covers cover/contain,
multiple and missing media, semantic error, long copy, free/ticket/registration/
phone/source admission, single/range calendar cases and cancelled/rescheduled
states. Event, image and SVG identities are pinned; the two-week append-only
window remains active through `2026-09-17T12:00:00Z`.

## 4. Local diagnostic versus publishable evidence

Local diagnostic is permitted only to shorten a defect loop:

```bash
PREVIEW_BUILD_ID="preview-golden-<sha8>-20270604-v1" \
  npm run build:golden-preview

PREVIEW_BUILD_ID="preview-golden-<sha8>-20270604-v1" \
  npm run check:golden-preview
```

It must not upload `site/dist`, update a current pointer or receive owner, V0,
PM0 or A=S=P build credit.

The only publishable Golden transaction is:

```bash
python scripts/run_static_site_builder_kaggle.py \
  --profile preview \
  --preview-data-mode golden \
  --page-class all \
  --repo-sha "<exact-integrated-sha>" \
  --build-id "preview-golden-<sha8>-20270604-v1" \
  --download-output \
  --publish-preview
```

The same runner uses `--preview-data-mode real` for the fresh-production Review
Preview. `catalog-mode: slice|full` remains event-data scope and is not the
page-class selector.

## 5. Acceptance classification

The accepted transaction supplies current durable evidence for:

```text
PM0-4  full Golden Kaggle build/check/publication path
PM0-6  frozen Europe/Kaliningrad clock and Friday/Saturday/Sunday dates
PM0-7  exact 5/6/5 density
PM0-8  exact Weekend occurrence reuse
PM0-9  published stress matrix; independent visual verdict still pending
PM0-10 pinned corpus/assets; append-only stability window still active
```

It does **not** complete:

```text
PM0-3  fresh-real full Kaggle Review Preview
PM0-5  production-form or Release Candidate
voice-review readiness
V0 visual PASS
ASTRO_NORMALIZATION_PASS
thin S or Penpot equality
```

R0 browser smoke is useful defect evidence but is not an independent V0 verdict.

## 6. Current V0 trigger and drift routing

V0 may immediately audit the exact accepted Golden URL above. The audit must
inspect actual DOM and computed styles across the Golden route/viewport matrix,
including MediaFrame cover/contain/fallback/error states, Adaptive grid
occupancy, long-copy containment, admission/calendar/lifecycle states and the
internally scrollable Today date rail.

Any `[DRIFT]` is routed to its existing owner. N0 does not issue a browser PASS
and does not infer one from 36/36 document responses.

## 7. Next common product gate

The nearest common gate remains:

```text
one current integrated successor SHA
+ fresh immutable production snapshot with source time/count/hash
+ full events-bot-new Kaggle build using preview-data-mode=real and page-class=all
+ checked artifact and create-only immutable-prefix publication
+ HTTP-200 exact /<buildId>/__preview/
+ matching preview-build.json and Kaggle operation/artifact identity
→ independent V0 DOM/computed-style verdict
```

Golden cannot substitute for the fresh-real preview or turn the voice-review
gate green.

## 8. Rollback and prohibitions

- Never publish a full/focused owner preview outside the canonical Kaggle rail.
- Never use `deploy:preview` or `deploy:golden-preview` as a launch path.
- Never mutate a stable root, stable `/ics/` keys or another immutable build
  prefix.
- Never reuse a successful build verdict for another SHA.
- Never treat source tests, local diagnostic output or R0 smoke as V0 PASS.
- A failed successor or Golden gate leaves the previous immutable result intact
  and rejects only the new prefix.
