# Event Age BGE Assessment (CPU)

CPU-only gated batch worker. The kernel only writes hash-bound artifacts; the
Fly importer rechecks current source/OCR hashes and never changes a declared
rating.

Input dataset files:

- `event_age_bge_input.jsonl`: `event_id`, `input_hash`, whole-product `text`,
  optional `declared_age`;
- `event_age_bge_prototypes.json`: versioned prototype metadata/text;
- optional `event_age_bge_prototype_vectors.npz`: `vectors`, `model_revision`,
  `encoder_contract`, `prototype_bank_hash`;
- optional `event_age_bge_classifier.npz`: either the legacy dense dual head or
  the approved raw-matrix `char_tfidf_safety_cascade_v1` bundle
  (vocabulary/IDF/coef/intercept only; no pickle/joblib);
- optional `event_age_bge_evaluation.json`: automatic quality-gate result,
  exact hashes, calibrated cascade thresholds and deterministic startup
  text→logits→decision parity cases. Human approval fields are neither required
  nor accepted as the gate authority.

`event_age_bge_run.json`, input JSONL and prototype bank must live in the same
private input dataset. A separate prepared-artifact dataset must not duplicate
those names; it contains only vectors/classifier/evaluation files.

The Kaggle kernel has `enable_gpu=false`. A pinned
`EVENT_AGE_BGE_MODEL_REVISION` is mandatory. A cold shadow run computes and
exports prototype vectors. Missing, mismatched, or automatically rejected
classifier artifacts produce terminal abstentions rather than numeric guesses.
The bootstrap probes the actual `BGEM3FlagModel` symbol and upgrades to the
Transformers-5-compatible `FlagEmbedding==1.4.0` when Kaggle exposes an older,
incompatible preinstalled package.

Manual canary through the standard status ledger/heartbeat path:

```bash
python kaggle/execute_event_age_bge_assessment.py \
  --dataset-source USER/PRIVATE-INPUT --run-id event-age-bge-canary
```

Canonical private kernel ref: `zigomaro/event-age-bge-assessment-shadow`.

The launcher checks both states: Kaggle kernel `COMPLETE` and the downloaded
worker report. A bounded `partial` report is returned as a non-zero launcher
result with `events_done/events_total`; it must be followed by a remainder
batch and must not be presented as a completed calibration.

Outputs: `event_age_bge_result.json`, `event_age_bge_event_vectors.npz`, optional
cold-start prototype vectors, and a partial checkpoint under `/kaggle/working`.

Normal production scheduling is not this manual launcher: Smart Update
coalesces all missing/stale events for 25 minutes into one `JobOutbox` batch.
Only approved event-scoped OCR (`ocr_title` and `ocr_text`) is placed ahead of
long prose. A row is imported only when the result input hash is still current.

Automatic calibration:

```bash
python scripts/build_event_age_bge_calibration.py \
  --db snapshot.sqlite --output-dir artifacts/codex/age-calibration

python scripts/calibrate_event_age_tfidf.py \
  --vectors event_age_bge_event_vectors.npz \
  --inputs artifacts/codex/age-calibration/event_age_bge_input.jsonl \
  --labels artifacts/codex/age-calibration/event_age_bge_labels.jsonl \
  --scope-review artifacts/codex/age-calibration/gemini-scope-review.json \
  --prototype-bank kaggle/EventAgeBgeAssessment/event_age_bge_prototypes.json \
  --output-dir artifacts/codex/age-calibration/bundle
```

The gold builder masks explicit age tokens before encoding. Only scope-clean
source-declared labels enter grouped OOF acceptance. The final cascade uses two
development seeds and one untouched seed, exports only raw matrices, verifies
pure-inference parity against sklearn at build time, and is activated only when
all hash-bound startup self-tests and quality gates pass.
