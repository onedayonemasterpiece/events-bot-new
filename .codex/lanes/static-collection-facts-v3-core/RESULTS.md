# static-collection-facts-v3-core — results

## Scope

- Lane: `static-collection-facts-v3-core`
- Requirements: R01–R03 plus R02 bounded-call guarantees from draft PR #226.
- Base SHA: `5aa9958a894b4b01f89410a05aab03aafd7703d7`
- Validated implementation SHA: `89beb60df1bc1b80c462da890fe2c051408c8559`
- Evidence-file commit: the commit containing this file (created after the
  validated implementation commit to avoid a self-referential SHA).

## Delivered

- Bumped the collection policy/schema to facts v3/adjudication v2.
- Replaced LLM-owned legacy audience output with independent
  `child_directed`, `family_suitable` and `joint_family_activity` decisions.
- Added exact continuous-quote and narrow entailment validation, including
  explicit-negative-only `denied` and impossible-joint rejection.
- Revalidated v3 quotes against persisted same-event `EventSource.source_text`
  at apply time.
- Added independent deep merge, official-first trust, manual locks,
  unknown-preserves-truth behavior and deterministic legacy projection.
- Added bounded validated `(source_id,input_hash)` receipts covering
  all-unknown results and reusable warm-coverage helpers.
- Added reason-filtered audience-only apply without admission, people or
  `is_free` mutation.
- Kept `collection_candidate_adjudication` on Gemma 4 even under staged Gemini;
  added a per-call GoogleAIClient ceiling so the label performs one native
  schema primary send, no Google retry/model fallback/JSON repair, and at most
  one existing GPT-4o fallback send.
- Extended Smart Update trace with physical-send/model/token evidence.
- Documented the production contract in the canonical Smart Update README.

## Validation

Passed:

```text
/home/dev/.codex/venvs/events-bot-new/bin/python -m pytest \
  tests/test_smart_event_update.py \
  tests/test_event_update_merge.py \
  tests/test_google_ai_client.py \
  tests/test_smart_update_native_schema.py -q
126 passed in 5.15s

python3 -m py_compile smart_event_update.py google_ai/client.py
git diff --check
```

Observed before integration (expected cross-lane dependency):

```text
tests/test_static_collection_backfills.py: 3 passed, 1 failed
```

The failing fixture still emitted the v1 `audience_decision` provider schema.
The assigned backfill lane owns and has been notified to migrate that fixture
and consume `collection_decision_hash_covers`; no backfill files were edited in
this lane.

## Changed files

- `smart_event_update.py`
- `google_ai/client.py`
- `tests/test_smart_event_update.py`
- `tests/test_event_update_merge.py`
- `tests/test_google_ai_client.py`
- `docs/features/smart-event-update/README.md`
- `.codex/lanes/static-collection-facts-v3-core/RESULTS.md`

## Risks / integration notes

- The narrow post-LLM entailment guards intentionally prefer abstention over a
  false confirmation; real-data replay may reveal wording that needs an
  ontology-safe validator extension.
- Evaluation receipts are bounded to 24 per event. This prevents unbounded JSON
  growth but deliberately expires the oldest source/hash combinations.
- GPT-4o physical-send count is recorded at the existing wrapper boundary;
  primary Google sends are observed exactly after limiter `mark_sent`.
- `CHANGELOG.md` is outside this lane's writable scope and must be updated by
  integration.
- This lane does not claim real-provider replay, production-copy apply/warm or
  Fly canary; those belong to the integration/acceptance lanes.
