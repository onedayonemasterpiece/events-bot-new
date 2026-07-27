# Lane R15-L02-SEMANTIC Results

## Status

committed

## Requirement IDs

- R08

## Branch

`agent/unusual-r15/semantic`

## Worktree

`/home/dev/.codex/worktrees/events-bot-new/unusual-r15-semantic`

## Base SHA

`31b72b93153c094ca16cd564bfdc6b56c2031867`

## Head SHA

Implementation commit: `a6d7a35f5ccb55a0a618f1361d7e154629454dbc`

The lane-results metadata is committed immediately after that implementation
commit; its final branch SHA is reported to the integrator at handoff.

## Files changed

- `site/scripts/static_event_bge.py`
- `site/scripts/unusual_event_semantics.py`
- `site/scripts/unusual_event_prototypes.v1.json`
- `site/scripts/unusual_event_classifier.v1.json`
- `tests/test_unusual_event_semantics_r15.py`
- `.codex/lanes/R15-L02-SEMANTIC/RESULTS.md`

## Delivered contract

- The shared CPU BGE boundary builds the existing canonical `related_v1`
  document and makes one pinned BGE-M3 encode invocation for event and prototype
  documents. The unusual scorer contains no encoder invocation.
- Exact model `BAAI/bge-m3`, revision
  `5617a9f61b028005a4858fdac845db406aefb181`, 1024 dimension,
  document version, L2 normalization, artifact, prototype-bank and classifier
  hashes are validated before scoring.
- The frozen bank covers all 15 required families with 30 positive, 15
  family-specific hard-negative and 10 neutral concrete Russian prototypes.
- Margin features, deterministic logistic head, public tiers
  `core_unusual|adjacent|ordinary|abstain`, hard eligibility, reason/prototype
  evidence, explicit concept hierarchy, mutual occurrence links, stable
  presentation-independent concept fallback and series dedup are implemented.
- `score_unusual_manifest(...)` returns `manifest`, `cache`, and `metrics`.
  Migration and every decision use `notify=false`; provider calls are zero.
- `evaluate_unusual_quality_fixture(...)` calculates hash-bound real-BGE canary
  metrics and deterministic repeat/frozen-flip evidence without duplicating
  classifier logic in the builder.

## Commands run

```text
python3 -m pytest -q tests/test_unusual_event_semantics_r15.py
/home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -p no:cacheprovider -q tests/test_unusual_event_semantics_r15.py
/home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -p no:cacheprovider -q tests/test_event_vector_sync.py tests/test_unusual_event_semantics_r15.py
/home/dev/.codex/venvs/events-bot-new/bin/python -m py_compile site/scripts/static_event_bge.py site/scripts/unusual_event_semantics.py tests/test_unusual_event_semantics_r15.py
/home/dev/.codex/venvs/events-bot-new/bin/python site/scripts/static_event_bge.py --help
/home/dev/.codex/venvs/events-bot-new/bin/python site/scripts/unusual_event_semantics.py --help
git diff --check
```

## Tests / verification

- Focused semantic suite: `6 passed`.
- Semantic suite plus existing event-vector sync regression: `21 passed`.
- Python compile: passed.
- Both CLI help/syntax probes: passed.
- `git diff --check`: passed.
- Read-only checked-example probe:
  - source:
    `artifacts/codex/unusual-events-20260727/manual_future_taxonomy.csv`;
  - 56 checked rows in the research set;
  - 37 still present in the current committed static fixture;
  - 37/37 produced unique canonical `related_v1` documents.
- Frozen artifact hashes at implementation commit:
  - prototype bank:
    `f85044901f2f0c6c99c91c4478f9221423a6acbeb083afcd970ccb7d11730c09`;
  - classifier:
    `edf0064379484656fe9e415d76ab77ab4c2eac226135e6327d14d11cdacc72a8`.

## Risks

- **Deliberately shadow/not approved:** no genuine BGE-M3 canary vector run and
  no sufficiently sized frozen editorial/hard-negative/confirmed-positive
  evaluation were available in this lane. The checked-in classifier says
  `approval_status=not_approved`; absent/incomplete/non-real evidence leaves
  manifest publication `items=[]`.
- Approval requires all frozen thresholds: precision@20 >= 85% or within five
  percentage points of the 88% reference; hard-negative FPR <= 5%; confirmed
  unusual recall >= 80%; no more than one duplicate concept in top 20; identical
  rebuild flip rate < 2%; exact deterministic repeat; one vector contract; zero
  ineligible publications; top-20 family diversity >= 5; and minimum sample
  sizes of 20 for editorial, hard-negative and confirmed-positive sets.
- The initial system Python had no pytest. The project Codex venv was used.
  A stale corrupt bytecode file was encountered once while the filesystem was
  full; generated `__pycache__` files in this worktree were removed, and the
  clean no-cache runs above passed.
- Canonical documentation and `CHANGELOG.md` are intentionally not edited in
  this disjoint lane; R15-L06 owns them.

## Merge notes

- Merge the implementation commit and the following lane-results commit.
- L03 should call
  `build_shared_bge_vector_artifact(..., classifier=load_unusual_classifier())`,
  set `artifact.metadata.build.evidence_kind=real_bge_canary` only for an actual
  pinned BGE run, then call `evaluate_unusual_quality_fixture(...)` and pass its
  output as `build_metadata.quality_evaluation` to
  `score_unusual_manifest(...)`.
- `build_metadata.as_of_date` is mandatory ISO `YYYY-MM-DD`; missing or invalid
  metadata fails closed.
