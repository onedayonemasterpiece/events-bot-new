# PR-A provisional audience-truth correction

Status: **complete**. Publication remains blocked; this lane does not create PR B artifacts.

## Editorial corrections

- `6562`: moved from `family_suitable.hard_negatives` to the
  high-confidence positive supply. Exact EventSource `7734705` contains
  `Приходите всей семьёй!`; reason is `explicit_whole_family_invitation`.
- `7102`, `7258`, `7290`: removed from `family_suitable` and
  `joint_family_activity` positives.
- `7172`, `7176`: removed from `child_directed` positives.
- `6898`: removed from `family_suitable`; its already-absent joint status is
  protected by a label-specific regression assertion.
- No replacement rows were added.

Corrected counts:

| label | positives | high-confidence positives | hard negatives |
|---|---:|---:|---:|
| `child_directed` | 16 | 12 | 20 |
| `family_suitable` | 11 | 9 | 23 |
| `joint_family_activity` | 1 | 1 | 24 |

## Provenance and denominator contract

- Data commit / `integration_repo_sha`:
  `08ca743839ea86866b51869164c4c254b876368f`.
- Extraction and seed-builder SHAs are unchanged.
- Source-review index SHA-256:
  `82d0c5ddd824af46aa0e64dd65ae4fdb814f2c7541d5166d408f71ad1db16649`.
- New corrected receipt hashes:
  - `6562.json`: `3451dddf9fd52025ab594a429c3bfc72317e2f4ee6d95bc2e2f9126bb4703130`;
  - `6898-7102-7172-7176-7258-7290.json`:
    `b89b21a508fa04b84fa193552f22f7dd64c29e8e004ca43aa7f07795f9367394`.
- All counted rows explicitly use `source_status=sufficient` under
  `static-collection-source-status-v1`. `insufficient` and
  `needs_source_review` are valid statuses but cannot enter review supply.
- The committed generator command reproduced the seed byte-for-byte against
  the ignored canonical evidence snapshot.

## Validation

Focused regressions:

```text
python -m pytest \
  tests/test_static_collection_quality_validator.py \
  tests/test_static_collection_review_seed.py \
  tests/test_static_collection_data_quality_reviews.py -q
22 passed in 0.76s
```

Review gate:

```text
validate_static_collections_quality.py --mode review
PASS; 0 errors; 11 warnings
```

Warnings are expected: six positive-supply shortfalls and five bindings that
belong to PR B. Strict mode remains expected-fail because owner gold, scores,
prototype winners and complete hash bindings do not exist; no PR B artifact was
added.
