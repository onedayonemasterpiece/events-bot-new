# Publisher profile import lane results

- Lane: `publisher-import`
- Requirements: `R04`, `R05`, `R07`
- Branch: `agent/region-talk-source-profile-recovery/publisher-import`
- Base SHA: `ba8ab078ba9894ccd5810045b1b8787ecb29d743`
- Implementation head SHA: `b369da11a60c670b83e74800c278902ebfc026b0`
- Production publication effect: **none**

## Requirement status

| Requirement | Status | Evidence |
|---|---|---|
| R04 — dedicated publisher profile import | Done | Exact UTF-8 bytes, duplicate-key rejection, Draft 2020-12 schema and semantic validation; raw input/profile/evidence hashes; `domain:<domain>` preserved and canonical runtime `web:<domain>` stored; dry-run default and explicit `--execute`; serializable rereads of batch, receipt, profile, correction, identity and intake rows; atomic writes; exact replay writes zero rows; same request/different SHA and incomplete replay fail with zero writes. |
| R05 — correction intake and RG fail-closed gate | Done | Corrections are separate `publisher_profile_candidate_correction_item` rows with `unreviewed`, `pending_live_revalidation`, `not_granted`, `candidate_mutation_allowed=false` and `regeneration_allowed=false`. The exact `reg-szfo` RG article requires the local-edition/local-correspondent reason codes and queues `re_adjudicate_externality`; the importer only snapshots the current intake and never mutates it. |
| R07 — monotonic future publisher evidence merge | Done | Normal external research, including a known-candidate replay, creates/merges `publisher_profile_item` evidence by fingerprint. A seed cannot replace a richer full dossier; scope/locality contradiction aborts atomically; profile-only enrichment does not reopen, promote, or publish an intake candidate. |

## Durable projection contract

- Profile kind/PK: `publisher_profile_item:rtpublisher_<sha24(web:<domain>)>`.
- Identity mapping: `input_canonical_source_key=domain:<domain>` and `canonical_source_key=web:<domain>`.
- Reusable trusted fields: `profile_status`, `usable_without_profile_llm`, `profile_hash`, `evidence_fingerprint`, `profile_dimensions`, `copy_projection`, `public_copy_eligibility`, `scope`, `entity_type`.
- A profile is immediately reusable by the Writer only when `ready + external + allowed`; every row still has `publication_permission=not_granted`.
- Correction kind: `publisher_profile_candidate_correction_item`; no candidate or publication row is written.
- Atomic audit rows: `publisher_profile_import_batch` and `publisher_profile_import_receipt_item`.

## TDD and validation evidence

Failing-first P0 run:

```text
/home/dev/.venvs/events-bot-region-talk/bin/pytest -q tests/test_region_talk_publisher_profile_import.py
6 failed (new importer did not yet exist)
```

Passing checks:

```text
/home/dev/.venvs/events-bot-region-talk/bin/pytest -q \
  tests/test_region_talk_publisher_profile_import.py \
  tests/test_region_talk_external_publication_import.py \
  tests/test_region_talk_publisher_profile_packages.py
43 passed

/home/dev/.venvs/events-bot-region-talk/bin/pytest -q tests/test_region_talk*.py
781 passed in 35.24s

python3 -m py_compile \
  scripts/region_talk_publisher_profile.py \
  scripts/region_talk_publisher_profile_import.py \
  scripts/region_talk_external_publication_import.py

git diff --check
```

The workflow YAML parsed successfully with PyYAML and every embedded shell
step passed `bash -n`.

All three supplied sidecars passed Draft 2020-12 JSON Schema validation and
exact-byte importer dry-runs:

| Runtime key | Exact input SHA-256 | Dry status |
|---|---|---|
| `web:archi.ru` | `f8440fd7d6430386624936c3181bac11936e64da0d26f7641b7c763f3c906666` | `validated`, 1 profile, 1 correction, effect none |
| `web:peasantstudies.ru` | `0d61c1eac7799e70e677a23eb61537bf8c725aebbd1e8fd035548fde28e37433` | `validated`, 1 profile, 1 correction, effect none |
| `web:rg.ru` | `2bae5d314ec2388b6a5033ef233e04dce4cf29e471e61237585157ff05918f1e` | `validated`, 1 mixed/needs-review profile, 1 fail-closed correction, effect none |

Dry reports and pytest logs are under
`artifacts/codex/region-talk-source-profile-recovery/publisher/` and are not
committed.

## Guarded workflow

`.github/workflows/region-talk-publisher-profile-import.yml`:

1. checks out and verifies exact trusted `main`;
2. accepts only the dedicated publisher sidecar filename mask;
3. validates every selected exact byte stream before OIDC exchange;
4. obtains a short-lived Yandex IAM token via GitHub OIDC/WIF;
5. imports selected files sequentially;
6. fails on conflict or incomplete strong reads;
7. uploads seven-day sanitized receipts;
8. never calls a finalizer, notifier, registry publisher, or autopublish path.

## Risks / follow-up boundary

- No live YDB import was run from this implementation worktree. Live execution
  must use the protected trusted-main workflow after integration; this lane has
  no production effect.
- Serializable behavior is covered with a deterministic fake YDB transaction.
  The protected workflow is still the required integration proof for actual
  YDB IAM/OIDC configuration and row readback.
- Canonical feature documentation and `CHANGELOG.md` were explicitly outside
  this lane's writable scope and must be updated by the integration/docs lane.

## Changed files

- `.github/workflows/region-talk-publisher-profile-import.yml`
- `scripts/region_talk_external_publication_import.py`
- `scripts/region_talk_publisher_profile.py`
- `scripts/region_talk_publisher_profile_import.py`
- `tests/test_region_talk_external_publication_import.py`
- `tests/test_region_talk_publisher_profile_import.py`
- `.codex/lanes/region-talk-source-profile-recovery-20260802/publisher/RESULTS.md`
