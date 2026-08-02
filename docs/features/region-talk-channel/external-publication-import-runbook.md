# Region Talk: guarded import of external-publication research

Status: **operator runbook for staging only**. This is the canonical release
path for the three immutable historical research results listed in
[`external-publication-research-results.md`](external-publication-research-results.md).
It operates the importer contract in
[`external-publications.md`](external-publications.md); it does not approve or
publish editorial material.

## Preconditions and trust boundary

The GitHub Actions workflow **Import trusted Region Talk external-publication
research** is intentionally manual (`workflow_dispatch`) and accepts only a
non-symlink file from its own `main` checkout whose path matches
`docs/features/region-talk-channel/region-talk-external-research-result-*.json`.
It refuses a path or commit that is not reachable from that trusted checkout.
A pull-request ref, a branch chosen by an operator, an arbitrary uploaded JSON
file, and any path outside that reviewed directory are not accepted import
inputs.

The protected environment `region-talk-ydb-import` is the approval boundary.
The job has only `contents: read` and `id-token: write`; it does not read a
Yandex static credential from GitHub Secrets. Configure these **non-secret
GitHub environment/repository variables** before the first successful staging
run:

| Variable | Purpose |
|---|---|
| `YANDEX_WIF_OIDC_AUDIENCE` | Audience accepted by the Yandex Workload Identity Federation provider. |
| `YANDEX_WIF_SERVICE_ACCOUNT_ID` | Yandex service account that receives the short-lived IAM token. |
| `REGION_TALK_YDB_ENDPOINT` | Target YDB endpoint. |
| `REGION_TALK_YDB_DATABASE` | Target YDB database path. |
| `REGION_TALK_YDB_NAMESPACE` | Region Talk YDB namespace (`region_talk_compact`). |

Yandex Workload Identity Federation must trust the GitHub OIDC audience and the
protected environment's exact GitHub subject. The job exchanges its short-lived
GitHub OIDC token only after dry validation succeeds, masks the returned IAM
token, and places it in the job environment only for the staging command. Do
not add a Yandex API key, service-account key, long-lived IAM token, or other
credential to GitHub Secrets to make this workflow run.

## Historical input status — do not dispatch the invalid files

The importer from workflow commit `a9c9d43e` was run in dry mode against the
committed bytes on 2026-08-01. All three files are schema-shaped research
records, but importer semantic validation is intentionally stricter than the
schema and returns a non-zero result if any candidate row is rejected.

| JSON / request ID | Dry result | Operator action |
|---|---|---|
| `region-talk-external-research-result-region-talk-external-2026-07-31-125052.json` | **Fails**: 15 valid and 5 rejected candidate rows. Rejections: indexes 2, 11, 16, 17 require `policy_classification.newsiness=non_news`; index 4 requires `publication.access_status=full_text`. | Do not dispatch. Produce a corrected successor with a **new** `request_id`; do not edit this historical payload. |
| `region-talk-external-research-result-region-talk-external-2026-07-31-174033.json` | **Fails**: 13 valid and 1 rejected candidate row. Index 1 requires `policy_classification.newsiness=non_news`. | Do not dispatch. Produce a corrected successor with a **new** `request_id`; do not edit this historical payload. |
| `region-talk-external-research-result-region-talk-external-2026-08-01-163142.json` | **Clean**: 20 valid, 0 rejected, 63 planned YDB rows in dry mode. | This is the only current historical input suitable for protected-environment dispatch, subject to live duplicate guard and environment approval. |

A failed validation exits before YDB authentication and before any write. The
historical bytes are deliberately preserved for audit. A corrected successor
must use the permitted filename pattern, be committed to that same directory,
documented with its SHA-256, reviewed, and merged to `main`; no workflow edit
is required for a new reviewed result file.

## Procedure

### 1. Pull request: review, do not import

1. Review the JSON bytes, schema/version, evidence, and the SHA-256 shown in
   [`external-publication-research-results.md`](external-publication-research-results.md).
2. Run local dry validation only; it does not contact YDB or request OIDC:

   ```bash
   python3 scripts/region_talk_external_publication_import.py \
     docs/features/region-talk-channel/region-talk-external-research-result-region-talk-external-2026-08-01-163142.json \
     --report artifacts/codex/region-talk-external-publications/2026-08-01-163142.dry-run.json
   ```

3. Inspect `rejected`, `conflicts`, `planned_ydb_rows`, the exact
   `input_json_sha256`, and `executed: false` in the report.
   A non-empty `rejected` list is a fail-closed result, not permission to stage
   only its valid subset. Correct the research in a successor instead.
4. Do not invoke the Actions workflow from a PR: its trusted checkout is
   `main`, not the PR, and it will not import proposed/unmerged bytes.

### 2. Main: establish the reviewed input

1. Merge the reviewed documentation and exact JSON to `main`; recheck that the
   selected path matches the permitted filename pattern and that its `sha256sum`
   matches the documented value.
2. Confirm that the protected environment and five OIDC/YDB variables above
   are present. No application secret is needed or permitted for this path.
3. Confirm the input is clean locally. At present this means selecting only the
   2026-08-01 historical file; the first two files are preserved evidence and
   must await corrected successors.

### 3. Manual protected dispatch

1. In GitHub Actions, open **Import trusted Region Talk external-publication
   research**, choose the approved clean JSON, and run it from `main`.
2. Approve the `region-talk-ydb-import` protected-environment deployment only
   after checking the chosen filename and main commit in the job summary.
3. The job hashes the exact input bytes, passes that SHA-256 into the importer,
   and dry-validates **before** OIDC exchange,
   obtains a short-lived Yandex IAM token through federation, then runs the
   same importer with `--execute`. The executing importer re-reads the live
   YDB identity ledger; canonical URL, DOI and exact normalized title+authors
   are checked together. A page that became known after research or conflicting
   identity keys are rejected rather than forced through.
4. Never replace this flow with a local `--execute` command or a static token.
   If a failure is due to OIDC/environment configuration, fix the federation or
   non-secret variables and rerun the protected workflow rather than adding a
   secret.

## Required audit evidence

For every dispatch, download and retain the Actions artifact named
`region-talk-external-publication-import-<input-sha256>` (30-day retention).
It contains:

- `input.sha256` — exact committed input identity;
- `validation.json` — dry validation result and all row rejections;
- `execute.json` — write result when execution was reached;
- `receipt.json` — deterministic presence/summary receipt for both reports.

The execute report, receipt and Actions step summary must show
`new_intake_count` plus sorted `new_intake_ids`, with replay/conflict counts
separate. This lets the operator see an asynchronous queue increase without
mistaking old IDs for new work.

Also retain the GitHub job URL, the main checkout SHA from the job summary, the
selected input path, environment approval record, and outcome. A validation
failure still has the hash and validation report; it must have no execute
report. A successful execute is not fully closed until the report confirms
`executed: true`, consistent `written_ydb_rows`, the new/replay ID sets, and
`live_duplicate_guard_applied: true`. An identical rerun is a successful
idempotent no-op: `new_intake_count=0`, no error row and no changed batch
identity. The same `request_id` with a different exact input SHA is a conflict,
so it must write nothing and require operator investigation.

## What staging does **not** do

The importer writes external intake/source/seen/batch/error ledger rows only.
It never writes `publication_candidate_item`, never marks a candidate
`llm_confirmed`, never schedules a Telegram/VK post, and never calls a public
publishing API. A staged row must still pass CandidateReport, text/vector,
image, final-verifier, and explicit operator gates before any later publication
planning. Therefore an import is an auditable intake event, **not** immediate
publication.

Every new intake row starts with `review_status=unreviewed` and
`publication_permission=not_granted`. A clean `candidate` is allowed to enter
the normal checking/scoring funnel; a `manual_review_required` row stays held.
Neither case authorizes public posting at intake time. If any input row is
invalid, conflicting, lacks required proof or live YDB cannot be read
completely, `--execute` is all-or-nothing and performs no staging writes.
