# W2 — production audit workflow

## Implemented

- Added `.github/workflows/smart-update-prod-audit.yml` as a manual-only,
  protected-environment workflow with `contents: read` and non-cancelling global
  concurrency.
- Added fail-closed `main`/exact-SHA/input/app validation and exact-SHA checkout.
- Pinned checkout, flyctl setup, flyctl CLI, and artifact upload versions.
- Kept the Fly token scoped to the single SSH step; public `/healthz` runs first
  without credentials.
- Sends the reviewed audit program and validated base64-JSON arguments over stdin
  to a fixed in-memory Python command. It does not deploy, restart, or create a
  remote file.
- Accepts only one sentinel envelope and exactly the nine requested evidence
  files. The runner validates the envelope, inventory, UTF-8/JSON, redaction
  patterns, observer-access gates, deployed SHA, and per-file SHA-256 values.
- Generates a sanitized exact-inventory fallback bundle when observer access is
  unavailable, uploads evidence before the terminal gate, and makes `FAIL` and
  `BLOCKED_OBSERVER_ACCESS` non-successful outcomes while allowing `PASS`/`WATCH`.
- Limits the Actions summary to classification, tested SHA, restricted-evidence
  policy, and the upload action's SHA-256 digest.

## Evidence contract for integration

The audit script must print exactly one line beginning with
`SMART_UPDATE_AUDIT_BUNDLE_V1:` followed by base64-encoded JSON with exact
top-level keys `classification`, `exit_code`, and `files`. `files` must contain
exactly the nine requested filenames. `manifest.json.artifact_sha256` must map
the eight non-manifest filenames to their lowercase SHA-256 values (the manifest
is excluded to avoid a self-referential digest). `qa-summary.json` must include
boolean `observer_access` keys `runtime_logs`, `database`, `limiter_ledger`, and
`exact_deployed_sha`; `redaction-audit.json.passed` must be true.

## Validation

- `actionlint 1.7.12`: clean.
- Local execution of the evidence extraction/fallback block: exact nine-file
  inventory, blocked classification, and eight non-self manifest hashes passed.
