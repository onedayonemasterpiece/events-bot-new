# W3 results — Smart Update production audit documentation

Status: complete

Owned changes:

- `docs/operations/smart-update-prod-audit.md` — canonical protected read-only
  production audit runbook, safety/threat model, observer/metric/evidence
  contract, classifications, known limitations, and one-time token/dispatch/
  artifact/revocation procedure.
- `docs/routes.yml` — operations route.
- `CHANGELOG.md` — concise `[Unreleased]` entry.

Validation:

- Required nine evidence filenames are enumerated exactly.
- Mandatory `BLOCKED_OBSERVER_ACCESS` sources, half-open UTC window, SQLite
  `mode=ro` transaction, limiter SELECT-only boundary, product sampling and
  redaction policy are explicit.
- Post-merge procedure uses a 24-hour app SSH token streamed directly to the
  protected GitHub Environment secret, exact remote `main` SHA, terminal watch,
  GitHub artifact digest, trap-based revocation/deletion and post-cleanup checks.
- Known 2026-08-04 limitations cover the absent limiter operation column,
  runtime correlation gaps and indeterminate exact warm replay.
