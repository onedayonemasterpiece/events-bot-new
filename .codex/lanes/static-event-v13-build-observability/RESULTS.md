# static-event-v13-build-observability — R09 results

## Lane contract

- Lane: `static-event-v13-build-observability`
- Requirement: `R09`
- Base SHA: `fb5a35ddec00157692d75e1610c8fd43f46b4c1e`
- Head SHA (implementation): `29426b5eb5560832492b5879d97a6910f95c8820`
- Branch: `agent/static-event-v13/build-observability`
- Production DB, Object Storage and stable-channel mutations: none

## Outcome

R09 is implemented as a standalone, read-only diagnostic surface:

- `static_site_diagnostics.py` opens SQLite with `mode=ro` and
  `PRAGMA query_only=ON`; it never calls the release schema initializer or any
  publisher/mutator.
- The default 24-hour summary reports distinct requests, all recorded history
  outcomes and explicit `success` / `failed` / `noop` totals.
- Per-run evidence merges generated `event_count`, `event_page_count`,
  `page_count`, `file_count`, `object_count` and `bytes` only when those values
  are explicitly present in history/state/Kaggle progress or supplied
  manifest/bucket evidence. Missing evidence remains zero with a separate
  evidence-build count rather than being guessed.
- Current secret-review state reports safe build/run/repo/snapshot/hash/count
  metadata and a literal `/_review/<redacted>/` location. It never returns the
  persisted `public_url`, candidate token, claim token, credential, ledger
  error or arbitrary evidence JSON.
- Optional release manifests and JSON bucket object listings add current
  secret-prefix, stable-release, object and byte inventories. Secret prefixes
  are represented only by their SHA-256 token hashes.
- Stable `current.json` is reported only when the supplied inventory proves
  that it exists or supplies a bounded safe pointer body. The tool labels the
  stable lane `diagnostics_only_no_activation`; it contains no upload,
  promotion, pointer write or production activation path, preserving R10 as
  design-only.
- Consistency checks surface history-without-ledger, ledger-without-history,
  terminal-status disagreement, claimed runs without terminal history, active
  state without ledger, malformed bounded JSON, missing current secret prefix,
  current-prefix object-count disagreement and unreferenced secret prefixes.
- `scripts/static_site_build_diagnostics.py` provides bounded text/JSON output,
  repeatable `--manifest` and `--bucket-inventory` inputs, configurable lookback
  and detail limits, and a credential-safe generic failure response.
- Final recursive redaction removes URLs, bearer headers, token-valued fields,
  credential-valued fields and `_review/<token>` path segments as a second
  defense after allow-listed report construction.

No model or schema drift change was required: diagnostics use additive column
inspection and remain compatible with databases that predate one or all three
source tables.

## Evidence and commands

- `python3 -m py_compile static_site_diagnostics.py scripts/static_site_build_diagnostics.py tests/test_static_site_diagnostics.py` — passed.
- `/home/dev/.venvs/events-bot-image-geometry/bin/python -m pytest -q tests/test_static_site_diagnostics.py` — `5 passed`.
- `/home/dev/.venvs/events-bot-image-geometry/bin/python -m pytest -q tests/test_static_site_release.py tests/test_static_site_build_handoff.py` — `28 passed`.
- `python3 scripts/static_site_build_diagnostics.py --db db.sqlite --format text` — passed against the repository fixture DB; missing source tables were reported without creation.
- `python3 scripts/static_site_build_diagnostics.py --db db.sqlite --format json` — passed with the bounded v1 report.
- `git diff --check` — passed.

Environment notes:

- The bare `python` command is unavailable in this shell, so validation used
  `python3`.
- System `python3` does not include pytest; focused tests used the existing
  project-compatible `/home/dev/.venvs/events-bot-image-geometry` environment.

Regression coverage proves:

- last-24h request/outcome and all six generated-count families;
- safe current secret pointer and observational stable pointer inventory;
- no bearer URL/token leakage through JSON or text output;
- history/ledger/state/bucket orphan and mismatch reporting;
- byte-identical SQLite content when source tables are absent;
- CLI JSON redaction and nested unexpected credential redaction.

## Changed files

- `static_site_diagnostics.py`
- `scripts/static_site_build_diagnostics.py`
- `tests/test_static_site_diagnostics.py`
- `.codex/lanes/static-event-v13-build-observability/RESULTS.md`

## Risks and handoff notes

- Generated totals are evidence-backed, not inferred. Existing successful
  history rows generally contain event/object counts but may need an optional
  manifest or complete bucket listing for event-page/page/file/byte counts.
- A supplied bucket inventory is treated as a complete point-in-time listing
  for current-prefix presence and object-count comparison. Partial listings can
  therefore produce an intentional fail-closed mismatch warning.
- SHA-256 token hashes are retained because they are required to correlate the
  durable pointer with a redacted bucket prefix; raw bearer material is never
  returned.
- This lane performed no live Kaggle call, production database access, bucket
  operation, deploy, push or stable-channel lifecycle change.
- Canonical documentation and `CHANGELOG.md` were intentionally left to the
  integrator per lane ownership.
