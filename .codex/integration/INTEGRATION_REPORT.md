# First-party action-map integration report

## Scope

- Base: `origin/main` at `d7731ab4235b325e9ca52d13c45fba83eaf5de0b`
- Integration branch: `integration/action-map-contract`
- Requirements: R01, R02, R03, R04, R06

| Lane | Requirements | Worker head | Status | Integration evidence |
|---|---|---|---|---|
| `action-map-events` | R01–R04, R06 | `6297267fb9086e198c8e5a369e2af5d36f05bc33` | merged | cherry-picked as six ordered commits; no conflicts |
| `action-map-design` | R05 | `e46eb71daf1cf8726cf540b92fef86a577e65c2f` | external-repo merged | integrated separately in `lovekgd-design-system` |

## Verification

- Exact source/canonical attachment comparison: `cmp` PASS, 61,558 bytes.
- SHA-256: `4ade21e6ad03d6e5d9bc934af17ad8bccb1463ebe595f16d8bafe75c0e88048a`.
- `docs/routes.yml` parses and all six new routes resolve.
- Added relative Markdown links resolve.
- Action-map producer enums match the canonical source and design consumer: `insufficient-data`, `instrument-better`.
- `git diff --check origin/main..HEAD`: PASS.
- No runtime, schema or deployment changes are included.
