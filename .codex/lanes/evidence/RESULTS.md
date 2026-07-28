# Evidence lane results

- Lane ID: `evidence`
- Requirement IDs: `R06`, `R09`
- Base SHA: `981aebd9d9179b3985e5fc10055ea96251997ec3`
- Head SHA (tested implementation before this lane-report-only commit):
  `ffa2daa4fffba2edfb14568fe1fa1a42738d7354`
- Branch: `agent/autopresenter-m0/evidence`
- Status: complete

## Delivered

- Draft 2020-12 JSON schemas for candidate, per-run, target-system, and
  aggregated M0 report evidence.
- Strict standard-library Node validator and evidence-directory aggregator.
- `M0-REPORT.json` and `M0-REPORT.md` atomic writers plus CLI.
- Fail-closed gate requiring, per candidate:
  - exactly runs `1..20` for local cold compatibility;
  - exactly `10` fresh-profile and `10` persistent-profile cold cycles;
  - fresh Node and browser processes on every cycle;
  - separate live-site runs `1..5`, including route and content success;
  - a passing offline self-test;
  - no administrator, install, browser download, system-browser, extra
    target dependency, or orphan-process evidence.
- Winner selection:
  - `19/20` is `FAIL`;
  - zero passing candidates returns
    `PLAYWRIGHT_ON_TARGET_WIN10_NO_GO`;
  - one passing candidate wins;
  - among two passing candidates, fewer stability signals wins;
  - the newer Playwright version wins only at equal stability, with candidate
    PASS already requiring no extra requirements.
- Required inventory covers `SYSTEM-INFO.json`, `VERSIONS.json`,
  `RELEASE-MANIFEST.json`, `SHA256SUMS.txt`, candidate records, per-run
  records, and generated `M0-REPORT.json` / `M0-REPORT.md`.

## Evidence and commands

```text
node --version
# v22.22.3

node --check tools/autopresenter/m0/reporting/validate.js
node --check tools/autopresenter/m0/reporting/aggregate.js
node --check tools/autopresenter/m0/reporting/cli.js

node --test tools/autopresenter/m0/tests/*.test.js
# tests 10
# pass 10
# fail 0

node -e 'for (const f of require("node:fs").readdirSync("tools/autopresenter/m0/schemas")) JSON.parse(require("node:fs").readFileSync("tools/autopresenter/m0/schemas/"+f)); console.log("schemas-json-ok")'
# schemas-json-ok

git diff --cached --check
# clean
```

Tests use only committed local JSON fixtures and temporary directories. No
external network operation is performed.

## Risks and integration notes

- This lane does not contain target Windows 10 empirical evidence and cannot
  claim M0 PASS. The aggregator's report explicitly marks evidence as
  target-machine-only.
- Runtime integration must emit the flat per-run fields in
  `run-record.schema.json`; suite-level runtime wrappers may be retained as
  separate artifacts but are not accepted as substitutes for per-run records.
- Candidate self-test evidence is nested in the candidate record so the gate
  cannot accidentally count a live-site run as an offline self-test.
- Packaging manifest contents remain packaging-lane-owned; this lane verifies
  their required presence while candidate evidence supplies exact stack and
  release hashes.

## Changed files

- `tools/autopresenter/m0/schemas/candidate-evidence.schema.json`
- `tools/autopresenter/m0/schemas/run-record.schema.json`
- `tools/autopresenter/m0/schemas/system-info.schema.json`
- `tools/autopresenter/m0/schemas/m0-report.schema.json`
- `tools/autopresenter/m0/reporting/aggregate.js`
- `tools/autopresenter/m0/reporting/validate.js`
- `tools/autopresenter/m0/reporting/cli.js`
- `tools/autopresenter/m0/reporting/index.js`
- `tools/autopresenter/m0/tests/reporting.test.js`
- `tools/autopresenter/m0/tests/fixtures/candidate-base.json`
- `tools/autopresenter/m0/tests/fixtures/run-local.json`
- `tools/autopresenter/m0/tests/fixtures/run-live.json`
- `tools/autopresenter/m0/tests/fixtures/system-info.json`
- `.codex/lanes/evidence/RESULTS.md`
