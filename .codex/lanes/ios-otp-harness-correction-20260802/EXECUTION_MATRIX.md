# iOS OTP harness correction — execution matrix

| ID | Requirement | Area | Likely files | Dependencies | Conflict risk | Lane | Parallelizable? | Done when |
|---|---|---|---|---|---|---|---|---|
| R01 | Reclassify run 30754894934 as `BLOCKED_SAFARI_FIRST_RUN_UI` and replace one-shot dismissal with a bounded allowlisted Safari system-UI state machine. | iOS adapter/evidence | `appium-ui.mjs`, `run.mjs`, tests | none | high with R03/R05 | ios_harness_map → integrator | discovery only | modal is detected/dismissed/verified or BLOCKED before side effects |
| R02 | Let Appium boot the exact pre-created shutdown Simulator and remove external boot/defaults/open initialization. | workflow/Appium | `external-focus-email-otp.yml`, adapter config | R01 | high | ios_harness_map → integrator | discovery only | workflow creates shutdown UDID; Appium owns launch and keyboard capabilities |
| R03 | Add side-effect-free iOS email/numeric keyboard preflight and classify control/product outcomes. | harness/scenario | adapter, new preflight runner/helpers, workflow, tests | R01,R02 | high | ios_harness_map → integrator | discovery only | three protected preflight runs can execute without mailbox/Auth writes |
| R04 | Remove raw Appium log output; mask recipient and OTP before WebDriver sees them. | security | workflow, mailbox boundary, adapters, run | R03 | high | ios_harness_map → integrator | discovery only | no raw tail; masks emitted at recipient/OTP boundaries; Appium implements secret hook |
| R05 | Enrich safe iOS evidence: modal, activation attempts, viewport baseline/focused geometry, preflight, simulator ownership/capabilities. | evidence | adapter, evidence schema/tests | R01-R04 | high | ios_harness_map → integrator | discovery only | structured sanitized fields/screenshots exist and redaction passes |
| R06 | Publish actual `qa-summary.json` values in issue terminal receipt rather than only workflow conclusion. | control plane | workflows, terminal summarizer/tests | R05,R07,R08 | medium | control_evidence_map → integrator | discovery only | receipt includes status/domain/SHA/counts/keyboards/warnings/redaction/artifact |
| R07 | Record harness/tested/observed SHAs and workflow/runner/Appium/WDA provenance. | reproducibility | run/evidence/workflows/tests | R05 | high | control_evidence_map → integrator | discovery only | manifest and summary distinguish harness from target and include runner toolchain |
| R08 | Split expected cancelled probes and unexpected failures; keep stable telemetry 403 as explicit warning or make it blocking by policy. | reporting | recorders/result schema/tests/docs | R05 | high | control_evidence_map → integrator | discovery only | PASS contains warnings and separate expected/unexpected counters |
| R09 | Correct registry, incident, focus/release docs and reusable skills; remove brittle implementation strings from contract tests. | docs/skills/tests | canonical docs, `.codex/skills/*`, tests | R01-R08 | medium | docs_skill_map → integrator | discovery only | current iOS state is BLOCKED and docs describe state-machine contract |
| R10 | Acceptance: three side-effect-free iOS preflights, then one full issue #253 iOS OTP; only PASS may close registry status. | live E2E/release | GitHub issue/workflow/docs | R01-R09 merged to main | external | integrator | no | three preflights terminal, then one live OTP with full evidence |

## Current closure audit (pre-live)

| ID | Status | Evidence | Remaining |
|---|---|---|---|
| R01 | Done | exact Safari title/action state machine, behavioral tests, corrected docs | live observation |
| R02 | Done | workflow creates exact shutdown UDID; no external boot/open/defaults | live macOS run |
| R03 | Done | iOS-only zero-side-effect preflight scenario and journey tests | three terminal passes |
| R04 | Done | immediate GitHub masks/adapter secret hook; raw tail removed and log cleanup trap | artifact audit |
| R05 | Done | empty-keyboard screenshots, viewport/attempt/modal/capability evidence | live artifact audit |
| R06 | Done | terminal formatter consumes downloaded `qa-summary.json` | live issue receipt |
| R07 | Done | manifest/summary SHA triple and toolchain/Appium/WDA provenance | live populated values |
| R08 | Done | correlated health cancellation plus explicit telemetry-403 warning | live diagnostic evidence |
| R09 | Done | registry, incident, feature/release/runbook and reusable skill corrected | checklist review |
| R10 | Pending live | control issue `#253` is ready | merge, 3 preflights, 1 full iOS OTP |
