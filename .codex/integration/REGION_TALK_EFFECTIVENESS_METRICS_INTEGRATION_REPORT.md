# Region Talk effectiveness metrics — integration report

Date: 2026-07-15
Branch: `agent/region-talk/R04-live-canary`
Mode: parallel read-only discovery, serial implementation and verification

## Requirement closure

| Requirement | Status | Result |
|---|---|---|
| R01 — processed post -> detected KO conversion | Done | The orchestrator now exposes unique `kaliningrad_oblast_only_scope=true` yield before content/media/Gemini filters, per-1,000 yield, evaluation coverage, and yield among evaluated rows. The audited snapshot is 525 / 12,075 = 4.35%; coverage is 3,563 / 12,075 = 29.51%. |
| R02 — decide whether CandidateReport is needed | Done | The acquisition/vector/YDB worker is retained. Automatic orchestration sets `REGION_TALK_WRITE_REPORT_ARTIFACTS=0` only after durable queue/state handoff; standalone/manual runs retain the review exports by default. |
| R03 — GitHub handoff for an independent consultant | Done | The raw Gemini audit and a self-contained independent-review prompt are tracked under `docs/reports/` and linked from the feature documentation/routes. |

## Key architectural finding

The 17,424-line `CandidateReport` file is misnamed: most of it is the live worker, not the report. Removing the whole file would remove acquisition, vector gates, state merge and YDB queue persistence. The optional report tail assembles 58 sheets and writes duplicate XLSX/CSV/full-JSON/Markdown/HTML artifacts; automatic runs now skip that tail while preserving minimal `stage_status.json` and `output.json` completion contracts. A later physical split into worker and exporter remains desirable, but is not required for the product KPI fix.

## Verification

- `python3 -m py_compile` for changed Python code/tests: passed.
- Five focused metric/config/no-artifact tests: passed.
- Full `tests.test_region_talk_orchestrator`: 85 tests passed.
- `git diff --check`: passed.
- Combined CandidateReport/orchestrator suite: 344/347 passed. The three non-gating failures do not exercise the changed functions: one missing optional `openpyxl` dependency in the system interpreter and two LLM wrapper expectations returning `llm_gate_status=error`. The new no-artifact and env-propagation tests passed.

## Safety

- No production/YDB mutation was performed.
- No Telegram auth bundle was used or repurposed.
- Existing content, media and publication safety gates remain enabled.
