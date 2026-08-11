# P0 final code remediation — execution matrix

Reviewed base: `5291c42897db8a157f0f9699bc678be42f96a331` (local/remote PR head equal; intake delta: none).

| ID | Requirement | Area / likely files | Dependency | Conflict risk | Primary lane | Done when |
|---|---|---|---|---|---|---|
| C1–C8 | Remove replay-fingerprint terminal success; drift/no-progress retry/rebase; reopen legacy replay rows; tests A–H | `vk_intake.py`, `db.py`, `models.py`, continuation tests | current continuation state machine | high (`vk_intake.py`) | IMPL-CONTINUATION | repeated page cannot produce done; old rows recover; all boundaries proven |
| V1–V11 | Pure seven-reason contradiction collector; VK/TG/direct wiring; one conditional verifier; children retained; tests A–H | `source_parse_contract.py`, `main.py`, `vk_intake.py`, TG producer/consumer, tests | continuation merged; typed decision contract | very high | IMPL-VERIFY-NOEVENT | all seven reasons have production producers and facade E2E tests |
| N1–N9 | Require closed no-event reason in provider/compatibility/receipts/prompts/metrics/static gate | same contract/prompt/caller files | shares verifier invariants | very high | IMPL-VERIFY-NOEVENT | reasonless/unknown no-event always retries, including legacy receipts |
| R1–R8 | Lossless-enough VK outer+nested envelope; shared semantic evidence builder; durable fallback; revision/replay tests A–G | `main.py`, `vk_intake.py`, `db.py`, `models.py`, VK tests | verifier/no-event merged | high | IMPL-RAW-PACKET | durable packet owns outer+nested text and attachment metadata before LLM |
| E1–E10 | Exact SHA, blocker table, state machine, seven-reason matrix, enum/adapter matrix, schema examples, tests/CI/static inventory | integration report, CI | all code lanes | medium | DOCS-EVIDENCE | committed report and exact local/CI receipts |
| E11–E14 | Strict RO A–T census/dry-run; incident update; external blockers; no deploy-ready claim | incident, ignored artifacts, PR comment | final source frozen | high operational | DOCS-EVIDENCE | mode=ro/query_only, hashes unchanged, counts use carriers, changes=0 |
| G1–G8 | Final acceptance criteria and no-scope-expansion review | full diff | all lanes | high | FINAL-REVIEW | every item Done/Blocked with evidence; PR stays Draft |
