# Lane Map — full future-event incident audit v2 (2026-07-10)

## Requirements

| ID | Requirement | Primary lane | Dependencies | Validation |
|---|---|---|---|---|
| R01 | Freeze an exact-cutoff inventory of every active canonical future event | `future-duplicate-location-audit` | production snapshot | Row-count and cutoff evidence |
| R02 | Scan every historical incident record and execute relevant event-quality regression contracts | `incident-regression-map` | incident corpus | File-count, taxonomy, check results |
| R03 | Vector-first duplicate and near-duplicate audit, with human/source adjudication | `future-duplicate-location-audit` | R01 | Candidate and adjudication ledger |
| R04 | Authenticated VK comments audit for managed posts tied to future events | `vk-comments-public-feedback` | R01/post mapping | Comment/post/event evidence |
| R05 | Validate that every location is location-like and source-correct | `future-duplicate-location-audit` | R01 | Full-scan metrics and reviewed exceptions |
| R06 | Establish source/public/log-backed root causes for confirmed defects | `integrator` | R02–R05 | Evidence chain per defect |
| R07 | Implement prevention in LLM-first and vector-first form, with tests/docs/changelog | `integrator` | R06 | Focused and regression tests |
| R08 | Repair all canonical/public surfaces, release safely, and verify/catch up production | `integrator` | R06–R07 | DB/API/release evidence |

## Lanes

1. `incident-regression-map` — read-only; owns R02; scans the complete incident corpus and maps regression contracts.
2. `future-duplicate-location-audit` — read-only; owns R01/R03/R05; production snapshot, vector-first similarity, location/source checks.
3. `vk-comments-public-feedback` — read-only; owns R04; authenticated VK API inspection only, no mutations.
4. `integrator` — writable integration branch; owns R06/R07/R08 after discovery gates.

## Integration order

R01/R02/R03/R04/R05 discovery → source-grounded adjudication → R06 root causes → R07 implementation → R08 repair/release/verification.

## Closure status

| Requirement | Status | Evidence |
|---|---|---|
| R01 | Done | final exact-cutoff export: 301 active canonical strict-future rows at 2026-07-10T23:10:02Z |
| R02 | Done | all 174 incident records / 23,831 lines mapped into 14 regression families |
| R03 | Done | exact-hash 301/301 vectors; 1,488 pairs; two same-date >=0.90 controls adjudicated as distinct |
| R04 | Done | 2,762 managed VK posts, 301 comments/replies, all comment-bearing posts screened |
| R05 | Done | 301/301 source/OCR coverage; zero missing/prose locations; confirmed exceptions repaired |
| R06 | Done | source/OCR/comment/public evidence chain recorded in canonical incident |
| R07 | Done | LLM-first location grounding plus exact-hash vector runner and regressions delivered |
| R08 | Partial | canonical/TG/VK/Telegraph/ICS repaired and released; legacy Calendar documents require an unavailable channel-admin role |
