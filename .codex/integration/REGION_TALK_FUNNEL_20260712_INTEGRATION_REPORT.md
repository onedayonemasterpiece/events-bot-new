# Region Talk funnel integration report — 2026-07-12

| Lane | Requirements | Status | Evidence |
|---|---|---|---|
| L1 | R01, R02, R05, R06 | merged | strict-gate audit, operator feedback, video/product metrics |
| L2 | R03, R04, R07, R08 | merged | URL PK, text lifecycle, YDB dump/drop/maintenance |
| L3 | R09, R10, R11 | merged | data-driven direct-KO prioritization |
| L4 | R12 | in progress | tests passed; controlled live run pending |

All writes were integrated serially because CandidateReport, finalizer and the
orchestrator share one state contract and parallel edits would have conflicted.
