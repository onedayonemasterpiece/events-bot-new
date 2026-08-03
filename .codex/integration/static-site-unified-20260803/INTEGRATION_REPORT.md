# Static Site Unified Integration Report

## Base

- Base: `origin/main@0bc8482dcda5cf16a89f312f9791ecbb6d0e9a3a`
- Integration branch: `integration/static-site-unified-20260803`
- Dirty root checkout preserved: `12ad425e98a93ce3c9c33ffbc4631ae232145b5e`

## Baseline verification

- `npm run build:preview`: PASS
- `npm run check:preview`: PASS
- resilient client tests: PASS (24 + 23)
- transport fault tests: PASS (2 + 2)
- static collection helper tests: PASS (5)
- focus product tests: PASS
- external OTP harness unit tests: PASS
- real OTP/mail sends: 0
- browser release gate: FAIL on preview fixture: `generated release has no static multi-image recommendation journey`; production CI evidence additionally reaches card 6407 geometry failure.

## Lanes

| Lane | Requirements | Branch | Status | Head | Merge | Evidence |
|---|---|---|---|---|---|---|
| L0 | R0a | `agent/static-unified/l0-builder-incident` | running | — | — | — |
| L1 | RYDB | `agent/static-unified/l1-ydb-compaction` | running | — | — | — |
| L2 | R5,R6,R7,R8 | `agent/static-unified/l2-auth-transport-focus` | running | — | — | — |
| L3 | R3 | `agent/static-unified/l3-p13n00` | running | — | — | — |
| L4 | R0b,R1 | `agent/static-unified/l4-page-runtime` | waiting | — | — | depends L2/L3 |
| L5 | R2 | `agent/static-unified/l5-collections` | running | — | — | — |
| L6 | R4 | `agent/static-unified/l6-weather` | running | — | — | — |

## Closure matrix

| ID | Status | Evidence | Missing/Risk |
|---|---|---|---|
| RYDB | Partial | lane running | canary gated |
| R0a | Partial | incident lane running | terminal candidate required |
| R0b | Missing | waits L2/L3 | — |
| R1 | Missing | waits L4 | — |
| R2 | Partial | lane running | — |
| R3 | Partial | lane running | — |
| R4 | Partial | lane running | 7-day public canary remains gated |
| R5 | Partial | lane running | — |
| R6 | Partial | lane running | seed launch gates remain separate |
| R7 | Partial | lane running | — |
| R8 | Partial | lane running | — |
| R9 | Superseded | empty user item | none |
