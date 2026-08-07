# Search Live Automation Integration Report

Base: `origin/main` at `ec09c011674eecddf9e9b8e154e3d102f9384b12`.

| Lane | Requirement IDs | Branch | Status | Head SHA | Merge/cherry-pick | Evidence |
|---|---|---|---|---|---|---|
| L1-auth-broker | R02 | agent/search-live-automation/auth-broker | merged | `fde00e4e0` | `d9c854070`,`d5564407f` | 12 Node + 34 Python PASS; RESULTS committed |
| L2-server-contract | R05 | agent/search-live-automation/server-contract | merged | `152ca0ca4` | `49dfd3103`,`4799ad774` | 24 Node, Deno check, PostgreSQL contract and security assertions PASS |
| L3-search-harness | R01,R03,R06 | agent/search-live-automation/harness | merged | `c7e2bbb5f` | `ff6092063`,`5a2bb836e` | 7 worker tests; 10 integrated harness/workflow tests PASS |
| L4-corpus-revision | R09 | agent/search-live-automation/corpus-revision | merged | `3917d2c77` | `e65decf5f`,`fa9baa1c0` | 58 worker tests; deterministic 288-event dry receipt |
| L6-ui-variant | supporting R06,R09 | agent/search-live-automation/ui-variant | merged blocked-patch record | `6fe3bb33a` | `3d49437e0`; RESULTS copied by integrator | 30 worker tests, 15 integrated Search UI tests PASS |
| L5-ci-registry-ops | R04,R07,R08 | integration/search-live-automation-20260807 | implemented; live release pending | integration HEAD | integrator-owned | registry/workflow/docs/tests complete; deployment and terminal hosted runs remain |
