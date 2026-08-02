# Static collections PR A integration report

| Lane | Requirement IDs | Branch | Status | Head SHA | Merge/cherry-pick | Evidence |
|---|---|---|---|---|---|---|
| map-quality-pr | R01,R05 | read-only | accepted | n/a | memo applied | intermediate review mode |
| map-ontology-code | R02 | read-only | accepted | n/a | memo applied selectively | policy v2, runtime deferred |
| map-review-data | R01,R03,R04 | read-only | accepted | n/a | memo + live source probes | seed/receipts/families |
| static-collections-pr-a | R01-R05 | agent/static-collections-quality/pr-a-ontology | integrated | `e2073976ef1d0f435ac3716a257e7bb4ab2e13d3` | serial integration | local review contract PASS |

No writable worker changes were merged from another worktree. The integrator
performed the single coupled write lane after the three independent maps.
