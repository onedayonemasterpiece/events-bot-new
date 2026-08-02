# Static collections PR A integration report

| Lane | Requirement IDs | Branch | Status | Head SHA | Merge/cherry-pick | Evidence |
|---|---|---|---|---|---|---|
| map-quality-pr | R03,R05,R06,R07 | read-only | accepted | n/a | memo applied | validator/test hardening map |
| map-ontology-code | R02 | read-only | accepted | n/a | memo applied selectively | policy v2, runtime deferred |
| map-review-data | R01,R02,R04 | read-only | accepted | n/a | memo + live source probes | 4648/6871/7103 and festival scope |
| static-collections-pr-a | R01-R07 | agent/static-collections-quality/pr-a-ontology | integrated | `6dec4628c3291e9433291ed13fdf9cb912b385a0` | serial integration | review PASS 0 errors; strict expected FAIL |

No writable worker changes were merged from another worktree. The integrator
performed the single coupled write lane after the three independent maps.
The hardened diff remains PR A only: no owner gold, scores, thresholds, Astro
routes, navigation or sitemap were added, and semantic publication is blocked.
