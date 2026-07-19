# Region Talk external publications integration report

| Lane | Requirement IDs | Branch | Status | Head SHA | Merge/cherry-pick | Evidence |
|---|---|---|---|---|---|---|
| L1 concept/prompt advisory | R01, R02, R08 | shared read-only | accepted | n/a | recommendations integrated serially | contract/prompt docs and Schema |
| L2 architecture advisory | R03, R04 | shared read-only | accepted | n/a | recommendations integrated serially | staging-only importer and release boundary |
| L3 chat/queue advisory | R05, R06, R07 | shared read-only | accepted | n/a | recommendations integrated serially | notifier fields and MMR queue helper |
| L4 checklist review | R01-R08 | shared read-only | accepted | n/a | two review rounds; concrete gaps fixed | final review: R01-R05, R07-R08 Done; R06 Partial |
| INT serial integrator | R01-R08 | integration/region-talk-publications-20260719 | committed | ffd794c5e1c556d0389bf90a73bf8616d3b4747a | direct implementation commit | 33 focused tests; 549 Region Talk regressions; Schema/CLI/YAML checks |

## Closure

- R01 Done — this is explicitly an extension of Region Talk, not a separate product.
- R02 Done — broad, contour-based research prompt avoids a fixed source allowlist.
- R03 Done — exact JSON Schema validation plus semantic fail-closed staging and stable dedupe.
- R04 Done within staging scope — contract targets the existing E5/BGE/image path; automatic consumer is intentionally unreleased.
- R05 Done — nested and flattened external-publication links, source overview, teaser, and numeric evaluation render in candidate messages.
- R06 Partial — a read-only queue is sent on explicit CLI request; there is no inbound chat command.
- R07 Done — compatible-vector MMR, anti-adjacency, self-match exclusion, and disclosed fallback/relaxation.
- R08 Done within contract/staging scope — public-interest/accessibility gates and evidence coverage for every non-empty editorial surface.

The implementation is safe to review as a staging/contract release. It is not evidence that external publications can yet be automatically confirmed or published.
