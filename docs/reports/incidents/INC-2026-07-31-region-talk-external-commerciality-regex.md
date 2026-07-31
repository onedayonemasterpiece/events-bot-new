# INC-2026-07-31-region-talk-external-commerciality-regex External article caveat was treated as advertising

Status: closed
Severity: sev2
Service: Region Talk external article discovery/publication funnel
Opened: 2026-07-31
Closed: 2026-07-31
Owners: events-bot / Region Talk
Related incidents: `INC-2026-07-31-region-talk-article-link-precedence`
Related docs: `docs/features/region-talk-channel/external-publications.md`

## Summary

A strict imported noncommercial editorial article was tombstoned because the
generic social-post regex matched the phrase `выбор гостиниц` inside an
editorial caveat. This contradicted the page-level research/import attestation
and removed a valid article from the scarce daily article lane before Gemini.

## User / Business Impact

- one high-quality nonlocal architecture article was excluded before final
  semantic verification;
- the article lane had only one accepted candidate, so the false negative had
  direct product impact on future daily coverage.

## Detection

A production YDB funnel audit after the article-lane recovery found the row as
`eligibility_reject_tombstone`; its evidence named `post_ad_or_promo`. A direct
CPU-only replay of the exact imported editorial text localized the hit to
`paid_tour_service`, while the imported policy was
`institutional_noncommercial` with no hard exclusions.

## Timeline

- 2026-07-31 19:02 UTC — 29-row intake funnel audited; 15 rows were strict
  ready, nine had fused vectors and one false advertising tombstone appeared.
- 2026-07-31 19:05 UTC — exact evidence localized to a generic social regex hit
  inside the research-authored caveat.
- 2026-07-31 19:08 UTC — bounded LLM-first policy-attestation fix and negative
  controls implemented.
- 2026-07-31 19:36 UTC — no-LLM production reconciliation proved the article
  eligible but exposed a stale eligibility-only tombstone that the monotonic
  provider merge would not reopen.
- 2026-07-31 19:44 UTC — the bounded reopen fix was live and remotely probed;
  a controlled finalizer moved the row from the advertising tombstone into the
  normal text-restore/downstream path without a provider bypass.
- 2026-07-31 19:56 UTC — CandidateReport restored the article state. The row
  no longer carries the false advertising decision and now waits at the
  independent current visual-review gate.
- 2026-07-31 20:34 UTC — the daily anti-vector plan was rebuilt successfully;
  one accepted article and nineteen accepted social candidates were eligible.

## Root Cause

1. The strict importer had already required `non_news`, noncommercial policy,
   product match and zero hard exclusions.
2. CandidateReport projected that evidence but later let a generic social regex
   override it before the final Gemini verifier.
3. The policy attestation was not preserved through candidate/image/publication
   compact state.

## Contributing Factors

- the social ad regex is intentionally recall-oriented and cannot distinguish a
  caveat about hotels from a sales CTA;
- the external-publication adapter had a scope attestation but no symmetric
  commerciality attestation.

## Automation Contract

### Treat as regression guard when

- changing external-publication import projection, ad/promo routing, vector
  fusion refresh or publication eligibility.

### Affected surfaces

- `kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py`;
- external-publication candidate/image/publication YDB projections;
- daily article supply.

### Mandatory checks before closure or deploy

- an imported ready external article with `independent` or
  `institutional_noncommercial` policy retains regex hits as evidence but is
  not rejected by the generic social ad gate;
- a social post with the same regex hit remains rejected;
- a sponsored/sales/unknown external row does not receive the attestation;
- hard exclusions, non-ready decisions and final Gemini ad review remain
  fail-closed;
- the affected production article leaves the advertising tombstone after a
  current CandidateReport pass, without any raw-key/provider bypass.

### Required evidence

- focused regression tests and finalizer suite;
- deployed SHA reachable from `origin/main`, Fly health and remote code probe;
- post-deploy YDB row/funnel counts and publication-plan refresh.

## Immediate Mitigation

The generic regex remains recorded, but a strict page-level research/import
noncommercial attestation now owns pre-Gemini routing for external web articles.

## Corrective Actions

- preserve research policy/decision evidence in compact state;
- version only the external-publication processing fingerprint, avoiding a
  costly rescan of unrelated social posts;
- reopen only changed deterministic eligibility tombstones that have no
  provider/operator verdict; all paid or human terminal decisions stay
  monotonic;
- keep Gemini as the final semantic ad/editorial gate.

## Follow-up Actions

- [x] Region Talk owner: verify the affected article after a current production
  CandidateReport pass; it left the advertising tombstone and reached the
  independent visual-review gate.
- [ ] Region Talk owner: record the article's controlled Gemini result after
  its current visual review is resolved and provider RPD is available. This is
  downstream product work and does not reopen the commerciality incident.

## Release And Closure Evidence

- deployed SHAs: `877e4183` (strict imported noncommercial attestation) and
  `6438a98f` (eligibility-only tombstone reopen), both reachable from
  `origin/main`; the remote `v1815`/`v1816` code probes contained both guards.
- deploy path: clean Fly remote builds from commits already reachable from
  `origin/main`; `/healthz` returned `ok=true`, `ready=true`, Region Talk
  scheduler `ok`.
- regression checks: candidate-policy attestation focused test passed; full
  publication-finalizer suite passed with 43 tests, including social,
  sales/sponsored, operator-reject and paid-provider negative controls.
- reconciliation evidence:
  `region-talk-external-commerciality-reconcile-20260731T1936Z-877e4183`
  read 96 inputs (72 actual-image, 16 video, eight link articles) with zero LLM
  calls; the controlled follow-up
  `region-talk-external-commerciality-reopen-20260731T1947Z-6438a98f`
  retained the shared Gemini limiter and made three normal verifier attempts.
- post-deploy verification: the exact article now has
  `publication_status=needs_visual_review`,
  `publication_candidate_status=visual_review_pending`, and no advertising
  tombstone. Candidate memory preserves `rights_policy=link_only`,
  `media_reuse_allowed=false`, the strict research-policy attestation and the
  generic regex evidence. Anti-vector snapshot
  `rtdayplan_63afa25453b33ab35ce6d475` produced one planned article and fourteen
  planned social slots over fourteen days.

## Prevention

The regression controls cover the attested external article, an equivalent
social post, and an untrusted sales-classified external row.
