# INC-2026-07-31-region-talk-external-commerciality-regex External article caveat was treated as advertising

Status: mitigated
Severity: sev2
Service: Region Talk external article discovery/publication funnel
Opened: 2026-07-31
Closed: —
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
- keep Gemini as the final semantic ad/editorial gate.

## Follow-up Actions

- [ ] Region Talk owner: verify the affected article after the next production
  CandidateReport pass and record its controlled Gemini result after RPD reset.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

The regression controls cover the attested external article, an equivalent
social post, and an untrusted sales-classified external row.
