# INC-2026-07-31-region-talk-article-link-precedence Region Talk article lane revoked an existing visual candidate

Status: closed
Severity: sev2
Service: Region Talk external article publication queue
Opened: 2026-07-31
Closed: 2026-07-31 18:57 UTC
Owners: events-bot / Region Talk
Related incidents: `INC-2026-07-31-google-ai-parallel-limiter-bypass`
Related docs: `docs/features/region-talk-channel/external-publications.md`, `docs/features/region-talk-channel/publication-queue.md`, `docs/operations/release-governance.md`

## Summary

The first production catch-up after adding the external link-only article lane
routed every external article through that lane, including an already accepted
Archi.ru row with current actual-image evidence. Its older general
`route_useful_candidate` vector type then failed the new link-only article
sub-gate and the finalizer wrote `eligibility_revoked`.

## User / Business Impact

- the only confirmed article in the daily Region Talk plan was temporarily
  removed from eligibility;
- social candidates were not admitted without media and were not affected by
  the new bypass;
- three new strict article candidates reached Gemini but remained retryable
  after the shared gateway returned the configured project RPD limit.

## Detection

The controlled production finalizer result
`region-talk-article-link-catchup-20260731T1845Z-c797f55a` reported Archi.ru as
`eligibility_revoked`; the same artifact showed all three new article calls as
`RateLimitError: rpd` and `llm_budget_deferred`.

## Timeline

- 2026-07-31 18:44 UTC — Fly `v1806` deployed link-only article support.
- 2026-07-31 18:45 UTC — bounded production finalizer catch-up started with
  both human Telegram sessions removed from its process environment.
- 2026-07-31 18:47 UTC — catch-up completed: nine link-article inputs were
  visible, three provider calls were shared-reserved, Archi.ru was revoked and
  the three new calls were RPD-limited.
- 2026-07-31 18:48 UTC — root cause localized to article-lane precedence;
  containment/fix prepared.
- 2026-07-31 18:51 UTC — `origin/main` SHA `7d1f530d` deployed as Fly
  `v1808`; health check passed and the remote image contained the corrected
  article routing predicate.
- 2026-07-31 18:53 UTC — compensating finalizer reconciliation completed with
  `--max-llm 0`: 96 inputs, 8 link-only article rows, 0 provider calls and 26
  accepted rows in the durable ledger. Archi.ru returned to `gemini_accept` /
  `sent_to_chat` while retaining `actual_scored` / `actual_image` evidence.
- 2026-07-31 18:56 UTC — the daily anti-vector plan was rebuilt and persisted:
  one eligible/planned article and 19 eligible social candidates, with 14
  social slots planned in the current horizon.

## Root Cause

1. `is_external_link_article_candidate()` described rights/origin eligibility,
   but was also used as the final routing decision.
2. The routing decision did not first check for current
   `image_model_input_type=actual_image` evidence.
3. The link-only vector subtype guard was therefore applied to a valid visual
   article that predated the external-article subtype.

## Contributing Factors

- the initial positive test covered no-media articles and the negative social
  control, but not an external article that already had actual-image evidence;
- the first live run occurred after deployment because production YDB
  credentials are not present in the local `.env`.

## Automation Contract

### Treat as regression guard when

- changing Region Talk publication eligibility, external article rights,
  CandidateReport publication routing or finalizer input assembly.

### Affected surfaces

- `kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py`;
- `scripts/region_talk_publication_finalizer.py`;
- `publication_candidate_item` eligibility state and the daily article plan;
- shared Gemini RPD/reservation behavior.

### Mandatory checks before closure or deploy

- a strict no-media external article is accepted only as
  `link_only_no_media_reuse`;
- an external article with current actual-image evidence uses
  `scored_actual_image`, including a legacy/general accepted vector subtype;
- ambiguous-vector and media-reuse article controls remain rejected;
- a Telegram/VK/social row without media remains rejected;
- production Archi.ru returns to `gemini_accept`/eligible without a new provider
  verdict;
- all new article provider attempts remain shared-reserved and an RPD failure
  stays retryable rather than falling back to a raw key.

### Required evidence

- passing candidate/finalizer regression tests;
- deployed SHA reachable from `origin/main`;
- Fly health and remote code probe;
- post-fix Archi.ru row and publication-plan counts;
- finalizer artifact or compact production summary showing link/article lanes.

## Immediate Mitigation

The routing predicate now gives current actual-image evidence precedence over
the no-media-reuse link lane. No provider-quota bypass was enabled.

## Corrective Actions

- split external article identity/rights qualification from the actual routing
  decision;
- add the missing actual-image external-article regression control;
- run a no-LLM reconciliation finalizer after deploy to restore the preserved
  terminal Archi.ru verdict.

## Follow-up Actions

- [ ] Region Talk owner: verify the retryable article cohort after Gemini RPD
  resets and record the increased daily article count.
- [ ] Region Talk owner: add a production dry-run/preflight command that reports
  article-lane routing without requiring provider calls.

## Release And Closure Evidence

- deployed SHA: `7d1f530d2206504e0238fca12fd17c68ae5b09fc`, reachable from
  `origin/main`;
- deploy path: clean Fly deploy, machine version `v1808`, image
  `deployment-01KYWR5YEYHTJBJXNE2TCAMT4S`;
- health: `/healthz` returned `ok=true`, `ready=true`, Region Talk scheduler
  `ok`, with the next autonomous start at `2026-07-31T19:20:00Z`;
- remote code probe: `uses_external_link_article_lane` and the actual-image
  precedence branch are present in the deployed CandidateReport/finalizer;
- regression checks: the focused link-only/actual-image/negative controls
  passed; the complete finalizer module passed 42 tests;
- controlled provider evidence: the preceding bounded catch-up reserved all
  three new requests through the shared gateway and persisted RPD failures as
  retryable `gemini_rate_limited` / `llm_budget_deferred`; no raw-key fallback
  was enabled;
- compensating artifact:
  `/data/runtime_logs/region_talk/article-link-reconcile/region-talk-article-incident-reconcile-20260731T1854Z-7d1f530d/publication_finalizer_results.json`;
- plan artifact:
  `/data/runtime_logs/region_talk/incident-reconcile-publication-plan-20260731T1857Z.json`;
- post-fix production state: Archi.ru is `gemini_accept` / `sent_to_chat`, with
  `image_queue_status=actual_scored` and
  `image_model_input_type=actual_image`; the rebuilt plan contains one article
  and 14 social posts.

## Prevention

Article routing tests now contain positive link-only, positive actual-image and
negative ambiguous/media-reuse/social-no-media controls. Production state and
the daily plan were restored without another provider verdict, so this
incident is closed. The broader article-supply RPD limit remains a separate
capacity follow-up and does not justify bypassing the shared request gateway.
