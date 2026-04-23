# INC-2026-04-23 Guide Digest Extraction Loss

Status: closed
Severity: sev1
Service: guide excursions monitoring / scheduled guide digest
Opened: 2026-04-23
Closed: 2026-04-23
Owners: bot operations / guide excursions
Related incidents: `INC-2026-04-21-guide-gemma4-partial-monitoring`
Related docs: `docs/features/guide-excursions-monitoring/README.md`, `docs/llm/request-guide.md`, `docs/operations/cron.md`

## Summary

The April 22 guide digest published only one excursion after a three-day publication gap, while monitoring runs on April 21-22 had reported at least five new occurrence outputs. Production evidence showed that some outputs were correctly ineligible, but one concrete future excursion was lost because Gemma 4 extraction marked a dated, bookable occurrence as `status=unknown`, `availability_mode=unknown`, and `digest_eligible=false`.

## User / Business Impact

- Subscribers did not receive at least one valid future excursion in the compensating digest window.
- Operators could not trust the `Новых выходов` counter because it included ineligible occurrences, duplicate/no-date noise, and non-excursion events.
- The gap was customer-visible: guide digest issue `#37` contained only one item despite multiple monitoring runs since issue `#36`.

## Detection

- Detected manually from the April 22 Telegram digest screenshot and run summaries for `ops_run_id=779` and `ops_run_id=787`.
- Confirmed against a production DB snapshot in `artifacts/db/prod_guide_digest_audit_2026-04-23.sqlite`.
- Reproduced with a live Gemma 4 eval pack under `artifacts/codex/guide-gemma4-incident-eval/`.

## Timeline

- 2026-04-19 18:30 UTC: guide digest issue `#36` was published with occurrences `[125, 126, 127, 129]`.
- 2026-04-20 18:10 UTC: `ops_run_id=765` reported `created=4`, `updated=4`, but no digest issue was published in the inspected window.
- 2026-04-21 18:10 UTC: `ops_run_id=779`, `run_id=a13e5f3e1d35`, reported `created=2`, `updated=6`, `partial=true`.
- 2026-04-22 08:18 UTC: compensating digest issue `#37` published only occurrence `#134`.
- 2026-04-22 11:20 UTC: `ops_run_id=787`, `run_id=677cf5eeb887`, reported `created=3`, `updated=3`.
- 2026-04-23: audit found `guide_occurrence #130` from `@amber_fringilla/5988` had a concrete future date/time (`2026-05-08 09:00`) and booking context, but Gemma extraction persisted it as not digest-eligible.
- 2026-04-23 08:53 UTC: deployed prompt hotfix `c73de47e` was followed by production catch-up `ops_run_id=810`, `run_id=a82ef5045439`, with `partial=false`, `errors=0`, `warnings=0`.
- 2026-04-23 09:07 UTC: extended production catch-up `ops_run_id=811`, `run_id=5aa425525c6b`, scanned `days_back=8`, imported current source state with `llm_ok=45`, `llm_error=0`, `partial=false`, `errors=0`, `warnings=0`.
- 2026-04-23 09:35 UTC: final digest preview `#40` contained no items (`items=[]`, `covered=[]`), so no subscriber-visible compensating publication was required.

## Root Cause

1. `trail_scout.announce_extract_tier1.v1` and block-level rescue did not explicitly tell Gemma 4 that each dated schedule line in a multi-announcement post must become one occurrence, and that a dated public schedule with shared booking facts and no sold-out/cancelled marker is available/digest-eligible.
2. `route_weaver.enrich.v1` could downgrade a seeded occurrence with grounded future date/time/booking facts to unknown availability or `digest_eligible=false` without an explicit disqualifying source claim.
3. `trail_scout.screen.v1` over-trusted guide sources and treated dated non-excursion events, on-demand offers, and no-date posts as digest-ready signals.
4. `title_normalized` guidance was too loose, allowing duplicate identities from the same source post when the same route/date was phrased with different guide/source suffixes.

## Contributing Factors

- The operator summary field `Новых выходов` counts raw occurrence outputs, not only digest-ready cards.
- The previous `partial` incident had already required a compensating catch-up, so this semantic extraction loss was easy to misread as only a publication gate problem.
- The canonical eval pack did not include a multi-date schedule with one sold-out line, one available line, an on-demand no-date offer, and a volunteer cleanup negative control.

## Automation Contract

### Treat as regression guard when

- Changing guide Gemma 4 screen/extract/enrich prompts.
- Changing multi-announcement block splitting, block rescue, or occurrence identity fields.
- Changing guide digest eligibility semantics, digest preview, or scheduled catch-up behavior.

### Affected surfaces

- `kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py`
- `guide_excursions/service.py`
- guide digest preview/publish and scheduled guide monitoring
- production guide tables: `guide_occurrence`, `guide_digest_issue`

### Mandatory checks before closure or deploy

- Run the live Gemma 4 incident eval cases for `@amber_fringilla/5988`, `@ruin_keepers/5221`, `@ruin_keepers/5222`, `@excursions_profitour/918`, and `@excursions_profitour/908`.
- Verify `@amber_fringilla/5988` extracts the available `Нижний пруд` occurrence as `digest_eligible=true` and the sold-out `Суздальский парк` occurrence as `digest_eligible=false`.
- Verify `@ruin_keepers/5221` extracts one digest-eligible occurrence without same-post template/no-date duplicates.
- Verify `@ruin_keepers/5222` is ignored as non-target volunteer cleanup/subbotnik unless a future prompt change introduces an explicit guided-tour offer in the fixture.
- Verify on-demand/no-date `@excursions_profitour/918` and `@excursions_profitour/908` do not become digest-ready without concrete dates.
- Run regression tests for guide schema, scheduled digest partial gate, local LLM timeouts, and prompt contract.
- After deploy, run a production-equivalent guide monitor/catch-up and publish a compensating digest if any missed, still-future digest-ready occurrence exists.

### Required evidence

- Live Gemma 4 eval logs under `artifacts/codex/guide-gemma4-incident-eval/`.
- Test output for the guide regression suite.
- Deployed SHA reachable from `origin/main`.
- Production `ops_run_id` / `run_id` for the fixed monitoring run.
- Guide digest issue/message evidence for the compensating publication, or explicit evidence that no missed still-future digest-ready occurrence remains.

## Immediate Mitigation

- Prompt tuning was completed against live Gemma 4 cases with OCR disabled to isolate text extraction behavior.
- No deterministic semantic regex bypass was added; the incident was corrected at the LLM prompt/stage-contract layer.
- Production rows created by the faulty extraction window were remediated after the fixed extended catch-up: `#130` is `sold_out`, `#136` is `non_target`, and `#137` is `duplicate`.

## Corrective Actions

- Tightened `trail_scout.screen.v1` so dated non-excursion events, volunteer cleanups/subbotniks, generic meetups, and no-date on-demand offers are not marked digest-ready by default.
- Tightened `trail_scout.announce_extract_tier1.v1` and block-level rescue so multi-date schedule lines become separate occurrences and available future lines remain digest-eligible.
- Tightened `route_weaver.enrich.v1` so it cannot downgrade grounded future public schedules without an explicit sold-out/cancelled/past/private claim.
- Added prompt-contract regression tests for the above LLM-first rules.

## Follow-up Actions

- [ ] Promote the incident eval pack from artifact-only into a minimal canonical fixture/harness that can be run without leaking production secrets.
- [ ] Split the operator summary into raw occurrence outputs vs digest-ready candidates so `Новых выходов` cannot be misread as subscriber-visible cards.
- [ ] Add a scheduler/catch-up smoke that proves a missed daily/full slot either publishes same-day fresh material or records explicit no-candidates evidence.

## Release And Closure Evidence

- deployed SHA: `c73de47e fix(guide): tune gemma digest extraction prompts`, reachable from `origin/main` at deploy time.
- deploy path: manual Fly deploy from clean hotfix worktree; image `events-bot-new-wngqia:deployment-01KPWRC3PA2G4GXBAQD51NDGBJ`, machine `48e42d5b714228`, version `988`.
- regression checks: `python -m pytest tests/test_guide_gemma4_prompt_contract.py tests/test_guide_kaggle_schema_contract.py tests/test_scheduling_guide_digest.py tests/test_guide_local_llm_timeout_contract.py -q` passed (`8 passed`); `python -m py_compile kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py`; `git diff --check`.
- live Gemma 4 eval: artifact-only run under `artifacts/codex/guide-gemma4-incident-eval/`; verified `@amber_fringilla/5988`, `@ruin_keepers/5221`, `@ruin_keepers/5222`, `@excursions_profitour/918`, and `@excursions_profitour/908` against the prompt contract without adding deterministic semantic extraction.
- post-deploy verification: `/healthz` returned `ok=true`, `ready=true`, `db=ok`, no issues after deploy and again after returning Fly memory from temporary `2048` MB to standard `1024` MB.
- production catch-up: `ops_run_id=810`, `run_id=a82ef5045439`, full `days_back=5`, `sources=12`, `posts=42`, `prefilter=24`, `llm_ok=24`, `llm_deferred=0`, `llm_error=0`, `created=0`, `updated=3`, `partial=false`, `errors=0`, `warnings=0`.
- extended production catch-up: `ops_run_id=811`, `run_id=5aa425525c6b`, full `days_back=8`, `limit=80`, `sources=12`, `posts=72`, `prefilter=45`, `llm_ok=45`, `llm_deferred=0`, `llm_error=0`, `created=0`, `updated=7`, `partial=false`, `errors=0`, `warnings=0`.
- data remediation: `guide_occurrence #130` set to `status=sold_out`, `digest_eligible=0`, `digest_eligibility_reason=sold_out`; `#136` set to `status=non_target`, `digest_eligible=0`, `digest_eligibility_reason=non_target_volunteer_cleanup`; `#137` set to `status=duplicate`, `digest_eligible=0`, `digest_eligibility_reason=duplicate_published_occurrence_134`.
- compensating publication: not performed because final preview `#40` had `items=[]`, `covered=[]`; earlier preview-only issues `#38` and `#39` remained `status=preview`, `published_at=NULL`, with no published message IDs.

## Prevention

- Keep multi-date schedule, no-date/on-demand, sold-out, and non-excursion volunteer controls in every guide Gemma 4 prompt-tuning pass.
- Treat guide digest completeness as occurrence-level evidence, not as a run-level output counter.
- Preserve LLM-first ownership for semantic scope and eligibility decisions; deterministic logic may guard schema/identity safety but must not replace prompt-stage meaning decisions.
