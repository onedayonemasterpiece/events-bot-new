# INC-2026-04-23 Guide Digest Extraction Loss

Status: active
Severity: sev1
Service: guide excursions monitoring / scheduled guide digest
Opened: 2026-04-23
Closed: 2026-04-23
Owners: bot operations / guide excursions
Related incidents: `INC-2026-04-21-guide-gemma4-partial-monitoring`
Related docs: `docs/features/guide-excursions-monitoring/README.md`, `docs/llm/request-guide.md`, `docs/operations/cron.md`

## Summary

The April 22 guide digest published only one excursion after a three-day publication gap, while monitoring runs on April 21-22 had reported at least five new occurrence outputs. Production evidence showed that some outputs were correctly ineligible, but concrete future excursions were lost or delayed because Gemma 4 extraction and digest selection did not fully preserve multi-block schedule facts and did not compare fresh candidates against already published future occurrences.

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
- 2026-04-23 10:18 UTC: user review identified another missed source post, `@katimartihobby/1940`, whose schedule uses Telegram keycap emoji dates (`3️⃣ мая`, `1️⃣3️⃣ мая`) and had been lost by the multi-block extraction path.
- 2026-04-23 10:51 UTC: deployed `2537f9b8`, fixing OCR `hashlib` import and allowing block rescue after a full-post `announce_multi` timeout.
- 2026-04-23 10:54-11:40 UTC: production catch-up `run_id=326250d4aaf9` completed `partial=false`, `posts=72`, `prefilter_true=46`, `llm_ok=46`, `llm_error=0`, `occurrences_total=21`; `@katimartihobby` extracted `6` occurrences.
- 2026-04-23 11:46 UTC: recovery import for `run_id=326250d4aaf9` succeeded after freeing `/data` artifact space and retrying with a longer SQLite timeout.
- 2026-04-23 12:03 UTC: compensating guide digest issue `#42` was published with occurrence ids `[142, 143, 144, 145, 147]`; occurrence `#146` remained unpublished because Gemma marked it `tentative_or_free_date`.
- 2026-04-23 12:04 UTC: deployed `11645b57`, adding bounded full-post timeout for multi-announce extraction, OCR post/media context logs, and the disqualifying-reason eligibility guardrail.
- 2026-04-23 12:20 UTC: post-compensation audit found that `@vkaliningrade` had extracted `9` occurrence payloads, but they mapped to already published occurrences (`#35`, `#120`, `#129`, `#61`) rather than new unpublished cards. The same audit found residual future `eligible + unpublished` rows from `@gid_zelenogradsk/2796`: `#140` was a duplicate of the already published `Огонь Брюстерорта`, while `#141` looked like a still-unpublished future Зеленоградск walk.
- 2026-04-23 12:30 UTC: follow-up fix added digest-time comparison against already published future occurrences through the existing guide dedup stage, ISO-only date filtering for candidate queries, shared post context for block-level Gemma rescue, and a timeout around public identity resolution.

## Root Cause

1. `trail_scout.announce_extract_tier1.v1` and block-level rescue did not explicitly tell Gemma 4 that each dated schedule line in a multi-announcement post must become one occurrence, and that a dated public schedule with shared booking facts and no sold-out/cancelled marker is available/digest-eligible.
2. `route_weaver.enrich.v1` could downgrade a seeded occurrence with grounded future date/time/booking facts to unknown availability or `digest_eligible=false` without an explicit disqualifying source claim.
3. `trail_scout.screen.v1` over-trusted guide sources and treated dated non-excursion events, on-demand offers, and no-date posts as digest-ready signals.
4. `title_normalized` guidance was too loose, allowing duplicate identities from the same source post when the same route/date was phrased with different guide/source suffixes.
5. OCR media hashing used `hashlib` without a top-level import, so image OCR failed open with `NameError` on poster/photo posts until the runtime import was fixed.
6. Long `announce_multi` full-post extraction retried the same broad prompt before block rescue, causing avoidable latency even when `schedule_blocks` were already available for smaller per-block LLM calls.
7. The eligibility normalizer did not treat LLM-provided disqualifying reasons as authoritative when the boolean field contradicted the reason.
8. Digest selection deduped only the currently unpublished candidate set. A repost/update of an already published future excursion could remain as a fresh unpublished row and compete with truly new rows unless it was manually remediated.
9. Block-level rescue received the isolated schedule block but not enough shared post context, so common booking/contact/price facts could be absent from cards materialized from compact multi-date schedules.

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
- Verify OCR logs include source username, source post id, media message id, image index, short hash, and success/empty/error status when OCR is enabled.

### Required evidence

- Live Gemma 4 eval logs under `artifacts/codex/guide-gemma4-incident-eval/`.
- Test output for the guide regression suite.
- Deployed SHA reachable from `origin/main`.
- Production `ops_run_id` / `run_id` for the fixed monitoring run.
- Guide digest issue/message evidence for the compensating publication, or explicit evidence that no missed still-future digest-ready occurrence remains.
- Data evidence that internally inconsistent `digest_eligible=true` plus disqualifying `digest_eligibility_reason` rows are normalized or remediated before publication.

## Immediate Mitigation

- Prompt tuning was completed against live Gemma 4 cases with OCR disabled to isolate text extraction behavior.
- No deterministic semantic regex bypass was added; the incident was corrected at the LLM prompt/stage-contract layer.
- Production rows created by the faulty extraction window were remediated after the fixed extended catch-up: `#130` is `sold_out`, `#136` is `non_target`, and `#137` is `duplicate`.

## Corrective Actions

- Tightened `trail_scout.screen.v1` so dated non-excursion events, volunteer cleanups/subbotniks, generic meetups, and no-date on-demand offers are not marked digest-ready by default.
- Tightened `trail_scout.announce_extract_tier1.v1` and block-level rescue so multi-date schedule lines become separate occurrences and available future lines remain digest-eligible.
- Tightened `route_weaver.enrich.v1` so it cannot downgrade grounded future public schedules without an explicit sold-out/cancelled/past/private claim.
- Added prompt-contract regression tests for the above LLM-first rules.
- Added keycap-date schedule-anchor normalization as a syntax aid for block splitting and Gemma context, then kept semantic occurrence materialization in Gemma.
- Added fail-open block rescue after bounded `announce_multi` full-post extraction so a slow broad prompt cannot erase all dated blocks.
- Fixed OCR media hashing import and added post/media-level OCR success/empty/error/retry context logs.
- Added an eligibility consistency guardrail: disqualifying LLM reasons such as `tentative_or_free_date` override an inconsistent positive boolean.
- Added digest-time comparison between fresh candidates and already published future occurrences through the existing LLM-first dedup stage, preventing reposted duplicates from crowding out real new cards.
- Added ISO-date filtering to digest candidate queries so recurring/free-text dates cannot enter daily `new_occurrences` until materialized as concrete dates.
- Added shared post context to block-level Gemma rescue prompts so common booking/contact/price facts can be applied by the LLM to each dated block without deterministic semantic extraction.
- Added a timeout around public identity resolution so a Telethon username lookup cannot stall digest preview/publication.

## Follow-up Actions

- [ ] Promote the incident eval pack from artifact-only into a minimal canonical fixture/harness that can be run without leaking production secrets.
- [ ] Split the operator summary into raw occurrence outputs vs digest-ready candidates so `Новых выходов` cannot be misread as subscriber-visible cards.
- [ ] Add a scheduler/catch-up smoke that proves a missed daily/full slot either publishes same-day fresh material or records explicit no-candidates evidence.
- [ ] Add retention policy for `/data/guide_monitoring_results` so old 50+ MB Kaggle bundles cannot block recovery import with `Errno 28`.
- [ ] Reduce or bound server-side guide digest writer/enrichment retries on repeated provider `500` so compensating publication is not delayed by non-critical copy polish.
- [ ] Add a persisted duplicate-remediation pass so candidates suppressed against already published reference rows are also marked as duplicate/no-digest in storage, not only suppressed at publish time.

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
- second deployed SHA: `11645b57 fix(guide): add OCR context logs and eligibility guard`, reachable from `origin/main`; includes prior `bdb265ef fix(guide): bound multi-announce full extraction` and `2537f9b8 fix(guide): harden OCR and multi-block rescue`.
- second deploy path: manual Fly deploy from clean hotfix worktree; image `events-bot-new-wngqia:deployment-01KPX3JVJPTRF9KXDMKRW480EW`, machine `48e42d5b714228`, version `992`; Fly memory returned to standard `1024 MB`.
- second regression checks: `python -m pytest tests/test_guide_gemma4_prompt_contract.py tests/test_guide_kaggle_schema_contract.py tests/test_scheduling_guide_digest.py tests/test_guide_local_llm_timeout_contract.py -q` passed (`14 passed`); `python -m py_compile kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py guide_excursions/service.py`; `git diff --check`.
- fixed production catch-up: `run_id=326250d4aaf9`, full `days_back=8`, `limit=80`, `sources=12`, `posts=72`, `prefilter_true=46`, `llm_ok=46`, `llm_deferred=0`, `llm_error=0`, `occurrences_total=21`, `partial=false`; `@katimartihobby` extracted `6` occurrences.
- compensation publication completed: guide digest issue `#42`, `status=published`, `items_json=[142,143,144,145,147]`, `published_at=2026-04-23 12:03:02`, `@wheretogo39` message ids `[48,49]`, `@youwillsee39` message ids `[66,67]`.
- eligibility remediation: `guide_occurrence #146` from `@katimartihobby/1940` kept unpublished with `digest_eligible=0`, `digest_eligibility_reason=tentative_or_free_date`; published occurrences `#142`, `#143`, `#144`, `#145`, and `#147` have `published_new_digest_issue_id=42`.

## Prevention

- Keep multi-date schedule, no-date/on-demand, sold-out, and non-excursion volunteer controls in every guide Gemma 4 prompt-tuning pass.
- Treat guide digest completeness as occurrence-level evidence, not as a run-level output counter.
- Preserve LLM-first ownership for semantic scope and eligibility decisions; deterministic logic may guard schema/identity safety but must not replace prompt-stage meaning decisions.
