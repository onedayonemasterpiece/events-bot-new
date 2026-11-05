# INC-2026-11-05-guide-title-drift-duplicate

Status: open
Severity: sev2
Service: Events Bot guide excursions
Opened: 2026-11-05
Closed: —
Owner: native Codex recovery thread
Registry: inc_4989967147933fd3484cb758
Related incidents: INC-2026-11-04-daily-announcement-duplicates; INC-2026-11-04-guide-vk-card-quality
Related docs: docs/features/guide-excursions-monitoring/dedup.md

## Summary and impact

Normal light guide import11648 completed07:32:54 UTC without transport errors, but created718 for the same single Sovietsk walk already stored as716 and marked published in issue329. The candidate can therefore repeat in a later digest. Actual repeat public publication is not yet established. Same original source24/message3914, November6 14:00, guide Andrey Boyko and exact booking URL; source text announces one tour. Titles differ: «Кинопрогулка по Советску» versus «Кинематографический Советск».

## Detection and timeline

-07:36 UTC: scheduled run11648 terminal success,30 sources47 posts,2 creates6 update operations, zero errors. All8 action/source anchors and final fields reconcile; updates cover4 unique old rows. New719 is a different dated outing.
-07:38: exact716/718 provenance read confirms same original post and outing. No canonical/public writes.
- Baseline display control returns718 as a new digest candidate: heuristic same_contact_but_different_product, despite same_channel_post=true. Existing published-reference exclusion cannot suppress separate clusters.
- Guard-only real model control reaches the default Gemma but exhausts existing pair budget, conservatively leaving two clusters. This is not a passing live control.
-07:42: same planned routing through existing Lite3.5 judge returns same_occurrence in2.416s,82 output tokens, shared limiter receipt. Existing cluster coverage716/718 makes published-reference filter suppress718. No Event, Job or canonical row writes.

## Root cause

Source fingerprint intentionally hashes title/date/time; model paraphrases can create separate rows. The digest has an existing semantic dedup stage for this. Its low-title-overlap distinct heuristic incorrectly bypassed the model even when both candidates reference one post. This is separate from catalogue ordinal identity and native ticket guards.

The existing default Gemma judge did not return inside the unchanged pair budget in the exact control. A working existing Lite model resolves the same pair promptly; increasing budgets or introducing another repair path is unnecessary.

## Corrective action and scope

One existing heuristic condition now applies only to different source posts. Same-post pairs reach the existing model, which may still return distinct for genuinely separate simultaneous tours. Default judge changes to Gemini3.5 Flash Lite through the existing model setting. Two scalar/condition line changes, zero net production lines. No new state, retry, worker, merge-by-date rule, source disable or canonical cleanup. Existing no-LLM fallback, timeouts and limiter remain.

## Automation contract

Treat as regression guard when changing guide fingerprints, pair candidate routing, heuristic distinct decisions, pair judge or published-reference suppression.

Affected surfaces: guide_excursions/dedup.py, parser fingerprint, service digest preview, visual digest dedup, existing model default.

Mandatory checks: same-post paraphrase reaches judge and published-reference suppression; same-post genuinely distinct products remain distinct by model; different-post shared-contact protection; distinct time/route model controls; unchanged date/native identity and publication spacing regressions. Baseline2 failing/2 passing candidate-filter tests; after28 focused digest/visual tests pass. Real Lite contrasts return distinct for simultaneous unrelated routes and same_template_other_occurrence for14:00/16:00 sessions, zero Event/Job writes. Provider audit unapproved0, allowlisted_debt0. Real controls and exact artifact receipts retained.

## Required release and closure evidence

Not delivered yet. Required: merged main, actual-runtime ancestry, full image proof, initialized isolated routing controls, safe Events-only cutover, live health/settings and normal visual/new-digest result. Do not close from the model-only probe or transport success alone. No deletion, merge or publication-mark mutation of716/718 has occurred. Original issue329 and posts preserved.

Retained artifact directory: /home/dev/artifacts/events-bot-new/20261102T233947Z-intake-recovery-20261103. Key evidence: guide11648-terminal-summary.json, guide11648-receipts-0738.json, guide716-718-provenance-0738.json, guide716-718-display-control.json, guide716-718-model-routing-probe.json/.log, guide716-718-lite-routing-probe.json/.log, guide-same-post-before.log and guide-same-post-after.log.


## Delivery and reviewer evidence, November5 08:06 UTC

PR828 delivered as merged main544379c810e2c9b5b418658e7a2b5b30468a8b32, immutable sha256:b514971057c7e1771fc0be0be52d4bf5ffdaee46ac26535bdfa93e79f46c1a9d. All four required CI passed. Full4448-file proof, initialized isolated actual routing and published-reference controls, inherited guards, wrong SHA and HTTP isolation passed. Own preflight removed. Original idle ops/MCP/background guard passed with no exception; no media claim crossed restart. Live model default and same-post/different-post routing pass, ready=true, settings and Kotopogoda unchanged. Normal08:30 visual digest still requires acceptance. No canonical merge or publication-mark cleanup.

Auditor Ester Show replied in assigned thread1793865044.069729 and supports the routing condition, requesting seven-day timeout evidence before model change. Exact retained coverage spansOct29 07:57–Nov5 07:57 UTC, oldest retained logOct29 04:15.22 dedup summary runs,7 pair-budget failures; after excluding my3 known diagnostic summaries and1 diagnostic timeout:6 exhausted budgets across19 summaries. Other operators probes cannot be separated; this is not a production-only per-pair rate. Gemma structured attempts:11 reserve/send outcomes,2 success4 errors5 cancellations, including1 known diagnostic cancellation. Failure is recurrent rather than one isolated timeout. Shared Lite scope sample146/500 RPD, limits/retries unchanged. This supports keeping the existing Lite default change; no new fallback or quota override. First aggregate probe hit a historical string-valued error field; corrected read-only decoder to accept only dictionary metadata and reran. No production mutation from aggregation.

Retained guide-dedup-seven-day.json and guide-dedup-seven-day-interpretation.json contain scope/caveats; delivery receipt guide-post-dedup-delivery-receipt.json links qualification/live logs. Same remote Cherryflash session1564 remained rendering with its durable kernel/dataset; existing watchdog resumes only its poller. Actual fresh polls after restart remain a separate check.


## November 5 08:49 UTC — normal visual schedule observation

Normal scheduled visual digest11652/issue330 completed after the existing heavy wait and publicly contains one Sovietsk walk, display718, with no second716 entry. Actual summary reports15 raw rows,13 display rows and2 suppressed. Five final items are714/718/717/702/711. This accepts the visible within-schedule duplicate absence, not canonical cleanup or an additional normal new-occurrences digest run. The latter family has published-reference suppression; daily visual schedule has its separate existing selection semantics. No forced callback, replay or publication-mark mutation. Public source wall-238875824_382, timestamp08:43:59 UTC; evidence guide330-public-0848.json and guide330-dedup-actual.log.
