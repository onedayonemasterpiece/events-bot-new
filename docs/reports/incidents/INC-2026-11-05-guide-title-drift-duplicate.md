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
