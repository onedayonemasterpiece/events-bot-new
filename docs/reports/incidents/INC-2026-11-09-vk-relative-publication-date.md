# INC-2026-11-09-vk-relative-publication-date — VK publication date dropped before event parsing

Status: open
Severity: sev2
Service: VK auto import, Smart Update, daily event announcements
Opened: 2026-11-09
Closed: —
Registry: inc_c7fe095f3bb276f6a80d08ca
Related incidents: INC-2026-11-06-eco-garazhka-attribution, INC-2026-11-04-daily-announcement-duplicates, INC-2026-11-02-intake-quota-vk-media

## Impact and exact evidence

Actual November 9 ordinary Telegram announcements kenigevents/5539 and keniggpt/3447 contain Event 10699, Festival Garazhka, dated November 9. The official November festival and canonical 10471 run November 7–8. This is a new wrong-date card, not recurrence of the repaired EcoKoenig attribution on 10471.

Event 10699 was created by normal VK operation 11732 at November 8 04:16 UTC from wall-226847232_735. Authenticated source read: carrier timestamp November 7 21:06 UTC, original repost wall-232202380_39 at November 7 20:06:51 UTC, text «Приглашаем завтра на фестиваль … Garazhka … Понарт». Both timestamps fall on November 7 in Europe/Kaliningrad, so tomorrow refers to November 8. Event 10699 instead has November 9. Later kldevents/5370 attached to it. No assumption that all similarly named events are duplicates is made.

## Root and narrow prevention

The VK queue supplies publish_ts to build_event_drafts_from_vk. That value was used only by post-parse date normalization and dropped at vk_intake_parse_llm. The universal primary parser receives Today as the import date; its existing verification and final adjudication interfaces support published_at but received none. The narrow candidate forwards the timestamp to that existing metadata field and includes the publication anchor before the primary prompt. Relative date interpretation remains with the existing LLM; raw semantic_source_text is unchanged. No deterministic date rewriting, keyword classifier, state, retry, worker, cap or new recovery mechanism.

A differentiating original transport test has two failures and three passing scope/no-timestamp contrasts. Candidate date/context/rollover/poster/future/history/raw-first controls: 141 passed. Three pre-existing rollover fixtures patched an obsolete 4o entry point and accidentally attempted a provider call; they now intercept the actual universal parser. Their date assertions are unchanged. No provider send succeeded from that local failure. Offline provider audit passes. One fresh exact-source model-only control returned November 8 under the candidate publication anchor, after primary Gemma failed and the unchanged Lite 3.5 fallback completed. This is not original-output recovery or a same-model causal comparison. Its address is ungrounded («Кимрская 7»), so this control does not establish full event quality or enabled Smart Update/grounding acceptance. No guard was relaxed. An explicit-date opposite is separately checked. The initial harness omitted the main-module bootstrap and failed before any provider send; it is discarded evidence. Both original source and raw model trace are retained.

## Automation contract

### Treat as regression guard when

Changing VK publish_ts transport, primary source context, verifier/adjudicator metadata or relative-date normalization.

### Affected surfaces

vk_intake.py, universal source parsing, canonical Event 10699 and its managed public projections; daily announcements containing that card.

### Mandatory checks before deploy or closure

- Publication timestamp reaches primary prompt and existing verifier metadata, preserving exact raw carrier.
- Integer and aware-datetime publication inputs retain their epoch; absent timestamp preserves the current default.
- Existing scope, explicit-year rollover, future/past intake, OCR budget and raw-first contracts pass.
- Exact-source model output plus opposite explicit-date and no-timestamp controls; no unsupported semantic claim from a transport spy.
- All mandatory CI, merged-main ancestry, full-source image and initialized isolated import/identity/native/date controls; safe Events-only delivery and health/settings/Kotopogoda preservation.
- Exact managed graph before any repair; preserve original source and public references, distinct native/date events and the owner story-retention decision.
- Verify corrected canonical and public ordinary announcement results before closure. No whole-source replay or broad public deletion.

## Initial mitigation state, superseded by the delivery below

No canonical merge, retime, source replay, public post edit or resend has been applied. Candidate is not delivered. The already corrected 10471 and its original source anchors are preserved. Duplicate December 7 improvisation cards were also observed in the public added-events announcement; exact identity and native/time controls remain required before repair.

Separate video issues: session 1587 ended PUBLISH_BLOCKED with required Telegram channel BOOSTS_REQUIRED; do not downgrade required fanout or rerender a ready video. Sessions 1589/1590 encountered Kaggle 401/kernel access denial, despite the existing notebook completing earlier that morning. SDK 2.2.4 wraps 401/403 as a misleading wrong-slug ValueError. Runtime has not restarted since November 8 01:28 UTC; session 1589 recovery's restart wording is not proof of restart. No credential replacement, access bypass, duplicate kernel or registry reset was performed.

## Retained evidence

Existing retained task directory /home/dev/artifacts/events-bot-new/20261102T233947Z-intake-recovery-20261103: non-guide-runs-20261109.json, daily-nonguide-public-tg-20261109.json, nonguide-vk-public.json, vk-published-date-before.log, vk-published-date-regressions.log, vk-published-date-provider-audit.log and model-control logs. Required production source/public verification remains open.

## November 9, 11:30 UTC — delivered prevention and bounded correction

PR 877 and PR 876 are delivered together from exact main `70a3d33fccf77840373116d3869ad4e793707904`, image `intake-vk-source-date-20261109`, immutable `sha256:e7eca5e85f20b588152dc04c3dd26b0142bec9db97faa46232bc9f534c75da12`. All four required checks passed. Full 4462-file source verification, actual 516 ancestry, initialized isolated inherited controls, four retained parser routing controls and six metadata contrasts passed. Eight VK context/rollover controls passed. Wrong SHA rejected before initialization; final isolated HTTP503 was ready=false/database skipped after 231 seconds. The only mounts were readonly launcher and data tmpfs. Owned preflight was removed. Six metadata controls initially ran with production optimization; an explicit unoptimized repeat passed all six. Actual live controls and ready=true passed, settings and Kotopogoda stayed unchanged.

The route spy is not enabled semantic identity or final prose proof. The exact source model control returned November8 but also an unsupported address. No whole-event quality conclusion follows from that control. Ordinary post-release acceptance remains required.

The existing managed operator merged 10699 into 10471 ONCE under manifest `78df907e6b4fe17f561220ff9b118de42aa31b2de19a21ca0fb7c07d015d5a0a`. Actual scoped graph apply/verify was first exercised in isolated SQLite. Production verification preserved the correct November7–8 interval, both moved source anchors, candidate ownership, poster graph and every existing public URL. All scoped orphan/owner checks were zero; database-wide foreign-key violations remained the pre-existing 195, not newly fixed or introduced. No whole-source replay, guessed retime, old title/fact driver or story operation occurred.

The wrong festival block was removed from existing daily messages [kenigevents5539](https://t.me/kenigevents/5539) and [keniggpt3447](https://t.me/keniggpt/3447). Entity offsets outside that block were preserved. The production publishing bot edited those exact IDs, with no replacement send. Independent shared-E2E reads match the exact planned remaining text. Individual obsolete projections and VK daily2352 have not been independently corrected or verified; preserving their IDs does not establish content correctness. The incident remains open.

Separate non-guide findings and precise limits are in [the audit report](../2026-11-09-non-guide-announcement-audit.md). Kaggle 401 affected the ordinary11:20 guide as well as partner video launches. No credential rotation, access bypass or required-channel downgrade was made.

Additional retained receipts: vk-source-date-qualification.json, vk-source-date-delivery-receipt.json, garazhka10699-shadow-control.json, garazhka10699-merge-receipt.jsonl, garazhka-daily-public-plan.json, garazhka-daily-tg-edit-receipt.json and garazhka-daily-public-verification.json.
