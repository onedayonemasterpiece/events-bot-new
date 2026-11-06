# INC-2026-11-06-eco-garazhka-attribution

Status: mitigated, prevention delivery pending; owner retains current story until expiry
Severity: sev2
Service: Telegram intake / Smart Update / Cherryflash ECO
Opened: 2026-11-06
Closed: —
Registry: inc_c1ac1f6a0678850f482f1848
Owner: native Codex recovery thread
Related incidents: INC-2026-11-04-cherryflash-cross-year-label; INC-2026-11-04-daily-announcement-duplicates

## Impact and target behavior

Owner supplied today's story screenshot and an organization representative's denial: EcoKoenig does not organize or participate in Garazhka; it only republishes announcements. Event10471 and the ECO1573 video falsely identify the festival as EcoKoenig. Preserve the valid November7–8 event, source bindings and identity guards; use the event's grounded name, not the publishing channel's name. Do not invent an organizer from channel metadata or forwarded first-person prose.

## Evidence and root cause

Live local Telethon read establishes ecoklgd/3626 is forwarded from channel2180759376/post1622, the official garazhka_kld channel. Its text says «До встречи на Гаражке». Official1623/1624 and independent meowafisha/9089 also name Garazhka. Initial candidate26061 already has title «Фестиваль ЭкоКёниг» and festival «ЭкоКёниг», created November4 00:57UTC; Event10471 created00:58UTC. Thus the renderer inherited an already wrong canonical title; no evidence attributes its introduction to today's renderer dependency or formatter fixes.

Canonical fact208464 persisted the unsupported festival identity. Correct titles from official1623 and independent9089 were subsequently rejected with semantic_title_mismatch (facts209812/210184). Fact-first descriptions kept reproducing the wrong festival. This is an extraction grounding error compounded by protection of an already wrong identity; it does not justify globally weakening title/native/date guards.

ECO session1573, partner_eco_nature_001, published November6 11:41:57UTC. Event10471 is an exact selected item. Managed business story receipt1866 is saved; story1574 is only SELECTED, not a second published recovery. Do not rerun the old Cherryflash driver or create another kernel blindly. Existing video/story bytes retain the old caption until separately repaired; canonical acceptance alone is not video repair.

## Immediate mitigation

Owner directly requested source verification and correction. Exact bounded transaction corrected title to «Фестиваль осознанного потребления „Гаражка“», festival to «Гаражка», retired unsupported fact208464 to conflict and inserted audit note212223 from official source13992414. The transaction checks old values, event schedule, official source and absence of concurrent MCP intake; repeated execution returns already_applied. No schedule, location, source binding or poster change.

Typed description operation evt_op_RRV2kGE-3NbCDl3mNwSWsgin3KQ6nngt accepted once at19:11:26UTC; only the false festival name was replaced in the existing description. A broader first proposal was rejected PUBLIC_TEXT_FACT_UNRESOLVED and never committed; consistency guard was retained. Ordinary workers completed Telegraph143666 and calendar143667/VK143668 reconciliation. Public Telegraph API and calendar10376 independently show the correct Garazhka title and no EcoKoenig attribution. Independent public VK13386 and rich TG5157 now also show the correct title and no EcoKoenig attribution; rich TG edit observed19:25:55UTC. All five exact outbox jobs143665–143669 are done. MCP revision-status still reports telegram/calendar outcome_unknown despite exact public and DB receipts; do not retry them on that status alone. Static-site last build remains failed/not built and is not claimed repaired. The owner explicitly chose «Оставить историю до истечения» in the native dialogue on November6 19:33UTC. Leave story1866 unchanged until expiry: do not delete, rerender or resend it. This resolves the requested story disposition, not prevention delivery.

## Narrow prevention candidate

Two existing event-schema field descriptions, shared by source-level primary/verifier/final parsing and legacy extraction, clarify publisher versus event identity. Title and festival must be grounded in message/OCR, including inflected names; generic event categories are not festival names. The first three-line prompt-rule proposal was rejected after an actual Lite control still returned the false EcoKoenig name; it is not the final implementation. No new state, flag, retry, worker, quota, normalizer or name-specific keyword guard. No global relaxation of semantic title safety. This is a semantic LLM-first fix.

Initial model-only baseline timed out at the existing90-second cap: not a reproduced before-model failure. First planned exact-source control avoided false EcoKoenig but returned generic festival/name, so it was not accepted as full preservation of the specific name. The explicit-named-festival EcoKoenig opposite control passed. Refined prompt-rule specific-name control also timed out at the existing90-second cap. Actual Lite then reproduced the unsupported EcoKoenig result with that rule, so the proposal was rejected. Moving the instruction into the authoritative title/festival schema descriptions produced actual title and festival «Гаражка» in24.17seconds on the exact forwarded source, preserving November7–8 and12:00. The same planned schema preserves the explicitly named EcoKoenig opposite control in28.28seconds. Both exact live outputs cross the real server Telegram import and Smart Update on isolated SQLite, network blocked, with valid typed evidence manifests. All calls use existing shared limiter and runtime gateway, production Event/Job writes zero. Do not claim qualification from these partial controls.

## Automation contract and required checks

Treat as regression guard when changing Telegram source metadata, forwarded-post interpretation, source semantic prompts, festival extraction, title updates, locked facts or Cherryflash copy selection.

Mandatory: exact forwarded source preserves Garazhka identity; opposite explicitly named EcoKoenig festival remains valid; no organizer/participant inferred from publisher alone; normal multi-event/date/native/19099 controls remain; source-level outputs cross real Telegram server import and Smart Update on isolated SQLite with no production writes. Current replay fixture provides typed evidence manifests; initial incomplete fixture was rejected, then corrected rather than weakening the contract. Both actual live schema-control outputs are substituted into the import fixture: real Telegram import and Smart Update create two appropriately distinct named Events with zero errors, network blocked.118 import/producer/source-contract tests pass (including the real-output replay). Earlier expected-output-only checks are not counted as producer evidence. This tests downstream identity preservation, not proof of the live producer's reasoning.

Before delivery: latest-head all required CI, exact merged main/actual-runtime ancestry, full source proof, initialized isolated preflight including controls, negative SHA/HTTP isolation, safe idle Events-only cutover and live verification. No prevention deployed yet. No closure until prevention delivery and passing end-to-end extraction/replay are evidenced; current story disposition is explicitly accepted by the owner until expiry.

## Required evidence

Before closure retain exact source/forward identity, before/after model outputs, passing same-import replay, opposite control, merged-main and actual immutable release, corrected current public projections and the explicit owner decision retaining this story until expiry. Existing story repair is independent from canonical repair and prevention.

## Retained evidence

Managed retained directory: /home/dev/artifacts/events-bot-new/20261102T233947Z-intake-recovery-20261103.

- eco-source-live.json and eco10471-evidence.json: exact source/forward/fact provenance.
- eco10471-title-plan.json / eco10471-title-applied.json: once-only canonical mitigation.
- eco10471-telegraph-public.json / eco10471-calendar-public.json: actual corrected public content.
- eco-publisher-model-control.log / eco-publisher-model-specific.log: bounded shared-limiter controls and explicit caveats.
- eco-publisher-tests-final-networkblocked.log:52 passing tests; eco-schema-replay-tests.log imports actual schema-control output.
- eco-publisher-lite-specific.log: rejected prompt-rule proposal; eco-publisher-lite-schema.log: exact-source schema control.
- eco10471-vk-public.json / eco10471-rich-tg-public.json: independent corrected VK and rich Telegram content. Earlier rejected fixture/control evidence is retained.

No production rebuild, restart, source replay, story resend or unrelated data cleanup has occurred.
