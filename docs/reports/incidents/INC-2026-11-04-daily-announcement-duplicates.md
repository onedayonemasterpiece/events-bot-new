# INC-2026-11-04 — duplicates in the daily VK added-event announcement

Registry: inc_0701843eeb6896237c767628. Status: open. Owner request on November 4: check the hypothesis that identical names on the same date are duplicate events, accounting for different times.

Public post: https://vk.com/wall-231828790_2317, published November 4 at 18:12 UTC / 20:12 Kaliningrad. Full read-only provider receipt saved; public text is 11,949 characters. MCP item/search text clamps at 4,000 characters, so it was insufficient for a full inventory. A bounded wall.get read supplied the complete post, with zero provider writes. No public post was edited or deleted.

## Confirmed duplicates

| Canonical cards | Event | Date and time | Evidence |
| --- | --- | --- | --- |
| 10393 / 10522 | Лето будет общим | November 10, 18:00 | Same Tretyakov venue, address Парадная набережная 3 and ticket event 52251. The second source uses the broader tickets URL, but its source text names the same ticket event, date and time. Both rows are active; both link to wall-231920894_13255. |
| 10442 / 10511 | Русская фортепианная школа | November 21, 20:00 | Same ticket occurrence 52226, venue, date and time. Full source_text is byte-identical. These two are both the Лестница sector; this conclusion does not rely on merging another sector. Both link to wall-231920894_13415. |

The complete public post includes both rows of each pair. The SQL reconstruction of its existing recent-cutoff/date/active/silent selection yields exactly 189 rows, matching the headline. Twelve normalized exact-title/date groups were found; that is a candidate count, not a count of confirmed duplicate pairs. One title variant also yields a third movie row from the own-channel digest.

## A separate own-digest ingestion chain

The enabled Telegram source 61830 is our channel kenigevents. Its post https://t.me/kenigevents/5455, published November 3 at 06:00:36 UTC, is a compact multi-event digest. The normal Telegram import created nine active Events 10460–10468 from it on November 4, 00:07–00:34 UTC. All nine lack time and have the wrong generic venue «пространство, сообщество». This phrase is part of the adjacent event title «Современная библиотека: читатель, пространство, сообщество», not proof of a venue for every child.

Rows: Лабиринт 10460, Концерт средневековой музыки 10461, Что происходит с текстом, когда он попадает на сцену? 10462, Бесконечная история 10463, Дельгранде 10464, Лето будет общим 10465, Казимир Малевич 10466, Сказки старого пианино 10467, Под небом Италии 10468. They need exact provenance reconciliation against existing original-source cards before any managed merge. Do not count all nine as already merged or fully adjudicated duplicates.

Actual identity decisions show that this is more than rendering the same list twice. For the own-digest Лабиринт child, decision 19105 chose FINAL_DISTINCT against existing 10334. For Бесконечная история, 19127 first chose FINAL_MATCH against 10335, then 19129 chose FINAL_DISTINCT/festival_sibling_not_same_event. For the movie, 19143 chose FINAL_MATCH against 10393, then 19145 chose FINAL_DISTINCT/insufficient_identity_evidence_location_mismatch. Candidate/source receipts prove new canonical writes. The broad cause is ingestion of a sparse digest plus wrong child venue/identity adjudication; the minimal corrective rule is not yet qualified.

## Counterexamples: preserve different sessions

- Экслибрис 10478 / 10479: the same original source signalkld/12396 explicitly says child workshop November 7 at 12:00 and adult workshop at 19:00. These are distinct sessions. The compact daily format omits time, making both lines look identical and linking to one source/publication.
- Лукоморье 10368 / 10369: December 27 at 14:00 and 17:00. Different times; do not merge by title/date alone.
- Мой снежный друг 10413 / 10512: January 6 at 11:00 and 13:00. Different explicit ticket occurrences; preserve.
- Красный фонарь 10493 / 10494 has 17:00 / 19:00 in stored rows. Without fresh original-source verification this is not a confirmed distinct-session acceptance or a confirmed duplicate.

## Root cause limits and safe next work

For piano candidate 26339, existing decision 19427 marked FINAL_DISTINCT against card 10396, which is the other Партер ticket sector 52231. It did not resolve the already existing matching Лестница card 10442 before creating 10511. This supports a shortlist/selected-owner investigation; it does not justify weakening explicit ticket identity checks or conflating sectors.

Movie candidate 26355 ended CREATED with explicit_occurrence_id_conflict. Decision 19448 shows vector allow_create with unrelated older film candidates; the exact final create branch still needs targeted evidence. Do not infer a model quota/provider cause from this alone.

No source recovery, model call, DB mutation, merge, deletion, source disable or public correction was performed in this investigation. No blanket title/date deduplication should be added: real different-time controls above must survive. Existing main runtime remains the qualified deployed release; the diagnostics-only PR 817 did not create these rows, which predate its 20:23 UTC deployment. This chronology does not exonerate every earlier change; causal attribution needs exact creation-time runtime and code-path review.

Next: reconcile exact duplicate pairs through the existing managed merge/projection contract; inspect why a matching exact ticket owner was omitted, and apply existing source controls to prevent own-output reingestion while preserving legitimate source imports. Qualify any fix with the two true duplicates, different-session/sector controls and disputed-date 19099. Preserve normal Telegram 21:40 UTC and all other active incident deadlines. Do not replay accepted source packets.

## Retained evidence

All files are retained under /home/dev/artifacts/events-bot-new/20261102T233947Z-intake-recovery-20261103:

- daily-vk-public-posts-20261104.json — full public post receipt, zero provider writes.
- daily-duplicate-candidates-20261104.json — exact 189-row reconstruction and twelve candidate groups.
- daily-duplicate-provenance-20261104.json — canonical source texts, dates/times and source anchors.
- daily-duplicate-decisions-20261104.json — own source settings, nine created rows and candidate ledgers.
- daily-duplicate-identity-20261104.json — own-digest identity decisions.
- daily-duplicate-parser-decisions-20261104.json and daily-duplicate-movie-decisions-20261104.json — bounded parser identity evidence.

Typed event_get and broad event search exceeded the existing read budget; narrowed read-only SQLite queries were used rather than changing budgets. Early social tool calls used a string locator or timestamp rather than the required locator object/date format; corrected requests succeeded. No failed diagnostic call is counted as production evidence.


## 2026-11-04 21:52 UTC — ordinal drift reconstructed exactly

Read-only EventSource/candidate snapshots and an offline stdlib SHA-256
reconstruction reproduce both old and new occurrence keys from the current
_parser_occurrence_key material, changing only producer_ordinal. Piano10442
key corresponds to ordinal10; candidate26339/new10511 corresponds to ordinal15.
Movie10393 broad tickets anchor corresponds to ordinal4; candidate26355/new10522
to ordinal3. Same canonical URL, source type, date, time and empty end_date.
No model calls, production writes or replay. Evidence:
daily-duplicate-anchor-keys-2150.json and daily-duplicate-ordinal-reconstruction.json.

The producer includes its catalogue ordinal in parser-slot keys. Consumer
is_explicit_occurrence_key currently treats parser-slot as source/vendor
identity, unlike structured/ordinal fallbacks. _explicit_occurrence_conflict_ids
therefore lists the original owner as conflicting and removes it before fuzzy
matching. That explains why the proper owner was absent; the later wrong-sector
LLM decision is downstream. The producer ordinal changed with catalogue order,
not event identity. This is stronger evidence than the earlier selected-owner
hypothesis; it does not prove every possible duplicate has the same cause.

Minimal next experiment: distinguish generated parser fallback from true native
occurrence identity in the existing contract, preserving real source-native,
vendor and full-ticket conflicts. Do not merely remove the source guard or
retime/merge same-title cards. Inspect every is_explicit_occurrence_key consumer
and binding invariant first. Qualify with actual two drifted cases, true
different-time and same-slot/different-sector events, same-slot unrelated titles,
ambiguous catalogue fallback and Telegram disputed-date19099. A producer-only
new key would not alone repair legacy ordinal bindings. No implementation is
yet made; do not call the ordinal fix delivered. Existing normal TG11638 is
healthy and must finish before any cutover or standalone intake.


## 2026-11-04 22:32 UTC — narrow prevention candidate, not deployed

Classified as mechanical source/candidate recall: generated parser-slot hashes
include catalogue order and are not source-native identity. The existing
is_explicit_occurrence_key classification now treats them as fallback keys,
like structured/ordinal keys. All four consumers were reviewed (two conflict
checks, exact source-anchor filter, definition). No producer key, DB schema,
binding uniqueness rule, native-ticket guard, model prompt or verdict changed.
Native/vendor/ticket keys retain protection; matching still reaches existing
identity gates. Net production +1 line (comment), zero net functional lines,
budget12. No new states, retries, workers, caches or recovery mechanism.

76 parser/source-ownership/identity/occurrence regression tests pass. Current
fixtures with the original function restored in memory have five failures,
one passing native-ID control: the isolated full pipeline creates a second
Event before, but merges the existing Event and preserves source ownership
after. Different-time and ambiguous same-slot controls pass after. Initial
fixture omitted the mandatory paired candidate_key and hit the DB invariant;
it was corrected before valid baseline evidence. An initial pipeline assertion
incorrectly assumed zero isolated JobOutbox rows despite the existing BGE
producer; removed that unrelated assertion. No production writes occurred.

Two real model-only controls using saved candidate and original-owner payloads
both return allow_merge with reason same_ticket_source_update.
Gemma failure reaches the configured Lite3.5 through the shared limiter and
existing ceiling; no retry or model-routing override. Controls do not call
Smart Update intake and write zero Event/Job rows. Disputed actual19099
consumer replay stays FINAL_RETRY, native ticket cross-binding regression
passes. Provider path audit PASS, unapproved0 and allowlisted_debt0.

Retained parser-ordinal-final-regression.log, parser-ordinal-final-baseline.log,
parser-ordinal-model-only-controls.json and parser-ordinal-19099-control.json.
Candidate is not merged, built or deployed yet. Ordinary TG11638 remains
active with real progress, so no cutover or competing CLI intake is permitted.
Existing duplicate pairs and nine own-digest cards have not been merged,
deleted, disabled or replayed. Existing public posts remain unchanged.


## Regression and release contract

Treat this record as a regression guard when changing parser occurrence keys,
explicit-key classification, source-anchor recall, shortlist selection or source
bindings. Root for the ordinal candidate is mechanical identity/recall; it does
not infer event meaning with title/date regexes. The separate own-digest venue
problem remains semantic and needs LLM-first source/child correction.

Required checks: original-function distinguishing baseline; ordinal drift full
SQLite pipeline; different-time, native-id, sector/cross-ticket and ambiguous
same-slot controls; actual19099 FINAL_RETRY; saved exact-owner model controls;
merged-main ancestry, full source qualification, initialized isolated preflight,
wrong-SHA control and safe Events-only release. Confirm normal scheduled parser
receipts after release, then reconcile old duplicate cards through the existing
managed merge/projection contract. Preserve legitimate multi-session cards.

Release evidence: not built or deployed. No public or data repair accepted.
Follow-ups: nine own-digest rows and source prevention, exact old-pair cleanup,
normal Telegram import acceptance and existing media/promo verification.


## 2026-11-04 22:57 UTC — merged-main candidate qualified, delivery pending

PR821 merged after all four required CI green (workflow41926319718). Exact
main834d5ea935aa53b9270e4d948b5aee0369e3d130 built as
runcoveer/events:intake-parser-ordinal-20261105, immutable
sha256:b49a6df0b3b2c7ea8d60950f8a0187fda5cd005c39f8c9eb713ce1238c252124.
Full4,444-file source proof, actual d148 ancestry and unchanged dependency
inputs passed. Initialized isolated runtime passed actual legacy ordinal recall
(10→15 and4→3), different-time/native countercontrols and inherited controls.
Wrong SHA refused before writes. HTTP ready=false/db=skipped establishes
isolation; own preflight container removed. Real model-only controls and
actual19099 replay from the unchanged candidate inputs remain valid.

NOT DELIVERED: normal scheduled TG11638 is still remote-scanning source56
kldevents;55 of57 sources done,74 messages. Last source transition22:51:03,
not a terminal acceptance or proof of continued per-post progress by itself.
Actual runtime remains healthy d148, no cutover attempt, intake interruption
or source replay. Staged exact cutover/verify scripts are under the candidate
release directory and require safe idle after remote AND local terminal.
Receipt parser-ordinal-qualification-receipt.json retained. Old duplicate pairs
and own-digest9 cards are not cleaned; ordinary parser receipt acceptance and
existing normal Telegram/media/promo controls remain outstanding.
