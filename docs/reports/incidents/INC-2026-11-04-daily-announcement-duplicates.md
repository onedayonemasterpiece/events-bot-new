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
