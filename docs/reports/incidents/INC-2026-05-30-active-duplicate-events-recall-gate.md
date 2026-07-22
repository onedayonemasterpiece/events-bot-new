# INC-2026-05-30-active-duplicate-events-recall-gate Active duplicate event cards because the dedup shortlist gates semantics behind exact venue/time/ticket anchors

Status: open
Severity: sev2
Service: `smart_event_update.py` create path — shortlist construction (`location`/`time` pre-filters) + `_pre_create_duplicate_probe` + `_llm_match_or_create_bundle`.
Opened: 2026-05-30
Closed: —
Owners: Smart Update / dup-probe owner / LLM-matching owner
Related incidents:
- `INC-2026-05-08-vk-tg-prompt-and-dup-probe.md` — owns `_pre_create_duplicate_probe` contract (this incident widens the recall that feeds it).
- `INC-2026-05-11-pre-create-dup-probe-missed-identical-ticket-merge.md` — branch-1 ticket-parity miss.
- `INC-2026-05-11-bar-bastion-stochastic-title-fallback-and-semantic-dup.md` — semantic dup with two ticket vendors + doors/start time skew (the `4486/5380` Скитальцы pair is still live).
- `INC-2026-05-09-event-location-alias-free-dup-regressions.md` — venue-alias dup class.
- `INC-2026-04-20-club-znakomstv-duplicate-event-cards.md` — merge-guard class.
Related docs: `docs/features/smart-event-update/README.md`, `docs/llm/request-guide.md` (LLM-first), `AGENTS.md` (LLM-first dup/match policy), `CHANGELOG.md`.

## Summary

A fresh production snapshot (`db_prod_fresh_20260530.sqlite`, pulled 2026-05-30 08:48 Europe/Kaliningrad, `max(event.added_at)=2026-05-30 08:19`) contains **27 true duplicate pairs** in the future window (`date >= 2026-05-30`, civic/generic recurring titles such as «День защиты детей» excluded). The defect is **live**: events `5446`, `5464`, `5496`, `5411` were created within the last 1–2 days. Operators are already cleaning up by hand (`lifecycle='duplicate'`, titles suffixed «(дубль)», e.g. `4659`/`5146` «Дачники (дубль)»).

Root cause is architectural, not another stochastic LLM lapse: the candidate shortlist that feeds **both** the LLM matcher (`_llm_match_or_create_bundle`) and the last-line `_pre_create_duplicate_probe` is built by hard structural pre-filters — date overlap, then **exact location match** ([smart_event_update.py:12112](../../../smart_event_update.py#L12112)), then **exact time-anchor equality** ([smart_event_update.py:12120](../../../smart_event_update.py#L12120)). When a genuine duplicate's venue string drifts (alias vs official name vs box-office vs ticket-vendor) or its time framing drifts (doors 19:00 vs start 20:00; matinee mislabel), the duplicate is removed from the shortlist **before any semantic comparison runs**. The LLM never sees it. This is a recall failure that no amount of probe/matcher tuning downstream can fix.

## User / Business Impact

The same event appears twice (or thrice) in `/daily`, the month/weekend pages, and as separate Telegraph cards. Buckets (fresh data, future window, civic-generic excluded):

| Bucket | Pairs | Mechanism |
|---|---|---|
| A. Time drift (doors/start/matinee) | 6 | time-anchor pre-filter drops the dup from shortlist |
| B. Venue-string variant | 9 | location pre-filter drops the dup from shortlist |
| C. Junk/prose `location_name` | 4 | `"театральный трамвай"`, `"Весь июнь каждый…"`, `"ООО «Уиандекс…»"` poison the location filter |
| D. Identical anchors, only title-wrapper differs | 8 | dup reaches probe but ticket-vendor / not-yet-canonical loc defeats it |

Representative live pairs:
- **A** `4769`/`4902` «Саша Ветров» @ Бар Бастион (19:00 doors vs 20:00 start), both VK.
- **A** `4690`/`5464` «Матч сборной России» @ Ростех Арена (20:00 vs 19:00).
- **B** `5018`/`5037` «Руки вверх» @ Бар Бастион vs @ Понарт (venue-string variant), parser vs VK.
- **C** `5105`/`5244` «VI Международный фестиваль…» @ `ООО «Уиандекс Му…»` (junk) vs @ Дом китобоя.
- **D / smoking gun** `5125`/`5126` «ПроСТО век Зацепина» — **identical** date `2026-06-21`, time `17:00`, `location_name`, address. Not merged because (1) two different ticket vendors (`domiskusstv.edinoepole.ru` vs `xn--39…рф`), defeating probe Branch 1; and (2) `location_name` was not yet canonicalised at probe time, so Branch 2's exact loc-string equality failed (both rows look identical only post-hoc in the snapshot).
- **D** `4486`/`5380` «Скитальцы / Беркут и Маврин» @ Бар Бастион — the pair already filed under INC-2026-05-11, still live.

## Detection

- 2026-05-30 operator request to investigate current production duplicates.
- Quantified from a freshly pulled prod snapshot (the prior local snapshot was 11 days stale and under-counted).
- No alert fired. Each create succeeded structurally; the dedup chain answered "no match" because the true sibling was never in the shortlist.
- 2026-07-17 date-listing visual review found a new «Эпидемия» recurrence:
  canonical `4671` at `20:00` and wrapper-title row `6859` at `19:00`. The newer
  row carries the same qTickets source already attached to `4671`; poster
  perceptual evidence is near-identical. This is the same wrapper + doors/start
  drift class, not a listing-layout defect.

### 2026-07-17 preview containment (not production remediation)

The immutable V10 listing preview uses a copied production snapshot and marks
`6859` duplicate/merged into `4671` only inside that review artifact. Evidence
is stored under
`artifacts/codex/listing-time-nav-media-v10-20260717/epidemia-*.json` and the
reconciled SQLite copy; artifacts are intentionally not committed. Production
was not changed by this UI task and still requires the official Smart Update /
operator merge flow. No UI title regex, fuzzy browser merge or permanent id
blacklist was introduced.

## Timeline

- 2026-05-08 .. 2026-05-11: prior dup incidents added ~15 narrow deterministic rescue branches to the matcher.
- 2026-04-12 .. 2026-05-30: affected duplicate rows imported across many days; newest (`54xx`) within the last 1–2 days.
- 2026-05-30 08:48: fresh prod snapshot pulled; 27 live dup pairs quantified and bucketed.
- 2026-05-30: this incident filed.

## Root Cause

1. **Recall is gated by exact structural anchors before semantics.** Shortlist construction filters date → exact `location_name` match → exact time-anchor equality. Buckets A+B+C (19/27 = 70%) are dups removed from the shortlist by these gates.
2. **`location_name` is matched as a raw string, not as a resolved canonical venue, at filter time.** Alias/official/box-office/ticket-vendor variants and post-hoc canonicalisation cause the same venue to read as different at decision time (bucket D smoking gun `5125/5126`).
3. **Doors-vs-start (and matinee) times are treated as a hard conflict** at shortlist time, not as a known same-event time skew (bucket A).
4. **Ticket-link parity is the only strong probe branch, but one event commonly has two ticket vendors** (artist-side vs venue-side; `qtickets` vs `edinoepole`), so Branch 1 silently misses (bucket D).
5. **Whack-a-mole of deterministic rescue branches** (~15 in `smart-event-update/README.md`) keeps the semantic decision in deterministic code, contrary to the `AGENTS.md` LLM-first dup/match policy. Each new variant escapes the hand-tuned anchors.
6. **Idempotency leak (separate but adjacent):** the same source post URL can attach to the original AND spawn a duplicate when two processings race (historical `wall-26560795_12503` → `4171`+`5024`).

## Contributing Factors

- The VK auto-import title writer systematically prepends decorative emoji + a generic `Спектакль/Концерт/Экскурсия «…»` wrapper (148 of 173 emoji-prefixed future titles are VK-sourced). This widens the string gap between the two siblings, though token-level `_titles_look_related` already tolerates the wrapper — so title normalisation alone does NOT fix the live dups (recall is the bottleneck, not relatedness).
- No probe/decision trace is persisted, so post-hoc the snapshot shows already-canonicalised fields, masking the at-decision-time mismatch.

## Automation Contract

### Treat as regression guard when

- changing shortlist construction in `smart_event_update.py` (the date/location/time pre-filter block immediately before the matcher);
- changing `_pre_create_duplicate_probe`, `_event_candidate_location_matches`, `_candidate_anchor_time`/`_event_anchor_time`/`_has_explicit_time_conflict`, or `_titles_look_related`;
- adding/altering any LLM dedup adjudicator over the shortlist;
- changing the create-path INSERT guard for `(source_type, source_url)` idempotency.

### Affected surfaces

- code: `smart_event_update.py` create path (shortlist + probe + matcher).
- data: the 27 live duplicate pairs enumerated from `db_prod_fresh_20260530.sqlite` (need merge / archive through the bot).

### Mandatory checks before closure or deploy

- unit tests proving: (a) same-canonical-venue siblings with drifted raw `location_name` survive into the shortlist; (b) doors/start time skew does not drop the sibling; (c) two-ticket-vendor same-slot siblings merge; (d) genuinely-distinct same-venue same-day events (matinee + evening, two different shows) are NOT collapsed;
- re-run the fresh-snapshot dup audit and confirm the bucket counts drop;
- regression checks for the related incidents above (probe branches must not regress).

## Fix Plan (LLM-first, agreed 2026-05-30; revised same day)

Initial agreement was order **3 → 5 → 1** (opt 4 title-emoji normalisation dropped — near-useless on current data; titles already match).

**Revision (2026-05-30, same day):** opt 3 as a *deterministic* recall-widen is **unsafe** and is folded into opt 1. Evidence: events `5426`/`5427` («Мастер-класс … «Овечка»», Гусевский музей, 11:00 and 13:00) come from the **same** Telegram post `t.me/gusmuseum/4509`, whose text announces two real sessions ("В 11:00 … В 13:00 …"). A deterministic recall-widen keyed on date + related title (ignoring venue/time) would merge these two legitimate sessions. The codebase already models this class via `_allow_parallel_events`. Distinguishing "one post → two sessions" from "two posts → one event duplicated" requires reading the source text — i.e. the LLM adjudicator, not a hand-tuned anchor window. Consequently the `27` heuristic pairs include a few legitimate multi-session/parallel splits; remediation must exclude them.

- **Opt 1 (the safe, load-bearing fix):** decouple recall from anchor gating — broaden the candidate recall by date+city + cheap title/venue blocking key (NOT gated by exact location/time), then a single LLM dedup adjudicator decides match/create over the full source text/posters/time. The prompt must: (a) treat doors-vs-start and venue-alias/box-office/ticket-vendor variants as NOT evidence of distinctness; (b) explicitly KEEP separate genuinely-distinct same-venue same-day events (matinee + evening, two sessions split from one post, two different shows). Per `AGENTS.md` Claude/Opus policy, the prompt-family + JSON schema for this stage should be designed via the `Opus` alias (`lollipop`-style small self-contained request, schema tightening).
- **Opt 5 (reassessed → folds into opt 1; no live victims):** a naive "block a source_url that is already attached to an event" guard is **wrong** — one source post legitimately maps to many events. Fresh-data evidence: `t.me/kldzoo/7436` → three distinct zoo activities at 11:00/11:30/13:30; `vk.com/wall-41284227_8374` → a weekly recurring kids program; `t.me/gusmuseum/4509` → two sessions of one masterclass. "One `source_url` → N events" is the normal multi-event extraction path, not a leak. The true historical leak (`wall-26560795_12503` → `4171`+`5024`, same event both merged and spawned) is a latent race with no current live victim in `db_prod_fresh_20260530.sqlite`. A correct guard must key on the extracted-event anchor signature, not on `source_url` alone — which is the same reasoning opt 1 does. Tracked as a latent-race follow-up; not implemented as a standalone deterministic guard.
- **Opt 4 (dropped):** title-emoji/wrapper normalisation — `_titles_look_related` already tolerates the VK writer's wrapper; recall, not relatedness, is the bottleneck.

## Remediation

Curated from `db_prod_fresh_20260530.sqlite` (future window, civic-generic excluded). **26 true-duplicate pairs** (all from DISTINCT source posts — genuine cross-source reposts of one event) are merge candidates; **1 pair is KEEP** (`5426`/`5427`, legitimate two-session split from one `t.me/gusmuseum/4509` post). Merge MUST go through the bot's merge flow (re-runs Smart Update merge + operator visibility), NOT blind SQL, because a few pairs need a same-event judgment call, e.g. `3722`/`5359` «Золотой ключик» (Драматический театр 17:00 vs Театр кукол 12:00 — possibly two different productions, do not auto-merge).

Merge candidates (canonical id ← duplicate id, verify each before merge):
- `4769`/`4902` Саша Ветров (doors/start); `5433`/`5437` «Вот это драма!»; `5441`/`5496` зоопарк-экскурсия; `5377`/`5432` DeLight Project; `5402`/`5446`/`5431` «Газеты Пишут» (triple, one bad time); `5434`/`5438` «Исцеление»; `4812`/`4835`/`5215` «Путешествие налегке» (triple); `3772`/`4213` «Цветы России»; `5018`/`5037` «Руки вверх»; `4690`/`5464` Матч сборной; `5105`/`5244` Pianissimo-фестиваль; `4772`/`5404` «Капитанская дочка»; `4978`/`5010`, `4979`/`5011`, `4980`/`5012`, `4981`/`5013` Закулисье театра (recurring series, 4 dates); `5125`/`5126` ПроСТО век Зацепина; `5202`/`5411` Pianissimo: Папоян; `4671`/`4794` Эпидемия; `4486`/`5380` Скитальцы/Беркут&Маврин; `4903`/`4961` Балтийский леторуб.
- Needs judgment (do NOT auto-merge): `3722`/`5359` «Золотой ключик» (two venues, possibly two productions).
- KEEP (not a dup): `5426`/`5427` «Овечка» masterclass — two real sessions from one post.
