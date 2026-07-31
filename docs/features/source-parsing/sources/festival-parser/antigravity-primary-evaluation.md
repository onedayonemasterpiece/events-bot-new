# Antigravity-primary festival evaluation cohort

Status: `design-only dated acceptance input`; cutoff `2026-07-31`. This document
does not run Antigravity, mutate `festival_queue` or create Festival/Event rows.

Purpose: ground the future Antigravity implementation in current non-social
queue data, all seven discovery topologies and explicit child Event decisions.
Canonical contracts:

- [runtime project](preproduction-web-research.md);
- [taxonomy/data/Event model](../../../festivals/data-model-v2.md);
- [prompt/checkpoint pack](../../../../llm/antigravity-festival-research.md).

## 1. Freshness and selection

Read-only production SQLite result:

```text
festival_queue total pending = 1318
tg = 536
vk = 694
url = 88
current defensible url rows at cutoff = 31
likely grouped edition targets = 22
```

The selector uses event/edition dates, not queue timestamps:

```text
status = pending
source_kind = url
one unambiguous single date or interval
effective end >= 2026-07-31
```

For `external_batch`, use the explicit `signals_json.period`. Otherwise parse
normalized `Дата`/`Дата окончания` evidence from `source_text`. Rows with
ambiguous alternative periods go to review/exclusion. Current date does not
prove `identity_kind=festival`.

Excluded from automatic cohort:

- ID `989`, «80 историй о главном»: primary interval ended `2026-07-19`, while
  `or 2026-09-01` is not a defensible interval end; keep as identity/currentness
  regression fixture only.

## 2. Current non-social queue groups

Expected labels below are **review hypotheses**, not facts copied into prompts.
Antigravity A/B must derive them independently from current evidence.

| Group | Queue IDs | Date clue | Seed host(s) | Expected review focus / topology hypothesis |
|---|---|---|---|---|
| Территория мира — Территория музыки | 18, 19, 20, 1000, 1291 | 28–30 Aug | `tickets.sobor-kaliningrad.ru`, `visit-kaliningrad.ru` | `lineup`; group day-ticket pages into one edition; concert/day blocks may be Events, never each performer |
| Третий международный фестиваль органистов | 128 | 4 Sep | `sobor39.ru` | identity/edition boundary; `series_season` vs `lineup` must be evidenced |
| Pianissimo | 471–474 | 31 Jul; 6, 7, 14 Aug | `kaliningrad.tretyakovgallery.ru` | `series_season`; separately actionable concerts expected as child Events |
| СовершенноЛетние концерты | 619 | 23 Aug | `tickets.sobor-kaliningrad.ru` | decide child source vs festival series; do not create a duplicate edition/event |
| LAGUNA BEACH | 688 | 31 Jul–2 Aug | `kaliningrad.qtickets.events` | festival identity and `lineup|territory`; ticket scope |
| Море светлой любви | 988 | through 31 Aug | `visit-kaliningrad.ru` | possible non-festival/continuous programme; adversarial identity/topology |
| Kaliningrad City Jazz | 995 | 31 Jul–2 Aug | `jazzfestival.ru` | `lineup`; days/concert products vs performer schedule slots |
| Гроздь | 996, 1262 | 1–2 Aug | official domain, `янтарьхолл.рф` | `market`; catalogue participants/products, supporting stage not dominant |
| Большой Кауп | 997 | 8–9 Aug | `visit-kaliningrad.ru` | `territory`; all-day zones vs temporal anchors |
| Море внутри | 998 | 8–9 Aug | `visit-kaliningrad.ru` | `route_promenade/free_promenade`; route points/permanent objects are not Events |
| Короче | 999 | 18–23 Aug | `korochekino.ru` | `grid_showcase`; sessions/blocks may be Events, films/works nested |
| Kaliningrad Street Food | 1001 | 4–6 Sep | `visit-kaliningrad.ru` | `market`; participants/products not Events, separately booked masterclasses may be |
| ВитаЛики | 1002 | 6 Sep | `marafonbards.ru` | `lineup` hypothesis and source authority/currentness |
| Водная ассамблея | 1003 | 12–13 Sep | `visit-kaliningrad.ru` | `territory`; parade/show anchors vs zones/gastro/service |
| Балтийские сезоны | 1004 | 6–7 Oct | `dramteatr39.ru` | `grid_showcase|series_season`; performance products and edition identity |
| Шедевры мировой классики | 1005 | 1 Oct–29 Nov | `visit-kaliningrad.ru` | `series_season`; independent concerts expected |
| Самайн | 1006 | 31 Oct | `visit-kaliningrad.ru` | festival vs holiday programme and `territory`; avoid word-based acceptance |
| Острова | 1007 | 24 Oct–4 Nov | `detivmuzee.ru` | `network_pass`; institutions are not Events; exact-time programmes may be |
| Джаз в Филармонии | 1008 | 13–18 Nov | `visit-kaliningrad.ru` | `lineup` vs series; individual concert ticket scope |
| ЛАФ child source | 1201 | 8 Aug | `kaliningrad.tretyakovgallery.ru` | child event/subject relation; must not become a second festival edition |
| Maggots Fest | 1257 | 14–15 Aug | `kaliningrad.qtickets.events` | festival identity, `lineup`, ticket product scope |
| Три кота: День Варенья | 1260, 1261 | 11 Oct, two sessions | `domiskusstv.edinoepole.ru` | expected `not_festival`/ordinary repeated performance; critical false-positive test |

The complete cohort covers every topology hypothesis:

```text
series_season    Pianissimo, Шедевры
lineup           Территория мира, City Jazz, Джаз в Филармонии
grid_showcase    Короче, Балтийские сезоны
territory        Большой Кауп, Водная ассамблея
market           Гроздь, Street Food
route_promenade  Море внутри
network_pass     Острова
```

## 3. Added case: «Балтийская Ухана»

### Source identity

This is «Балтийская Ухана», the author-song/bard festival, not the Pionersky
Fisherman's Day article about “балтийская уха”. Current non-social sources:

- official home and programme entry: <https://uhana.ru/>;
- official 2026 one-page programme PDF:
  <https://uhana.ru/wp-content/uploads/2026/07/uhana2026.pdf>;
- official directions: <https://uhana.ru/howtoget/>;
- official contest details: <https://uhana.ru/contest/>;
- official registration terms: <https://uhana.ru/registration/>;
- official guests: <https://uhana.ru/guests/>;
- regional TIC confirmation:
  <https://visit-kaliningrad.ru/events/festivali/Baltiyskaya_Ukhana/>.

Do not open Telegram/VK from this contour; they may be retained only as contact
links discovered on the official website.

### Accepted current facts for a reviewed fixture

- edition dates: `2026-08-07..2026-08-09`;
- place: festival field on the Baltic coast near Pavlovo; current official
  coordinates `54.717467, 19.941529`;
- 6+ is supported by regional TIC;
- three-day tent-camp author-song festival with concerts, competition,
  workshops, camp singing, volleyball and communal fish soup;
- current contact is festival committee/Andrey Lazakovich; historical 2024
  institutional organizers must not be asserted unchanged for 2026;
- registration is explicitly voluntary and free; this does **not** by itself
  prove a blanket admission-price fact, so `is_free` remains unknown unless a
  direct admission statement is found.

Do not infer an edition ordinal. `2024=37th` plus arithmetic is not current 2026
official evidence.

### Expected classification hypothesis

```yaml
identity_kind: festival
primary_topology: territory
secondary_topologies: [lineup]
discovery_unit: zone       # common camp/territory experience; operator to gold-review
route_subtype: null
time_mode: continuous_with_anchors
space_mode: bounded_site
access_mode: [unknown]     # voluntary free registration is not blanket admission proof
program_mechanics: [competition, all_day_activities]
data_completeness: conflicting
programme_structure: continuous_experience
```

`continuous_experience` is intentional: the common camp/territory experience
contains timed anchors but no expected Event-linked programme item. Timed
schedule slots alone do not make the structure `hybrid`.

`data_completeness=conflicting` is required because the official PDF and contest
page disagree on 8 August times:

| Subject | PDF | Contest page |
|---|---|---|
| first round | 13:00–15:00 | 12:00 |
| second round | 16:30–17:30 | 17:00 |

Do not silently choose by crawl date. Preserve both claims and require an
explicit current-source authority/update decision.

### Expected subject dispositions

| Subject | Entity role | Expected disposition | Why no child Event |
|---|---|---|---|
| arrival/camp placement | service information | `service_information` | logistics |
| festival registration | service information | `service_information` | general/voluntary festival registration |
| history photo exhibition | activity/zone | `continuous_activity` | long open interval, no independent action |
| camp setup/free communication | activity/zone | `programme_only` | no standalone identity/action |
| «Балтийская акустика» concert block | programme block/temporal anchor | `schedule_slot` | shared festival context, no item-specific booking/detail action |
| camp concerts «Гитара по кругу» | programme block | `programme_only` or `schedule_slot` | informal shared camp activity |
| volleyball | activity/zone | `continuous_activity` | internal festival activity, not separately actionable public Event |
| contest applications | service information | `service_information` | application logistics |
| children’s concert | programme block | `schedule_slot` | exact time but no independent ticket/registration/detail action |
| contest rounds | programme blocks | `schedule_slot` | competition stages within common festival; conflicting times |
| guest/laureate concert | programme block | `schedule_slot` | no separate action/product |
| creative workshops | programme block | `programme_only` or `schedule_slot` | generic block; advance registration only recommended at festival scope |
| Neptune/fish soup | temporal anchor | `schedule_slot` | shared culmination |
| awards/gala | programme block/anchor | `schedule_slot` | common closing block, no separate action |
| Sunday swimming/cleanup | activity/service | `programme_only` / `service_information` | no fixed independent occurrence |

Expected child Event candidates from the published festival programme: **0**.
This deliberately tests that exact time + named concert is not enough without
independent public choice/action and item-specific scope.

The separate 4–5 August architecture workshop teaser is not part of the
7–9 August programme and could be an ordinary separate Event, but the linked
article is incomplete/unavailable. Expected disposition:
`unresolved/needs_source`; do not create from the teaser.

The edition itself is represented by the festival detail/index data and is not
automatically duplicated into Event. An umbrella compatibility Event would
require a separate reviewed product decision.

## 4. Future execution order

When implementation is separately authorized, do not spend the whole quota on
the cohort. Proposed order:

1. offline replay of saved/reference sources;
2. one manual group covering multi-URL grouping (`Территория мира`);
3. «Балтийская Ухана» for territory + conflicting schedule + zero child Events;
4. one clear child-Event series (`Pianissimo`);
5. one catalogue non-Event case (`Гроздь` or Street Food);
6. one network/pass case (`Острова`);
7. one negative identity case (`Три кота`).

Typical A+B means two calls/group; C only for a locally valid critical conflict.
Unchanged fingerprints consume no calls. The full 22-group cohort is used only
after the five-to-seven diverse cases satisfy the checkpoint and precision
gates.

## 5. Golden-review checklist

For every group the reviewer must label:

```text
current edition identity
identity_kind
primary/secondary topology + discovery unit
route subtype when applicable
programme structure
complete source-grounded subject inventory
entity role and disposition per subject
child Event yes/no with every normative gate
source/offer scope conflicts
unknowns and required evidence
```

A model output is not gold. Golden labels are versioned operator decisions with
source snapshots/hashes; changed source content creates a new review revision.
