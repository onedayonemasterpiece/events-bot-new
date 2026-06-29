# Event Reference Enrichment (справочное дообогащение события)

> **Status:** design / implementation contract, **not provider-first**  
> **Caller:** Smart Update (`smart_event_update.py`) after source-role validation, sparse safety and writer coverage checks.  
> **Primary goal:** first stop unsupported sparse/giveaway descriptions; only then add at most one safe reference sentence for already valid sparse events.

## P0: do not implement Wikimedia provider first

Implementation **must not** start with `Wikimedia client -> entity lookup -> add sentence`.

The first deliverable is **Sparse Event Safety**:

1. classify source role;
2. block giveaway/promo-only sources as `needs_event_source` unless an independent event-specific source exists;
3. separate event facts from reference/promo/giveaway facts;
4. classify sparse/rich using event-specific **content** facts, not date/time/place anchors;
5. generate dry sparse text for sparse events;
6. run writer coverage check by claim class;
7. remove unsupported existing public claims on rerender;
8. pass the SQWOZ BAB / event `6501` regression.

`WIKIMEDIA_REFERENCE_ENRICHMENT` may be enabled only after Sparse Event Safety passes.

## Original regression this must prevent

The SQWOZ BAB Telegraph page (`event_id=6501`) was created from a sparse giveaway-like source that had only basic anchors: artist, date, time and venue. Public text still gained unsupported event-specific claims such as:

- `хиты`;
- `сет-лист`;
- `новые композиции` / `свежий материал`;
- `живое исполнение`;
- `программа выступления`.

Before adding Wikimedia provider code, the implementation must identify and patch the Smart Update path that allowed those claims despite the existing fact-first policy.

Required regression:

```text
Given the original sparse/giveaway SQWOZ BAB source for event 6501,
Smart Update must not produce programme/set-list/hits/new-compositions/live-band claims
unless those claims are present in event_source facts.
```

## Product invariant

Reference enrichment is allowed only after Smart Update has already decided that the event itself is valid from event-specific sources.

- **Event sources** (`source.role=event`) confirm the event: official artist/venue/ticket/organizer page, direct organizer announcement, or trusted aggregator page for this concrete event.
- **Reference sources** (`source.role=reference`) explain an entity mentioned in the event: in v1 only artist/band/musical collective/person.
- **Giveaway/promo/repost sources** may provide weak candidate anchors, but cannot by themselves validate publication.

Hard rule:

> Wikipedia/Wikidata can answer “who/what is this entity?”. They cannot answer “does this exact event happen?” and cannot justify publication of an otherwise weak event.

## Required implementation order

This order is mandatory, not advisory:

```text
1. classify source_role
2. classify event_status
3. block giveaway/promo-only without independent event_source
4. extract event facts with claim_class and render_policy
5. classify sparse/rich from event-specific content facts
6. generate dry sparse text for publish_sparse
7. coverage-check every public sentence by claim_class/source_role
8. remove unsupported existing description claims on rerender
9. only then run reference enrichment for eligible publish_sparse events
10. include enrichment/safety decisions in the Smart Update bot report
```

`WIKIMEDIA_REFERENCE_ENRICHMENT=1` must not be enabled unless all are implemented and enabled:

- `STRICT_SPARSE_WRITER=1`;
- `GIVEAWAY_REQUIRES_EVENT_SOURCE=1`;
- source-role separation;
- claim-class writer coverage check;
- SQWOZ BAB regression fixture.

## Processing statuses

| Status | Meaning | Writer behaviour | Reference enrichment |
| --- | --- | --- | --- |
| `publish_rich` | Valid event source and enough event-specific content facts. | Normal fact-first description from event facts. | Off by default. |
| `publish_sparse` | Valid event source, but description is empty/poor/generic. | Short dry description; may add at most one safe inline reference sentence. | Allowed if all gates pass. |
| `needs_event_source` | Giveaway/promo/repost/unclear source needs an independent event source. | Do not auto-publish a full public card. Existing unsupported public text must be removed if rerendering. | Forbidden. |
| `skip` | Not a valid event or required anchors missing. | Do not create/update public event. | Forbidden. |

## v1 scope

Strict v1 scope:

- `event_type=концерт` only;
- entity type: `artist`, `band`, `musical_collective`, `person` only;
- public facts: `identity` and `occupation` only;
- public rendering: at most one inline sentence, no separate block.

Out of scope for v1:

- films, books, exhibitions, lectures, theatre, festivals;
- multi-artist line-ups;
- tribute/cover/symphonic “music of X” shows where the original artist is not the performer;
- genre/origin/discography/albums/popular tracks/biography paragraphs;
- using Wikipedia prose/extract in public text without visible attribution.

## Source-role classification

### Source roles

`source.role` must be one of:

- `event` — event-specific confirmation;
- `reference` — entity reference only;
- `giveaway` — contest/prize/repost mechanics;
- `promo` — generic promo/discount/channel promotion;
- `repost` — repost/copy signal without enough authority;
- `unclear` — not enough evidence.

A source is `event` only if it directly asserts that this concrete event happens and is one of:

- official artist page/site/social account;
- official venue page/site/social account;
- ticket operator page for this concrete event;
- organizer announcement;
- trusted aggregator page for this concrete event.

A source is `giveaway` if its primary purpose is contest/prize/repost mechanics. A giveaway source may contribute candidate anchors, but cannot validate publication unless there is at least one independent `event` source.

If one source contains both giveaway mechanics and direct event assertion, classify facts separately:

- giveaway mechanics -> `source.role=giveaway`, `fact.scope=giveaway_fact`, `render_policy=never_render`;
- date/time/venue/title from a non-authoritative giveaway post -> `claim_class=weak_event_signal`;
- date/time/venue/title from official venue/organizer/artist post -> may be `event_anchor` with `source.role=event`.

## Event validity score caps

`event_validity_score` is not a free LLM confidence. The LLM may lower a score, but must not raise it above the source-role cap.

| Source evidence | Max score |
| --- | ---: |
| Official artist/venue/ticket concrete event page | `1.00` |
| Trusted aggregator concrete event page | `0.90` |
| Direct organizer social post | `0.85` |
| Local unofficial direct announcement | `0.70` |
| Repost/copy signal | `0.60` |
| Giveaway/prize source without independent event source | `0.55` |
| Unclear mention | `0.40` |

Auto-publication threshold: `event_validity_score >= 0.75` and required anchors present. Therefore giveaway/repost/local-unofficial sources cannot become `publish_sparse` through LLM optimism alone.

## Event-specific content facts

Do **not** count as event-specific content facts:

- artist/title;
- date;
- time;
- venue;
- city;
- price;
- ticket link;
- age limit;
- source URL;
- poster URL.

These are anchors/logistics, not content richness.

Do count as event-specific content facts only when supported by `source.role=event`:

- named programme/tour;
- confirmed set-list/works/tracks to be performed;
- confirmed live band/orchestra/guest participation;
- album presentation tied to this event;
- format details from the event source;
- event-specific programme blocks or schedule items.

`publish_sparse` means there are enough anchors to publish but `event_specific_content_fact_count <= 1` or source description quality is `empty|poor|generic`.

## Required persistence contract

Do not store reference facts as indistinguishable ordinary event facts.

Minimum schema contract (physical implementation may extend the existing tables, but these fields must be queryable and used by renderer/report/coverage):

### Source fields

```text
event_source.role              enum(event, reference, giveaway, promo, repost, unclear)
event_source.provider          text nullable -- e.g. wikidata, wikipedia, vk, telegram, parser
event_source.wikidata_qid      text nullable
event_source.page_title        text nullable
event_source.page_id           text nullable
event_source.revision_id       text nullable
event_source.fetched_at        timestamp nullable
event_source.entity_match_confidence real nullable
event_source.publicly_counted_as_event_source boolean default true
```

For `role=reference`, `publicly_counted_as_event_source=false` is mandatory.

### Fact fields

```text
event_source_fact.scope         enum(event_fact, entity_reference, giveaway_fact, promo_fact, weak_event_signal)
event_source_fact.claim_class   enum(
  event_anchor,
  event_programme,
  event_logistics,
  event_ticketing,
  performer_presence,
  entity_identity,
  entity_occupation,
  giveaway_mechanics,
  promo_noise,
  weak_event_signal
)
event_source_fact.render_policy enum(public_allowed, admin_only, never_render)
event_source_fact.source_role   enum(event, reference, giveaway, promo, repost, unclear)
event_source_fact.confidence    real nullable
```

Core invariants:

- A sentence with `claim_class=event_programme` must be supported only by facts whose `source_role=event`.
- A sentence with `source_role=reference` cannot support event programme, date, time, venue, price, ticket, line-up, performer-presence, or “what will happen on stage” claims.
- `source.role=reference` cannot contribute to public `Источников: N`.

## Writer coverage check

Every public sentence must be coverage-checked by claim class, not just by “some fact id exists”.

Example reject payload:

```json
{
  "sentence": "На концерте прозвучат хиты SQWOZ BAB.",
  "claim_classes": ["event_programme", "setlist_or_track_promise"],
  "required_source_role": "event",
  "provided_sources": ["reference"],
  "verdict": "reject",
  "reason": "reference facts cannot support programme/set-list claims"
}
```

For sparse events, reject any sentence implying:

- what will be performed;
- what programme is planned;
- whether tracks are new/old/popular;
- whether there will be live band/orchestra/guests/surprises;
- whether the event is part of a tour/presentation;
- whether the performer will present a named album or special material;

unless supported by `source.role=event` facts with a matching claim class.

Denylist terms are a safety net, not the only guard:

- `хиты`, `сет-лист`, `новые композиции`, `свежий материал`;
- `живое исполнение`, `специальная программа`, `сюрпризы`, `гости`;
- `презентация альбома`, `туровая программа`, `live-бэнд`.

## Existing bad-description remediation

When Smart Update reruns on an existing event, unsupported existing public claims must be removed, not preserved.

Regression:

```text
Given an existing description that contains unsupported programme claims,
and source facts contain only artist/date/time/venue,
after Smart Update rerender the description becomes dry sparse text
plus optional one identity/occupation reference sentence.
```

This applies to event `6501` and to future repairs of the same defect family.

## Wikimedia provider v1

### Public data source rule

Public renderable reference facts in v1 may come only from **Wikidata structured data** or other CC0/structured provider fields approved in code.

Wikipedia page/search may be used for:

- search/disambiguation;
- page title and sitelink;
- QID resolution;
- revision/page metadata;
- admin provenance.

Wikipedia prose/extract must not be rendered publicly or closely paraphrased unless visible attribution and license-compatible links are implemented. The v1 path avoids this by rendering only structured identity/occupation facts.

Allowed v1 fact types:

- `identity` — e.g. stage name / real name relation;
- `occupation` — e.g. rapper, musician, actor, writer.

Forbidden v1 facts:

- `genre`, `origin`, discography, albums, popular tracks, biography paragraphs;
- “known for” claims;
- personal life, scandals, ratings;
- any fact that can sound like a promise about the current event programme.

### Entity confidence caps

`entity_match_confidence` must combine deterministic evidence with LLM adjudication, but final confidence cannot exceed deterministic caps.

| Evidence | Max confidence |
| --- | ---: |
| Exact normalized title/alias + Wikidata QID + matching occupation/person/band | `0.98` |
| Exact normalized title + ruwiki sitelink but weak structured type | `0.85` |
| Exact title only, no QID | `0.80` |
| Multiple plausible search results | `0.75` |
| Fuzzy title match | `0.70` |
| Generic title | `0.60` |
| Tribute/cover context | `0.50` |

If deterministic cap `< 0.90`, reference facts are not renderable even if the LLM says `0.95`.

## Public rendering v1

For reference enrichment v1:

- no separate `Справка` block;
- no `Об артисте` block;
- no heading generated from reference facts;
- no bullet list of reference facts;
- no public Wikipedia label/link;
- no change to title, short title, event type, date, time, venue, city, price, ticket CTA;
- at most one inline sentence from `entity_reference` facts.

Acceptance check: rendered Telegraph body contains `0` reference-specific headings and at most `1` sentence sourced from `entity_reference` facts.

## Smart Update report contract

The operator/admin bot report must expose both positive enrichment and anti-garbage decisions.

### Accepted/used reference facts

```text
Справочное обогащение: Wikidata/Wikipedia — Sqwoz Bab / Q106541067, +2 факта, confidence 0.93
```

Use wording `Справочное обогащение`, never `Источник события`.

### Blocked by source role

```text
Справочное обогащение: заблокировано — needs_event_source; reason=giveaway_only_without_event_source
```

### Unsupported claims removed

```text
Sparse safety: удалены неподтверждённые claims: хиты, новые композиции, живое исполнение
```

### Lookup not attempted

In debug/operator mode:

```text
Справочное обогащение: не запускалось — event_status=publish_rich / source already rich
Справочное обогащение: не запускалось — ambiguous entity
```

## Provider failure behaviour

- Per Wikimedia call timeout: `2–3s`.
- Total enrichment budget in Smart Update hot path: `<=5s`.
- No provider call unless `event_status=publish_sparse` and Sparse Event Safety passed.
- No retry on 4xx.
- At most one retry on transient timeout/5xx.
- Negative cache no-match / ambiguous match.
- All provider exceptions become `enrichment_status=failed_soft` and must not fail Smart Update.
- User-Agent must contain project name and contact; default library agents are forbidden.
- Cache QID/page/revision responses where possible.

## Rollout flags

Required flags:

- `STRICT_SPARSE_WRITER=1` — block unsupported sparse claims;
- `GIVEAWAY_REQUIRES_EVENT_SOURCE=1` — block giveaway-only auto-publication;
- `EVENT_REFERENCE_ENRICHMENT=1` — master reference enrichment switch;
- `WIKIMEDIA_REFERENCE_ENRICHMENT=1` — Wikimedia provider.

`WIKIMEDIA_REFERENCE_ENRICHMENT` depends on the first two flags and must fail closed when they are off.

## Test matrix / acceptance

Minimum tests before implementation is accepted:

### T1. SQWOZ BAB giveaway-only source

Expected:

- `event_status=needs_event_source`;
- Wikimedia not called;
- full public writer not called for a new full card;
- no Telegraph page created, or existing bad text is removed on rerender;
- no `хиты`, `сет-лист`, `новые композиции`, `свежий материал`, `живое исполнение`, `программа выступления`.

### T2. SQWOZ BAB valid sparse event_source

Expected:

- `event_status=publish_sparse`;
- Wikimedia/Wikidata may be called;
- public text has event anchors plus at most one identity/occupation sentence;
- no programme/set-list/hits/new-compositions/live-band claims.

### T3. SQWOZ BAB rich event_source

Expected:

- `event_status=publish_rich`;
- reference enrichment off by default;
- programme claims allowed only when extracted from `source.role=event`.

### T4. Ambiguous entity: `Кино` / `Мираж`

Expected:

- deterministic confidence cap `<0.90` or adjudicator rejects;
- no reference facts rendered.

### T5. Tribute / cover / symphonic show

Expected:

- original artist is not enriched as performer;
- no sentence implies original artist participates.

### T6. Multi-artist festival

Expected:

- no per-artist enrichment in v1.

### T7. Wikimedia timeout / 5xx

Expected:

- dry sparse event;
- Smart Update succeeds;
- provider failure is `failed_soft`;
- optional operator/debug report note.

### T8. Existing bad description remediation

Expected:

- unsupported existing claims removed on rerender;
- final text is dry sparse + optional one identity/occupation sentence.

## Definition of done

Implementation is not done until:

1. Sparse Event Safety is implemented and enabled;
2. SQWOZ BAB/event `6501` replay passes through the production import boundary or a faithful shadow equivalent;
3. coverage-check rejects semantic programme claims, not just denylist words;
4. source/fact role fields are persisted and used by renderer/report/coverage;
5. public source count excludes reference sources;
6. Smart Update report includes enrichment and blocked/removed-claim decisions;
7. Wikimedia provider uses structured Wikidata facts only for public rendering in v1;
8. all tests in the matrix pass.
