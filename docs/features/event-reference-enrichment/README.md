# Event Reference Enrichment (справочное дообогащение события)

> **Status:** design / rollout contract
> **Caller:** Smart Update (`smart_event_update.py`) after event validation and source fact extraction, before public text writer.
> **Primary goal:** improve sparse but valid event cards with short, provenance-tracked reference facts without inventing event-specific programme details.

## Product rule

Reference enrichment is allowed only after Smart Update has already decided that the event itself is valid from event-specific sources.

- **Event-specific sources** (`event_source`) confirm the event: official site, venue page, ticket page, trusted aggregator, direct organizer/source announcement.
- **Reference sources** (`reference_source`) explain an entity mentioned in the event: artist, band, person, film, book, exhibition project.
- A reference source **must not** confirm that the event happens, raise event validity, or rescue a giveaway/promo-only source.

Hard invariant:

> Wikipedia/Wikidata can answer “who/what is this entity?”. They cannot answer “does this exact event happen?” and cannot justify publication of an otherwise weak event.

## Smart Update integration point

Smart Update calls this feature only on the create/merge path where all of the following are already available:

1. parsed candidate anchors (`title`, `date`, `time`, venue/city, event type);
2. source-role / source-quality assessment;
3. `facts_text_clean` or equivalent source-grounded facts;
4. sparse-writer decision (`publish_rich`, `publish_sparse`, `needs_event_source`, `skip`).

Suggested order:

```text
source candidate
  -> event validation / source role gate
  -> source fact extraction
  -> sparse/rich classification
  -> Event Reference Enrichment (only for publish_sparse)
  -> fact-first writer + coverage check
  -> Smart Update operator report
```

Reference facts flow into the writer as a separate scope (`entity_reference`) and must be distinguishable from event facts during coverage checks.

## Processing statuses

| Status | Meaning | Writer behaviour | Reference enrichment |
| --- | --- | --- | --- |
| `publish_rich` | Event source is valid and has enough event-specific content facts. | Normal fact-first description from event facts. | Off by default; reference facts add little value. |
| `publish_sparse` | Event source is valid, but description is empty/poor/generic. | Short, dry event description; may add one safe inline reference fact. | Allowed if all gates pass. |
| `needs_event_source` | Giveaway/promo/repost/unclear source needs a second event-specific source. | Do not auto-publish a full event card. | Forbidden. |
| `skip` | Not a valid event or missing required anchors. | Do not create/update public event. | Forbidden. |

## Source roles

The feature requires role separation in the source ledger:

- `event_source` — event-specific confirmation and facts; counted in public “Источников: N”.
- `reference_source` — encyclopedia/reference metadata about an entity; **not** counted in public “Источников: N”.
- `promo_source` / `giveaway_source` — promo/contest signal; cannot by itself validate publication.

If a source has multiple roles, store or report the role at the fact/source level so the writer can prove every public sentence against the right scope.

## Wikipedia/Wikidata provider v1

Wikipedia/Wikidata is the first planned provider. It is intentionally small:

- scope MVP: `event_type=концерт` and entity types `artist`, `band`, `musical_collective`, `person`;
- output: maximum **one** short inline reference sentence in public copy;
- fact types allowed:
  - `identity` — real name / stage name relation;
  - `occupation` — rapper, musician, actor, writer, etc.;
  - `genre` — broad stable genre/category when not likely to imply event programme;
  - `origin` only when low-risk and useful.

Forbidden in MVP:

- discography, current albums, “popular tracks”, ratings, scandals, personal life;
- claims about set list, guests, live band, programme, “new compositions”, surprises, album presentation;
- facts that could be read as event-specific promises unless confirmed by an `event_source`.

Example for a sparse but valid SQWOZ BAB concert:

```text
2 июля в 19:00 во Дворце спорта «Янтарный» состоится концерт SQWOZ BAB. SQWOZ BAB — сценический псевдоним Марата Мингазова, российского комедийного рэпера.
```

This is allowed only if the event is already validated by an event-specific source. It does **not** allow claims such as “сет-лист”, “популярные хиты”, “новые композиции”, or “живое исполнение” unless those claims exist in event-specific facts.

## Wikipedia lookup gates

Run Wikipedia/Wikidata lookup only when **all** conditions are true:

```json
{
  "event_status": "publish_sparse",
  "event_validity_score_min": 0.75,
  "source_is_giveaway_only": false,
  "source_description_quality": ["empty", "poor", "generic"],
  "event_specific_fact_count_max": 1,
  "entity_extraction_confidence_min": 0.85,
  "lookup_entity_type": ["artist", "band", "musical_collective", "person"],
  "mvp_event_type": "concert"
}
```

Stop immediately if any flag is true:

- `source_is_giveaway_only=true`;
- `source_role in [contest, repost, prize, promo_only]` without a second event-specific source;
- `event_validity_score < 0.75`;
- required event anchors are missing;
- entity is generic or ambiguous;
- event is a tribute/cover/symphonic “music of X” show where the original artist is not the performer;
- event is a multi-artist festival/line-up card;
- source is already rich.

If lookup runs but `entity_match_confidence < 0.90`, discard reference facts and continue with dry `publish_sparse` text.

## LLM gate schema

The LLM owns semantic classification, but deterministic checks should guard obvious failures.

### 1. Event validity / source role

```json
{
  "does_source_assert_event_happens": true,
  "source_role": "direct_announcement | ticket_page | venue_page | artist_site | aggregator | giveaway | repost | unclear",
  "is_giveaway_or_promo_only": false,
  "required_fields_present": {
    "event_name_or_subject": true,
    "date": true,
    "time": true,
    "venue": true,
    "city": true
  },
  "event_validity_score": 0.0,
  "event_status": "publish_rich | publish_sparse | needs_event_source | skip",
  "reason": "..."
}
```

Rule: if `source_role=giveaway` and there is no second event-specific source, set `event_status=needs_event_source`; Wikipedia lookup stays blocked even when the entity is confidently found.

### 2. Source richness

```json
{
  "core_facts": ["кто выступает", "дата", "время", "место"],
  "event_specific_facts": [],
  "event_specific_fact_count": 0,
  "description_quality": "empty | poor | normal | rich",
  "unsupported_claims_forbidden": [
    "сет-лист",
    "новые композиции",
    "хиты",
    "презентация альбома",
    "live-бэнд",
    "гости",
    "специальная программа"
  ],
  "should_try_reference_lookup": true
}
```

### 3. Entity match and allowed facts

```json
{
  "lookup_entity": "SQWOZ BAB",
  "lookup_entity_type": "artist",
  "candidate_wikipedia_title": "Sqwoz Bab",
  "candidate_wikidata_id": "Q106541067",
  "title_or_alias_match": true,
  "entity_type_match": true,
  "context_match": true,
  "is_disambiguation": false,
  "is_tribute_risk": false,
  "entity_match_confidence": 0.93,
  "allowed_reference_facts": [
    {"fact": "SQWOZ BAB — сценический псевдоним Марата Мингазова", "fact_type": "identity"},
    {"fact": "SQWOZ BAB — российский комедийный рэпер", "fact_type": "occupation_genre"}
  ],
  "forbidden_reference_facts": [
    "какие треки будут на концерте",
    "что войдёт в программу",
    "будет ли live-бэнд",
    "будут ли новые композиции"
  ]
}
```

## Storage / provenance contract

Reference facts must be stored with provider metadata so an admin can audit why a sentence appeared.

Conceptual source record:

```json
{
  "source_role": "reference",
  "provider": "wikipedia",
  "url": "https://ru.wikipedia.org/wiki/Sqwoz_Bab",
  "wikidata_qid": "Q106541067",
  "page_title": "Sqwoz Bab",
  "page_id": "8567077",
  "revision_id": "...",
  "fetched_at": "2026-06-29T...Z",
  "entity_match_confidence": 0.93,
  "publicly_counted_as_event_source": false
}
```

Conceptual fact record:

```json
{
  "scope": "entity_reference",
  "fact_type": "identity",
  "text": "SQWOZ BAB — сценический псевдоним Марата Мингазова",
  "source_role": "reference",
  "provider": "wikipedia",
  "confidence": 0.92,
  "render_allowed": true
}
```

Implementation may use existing `event_source` / `event_source_fact` with role metadata or dedicated tables, but the public renderer and reports must preserve the role distinction.

## Public rendering and licensing

Preferred MVP rendering is one original, neutral inline sentence generated from structured facts/triples, not copied from the Wikipedia extract.

- Do not paste or closely paraphrase long Wikipedia text.
- Keep public “Источников: N” limited to event-specific sources.
- If a public page renders a direct Wikipedia-derived text passage or close paraphrase, it must include visible attribution and license-compatible links. The MVP avoids this by using short factual triples plus internal provenance.
- Admin/operator UI must still show that Wikipedia was used (see Smart Update report contract below).

## Writer coverage check

Every public sentence must be supported by allowed fact ids.

For sparse events, reject unsupported event-specific claims even if they sound plausible:

- “хиты”, “сет-лист”, “новые композиции”, “свежий материал”;
- “живое исполнение”, “специальная программа”, “сюрпризы”, “гости”;
- “презентация альбома”, “туровая программа”.

These are allowed only with supporting `event_source` facts, not with `reference_source` facts.

## Smart Update report contract

When any Wikipedia/Wikidata fact is accepted or used by the writer, the Smart Update operator/admin report sent in the bot must include a visible line, for example:

```text
Справочное обогащение: Wikipedia — Sqwoz Bab / Q106541067, +2 факта, confidence 0.93
```

If lookup was attempted but rejected, the report may include a compact note when useful for operator debugging:

```text
Справочное обогащение: Wikipedia пропущена — ambiguous entity / confidence 0.61
```

The report line must not imply that Wikipedia confirmed the event. Use wording `Справочное обогащение` / `reference`, not `Источник события`.

## Failure behaviour

- API timeout, rate limit, missing QID, ambiguous match, low confidence: publish dry sparse text without enrichment.
- No infinite retries in the Smart Update hot path.
- Cache provider responses by page/QID/revision where possible.
- Use a meaningful Wikimedia User-Agent and respect provider rate limits.

## MVP flags

Suggested rollout flags:

- `EVENT_REFERENCE_ENRICHMENT=1` — master switch;
- `WIKIMEDIA_REFERENCE_ENRICHMENT=1` — Wikimedia provider;
- `STRICT_SPARSE_WRITER=1` — block unsupported sparse claims;
- `GIVEAWAY_REQUIRES_EVENT_SOURCE=1` — block giveaway-only auto-publication.

## Acceptance checks

1. Sparse valid concert + high-confidence artist match adds at most one safe artist-reference sentence.
2. Giveaway-only concert mention stays `needs_event_source`; Wikipedia is blocked.
3. Ambiguous entities (`Кино`, `Мираж`) publish dry without reference facts.
4. Tribute/cover events do not enrich the original artist as the performer.
5. Public source count excludes reference sources.
6. Smart Update bot report mentions Wikipedia whenever Wikipedia facts are accepted/used.
7. Writer coverage rejects programme/set-list/live-band/new-track claims unless supported by event-specific source facts.
