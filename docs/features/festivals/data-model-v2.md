# Festival Data Model v2: taxonomy, evidence and Antigravity collection

Status: `proposed preproduction contract`; no production migration or public
festival detail pages are included in this change.

Scope: the canonical JSON and persistence model needed to collect enough
source-backed data for future detailed festival pages. Page layouts and the
number of visual page templates are explicitly out of scope.

Related contracts:

- current festival model and queue: [`README.md`](README.md);
- Antigravity-first non-social research:
  [`../source-parsing/sources/festival-parser/preproduction-web-research.md`](../source-parsing/sources/festival-parser/preproduction-web-research.md);
- reusable evidence-first prompts:
  [`../../llm/antigravity-festival-research.md`](../../llm/antigravity-festival-research.md);
- reviewed 2026 calendar projection:
  [`../static-site-pages/festival-timeline.md`](../static-site-pages/festival-timeline.md).

## Executive decision

The festival calendar branch at commit `940fea2e` contains valuable reviewed
facts, but it does **not** contain one seven-class festival taxonomy. It has
several independent, mostly presentation-oriented dimensions:

| Dimension | Values at exact tree `940fea2e` | Meaning |
|---|---:|---|
| calendar items | 21 | reviewed 2026 edition-like rows |
| displayed `category` | 16 free-text labels | theme shown on a card |
| `status` | 3 | `announced`, `program-pending`, `date-pending` |
| observed date/status shapes | 7 | derived combinations, not declared types |
| media source kind | 4 | festival, organizer, venue, regional official |
| `internalEventId` coverage | 4/21 | optional link to one current `event` |

The exact tree has no `festivalId`, `startDate`, `endDate`, `datePrecision` or
programme children at all. Its seven observable shapes are merely combinations
of free-form `dateLabel` syntax and `status`: bounded/single-day announced,
bounded/single-day programme-pending, and month/multi-month/open-start
date-pending. The number seven therefore does not describe festival semantics.

A later successor (`0abe04ab`, not a descendant of `940fea2e`) moves the
calendar into `festivalTimelineSeed.json`/`festival_calendar_item`, adds the
four date-precision values and optional `festival_id`. In the current reviewed
seed those links cover 9/21 Festival rows and 4/21 Event rows; one item has both,
so only 12/21 have either backend link. Both histories prove a good
calendar/provenance contract, not a complete series/edition/programme model.

The to-be design must not replace those dimensions with one overloaded enum.
It uses:

1. stable **series** and **edition** identity;
2. seven orthogonal classification axes;
3. a seven-value item disposition that decides whether a programme row becomes
   an Event, stays a schedule row, or is rejected;
4. atomic source claims and immutable revisions;
5. a generated `festival-edition-v2` JSON projection as the only future page
   input.

Antigravity owns semantic classification and claim extraction. Local code owns
input bounds, schemas, exact-quote/reference validation, revisioning and the
fail-closed apply boundary.

## 1. What can and cannot be reused from the calendar branch

### Reusable

- truthful date precision rather than invented dates;
- explicit programme/date pending states;
- reviewed source URL and reader-facing source label;
- source-classified, hash-bound media provenance;
- stable year + slug calendar identity as migration input;
- optional legacy Festival/Event links;
- raw category labels as evidence-backed editorial labels;
- calendar ordering and place labels as a reviewed projection.

### Not canonical festival truth

- `category` is a free-text display label, not a controlled taxonomy;
- `status` mixes research completeness with public lifecycle;
- `dateLabel`, `placeLabel` and `description` are rendered copy rather than
  atomic facts;
- one `sourceUrl` cannot represent all programme, ticket and venue evidence;
- one optional `internalEventId` cannot represent a multi-event programme;
- a calendar item has no sections, programme item identity, participants,
  offers, organisers, source conflicts or revision history;
- the later optional `festivalId` points to the legacy edition-like row, while
  the stable series identity is absent.

The exact 21-row TypeScript array and its later equivalent JSON seed remain
reviewed import/evaluation fixtures. They must not be copied into reusable
Antigravity prompts as factual few-shot content.

## 2. Current backend gaps

The current persistence contour has three overlapping representations:

1. `festival`: one flat, edition-like row. It mixes identity, current facts,
   parser metadata, one source, media and opaque `activities_json`;
2. `festival_calendar_item`: reviewed year-scoped calendar projection;
3. `event.festival`: a string relation rather than an edition foreign key.

The legacy Universal Festival Parser writes UDS v1 directly into the flat
Festival row. It matches by `source_url` or case-insensitive name and updates
non-null values. Programme rows are stored wholesale in `activities_json` and
strong rows are separately routed through Smart Update. That cannot express:

- series vs edition identity;
- several editions with stable redirects;
- multiple sources and immutable snapshots;
- a fact-level conflict or its evidence;
- a programme section/track/day/stage hierarchy;
- an item that is simultaneously a schedule row and a link to an Event;
- independent programme, ticket, venue and media freshness;
- an approved revision distinct from a newly researched candidate;
- a controlled taxonomy version.

At `940fea2e` the legacy reasoning prompt does ask for one of eight theme
strings (`music|film|theater|art|literature|food|sport|other`). However
`UDSFestival` does not declare that field; its default nested model behaviour
discards the undeclared value, while the outer `UDSOutput.extra="allow"` does
not make it a typed Festival field. The server upsert does not persist it.
This is neither the remembered seven-value taxonomy nor durable
classification.

## 3. Seven classification axes

These axes answer different questions and are stored independently. `unknown`
is always valid during research; it is never silently converted to a default.

### T1 — identity kind

What is the researched container?

```text
festival
festival_cycle
civic_programme
holiday_programme
fair_or_market
competition_or_showcase
not_festival
unknown
```

`not_festival` is a rejection verdict, not a public edition. A marketing word
such as “fest” is not enough to choose `festival`.

### T2 — programme profile

How should the current known programme be represented? This is the axis that
answers “separate Events or only a schedule?”.

| Value | Meaning | Event behaviour |
|---|---|---|
| `identity_only` | edition/dates exist, programme is not published or not found | create no programme Events |
| `single_compound_event` | one indivisible festival visit/admission; internal timetable does not define separate visits | at most one canonical Event linked to the edition |
| `standalone_events` | programme consists of independently attendable items | link/create one Event per accepted item |
| `schedule_only` | named/timed slots share one admission/container and are useful only as a timetable | keep rows in festival schedule, create no item Events |
| `hybrid` | some independently attendable Events plus schedule/programme-only rows | materialize only accepted Event rows |
| `continuous_experience` | exhibition, fair, zones or activities operate over a time range rather than discrete starts | store hours/zones/activities; create only separately announced Events |
| `distributed_cycle` | a branded programme spans non-contiguous dates/venues over a long period | link/create occurrence Events while preserving edition/track grouping |
| `unknown` | evidence is insufficient or A/B disagree | manual review; no automatic materialization |

`identity_only` is a snapshot state and may change when the programme appears.
A profile change creates a new revision; it never destructively rewrites the
last approved programme.

### T3 — controlled topics

The branch's 16 labels are retained as `raw_labels` and mapped into versioned
controlled IDs. A festival may have one `primary_topic_id` and several
`secondary_topic_ids`; multi-topic data is not collapsed into a page type.

Initial mapping proposal:

| Calendar label | Primary controlled topic | Suggested facets |
|---|---|---|
| Авторская песня | `music` | `singer_songwriter` |
| Вино и гастрономия | `food_and_drink` | `wine`, `gastronomy` |
| Гастрономия | `food_and_drink` | `gastronomy`, `street_food` when supported |
| Джаз | `music` | `jazz` |
| История и реконструкция | `heritage_and_cultures` | `history`, `reenactment` |
| Кино | `screen_and_visual_arts` | `cinema` |
| Классическая музыка | `music` | `classical_music` |
| Косплей | `popular_and_fan_culture` | `cosplay` |
| Культура народов | `heritage_and_cultures` | `multicultural` |
| Литература | `literature_and_ideas` | `literature` |
| Море и техника | `travel_maritime_and_technology` | `maritime`, `technology` |
| Музыка | `music` | no narrower facet without evidence |
| Путешествия | `travel_maritime_and_technology` | `travel`, `exploration` |
| Семейный фестиваль | `family_and_community` | `family` |
| Современное искусство | `screen_and_visual_arts` | `contemporary_art` |
| Театр | `performing_arts` | `theatre` |

This mapping is migration input, not an approved immutable vocabulary. Unknown
Antigravity proposals go to `unmapped_topic_labels`; they never become serving
filters until the vocabulary version changes.

### T4 — temporal profile

```text
single_day
consecutive_range
non_contiguous_dates
recurring_schedule
continuous_range
seasonal_window
unknown
```

Exact facts live separately from this derived label. `date_precision` remains:
`exact|month|month_range|start_only|year|unknown`.

### T5 — spatial profile

```text
single_venue
campus_multi_stage
city_multi_venue
regional_distributed
touring
online_or_hybrid
unknown
```

### T6 — access profile

Access is multi-valued because an edition can mix free and paid items:

```text
common_ticket
festival_pass
per_event_ticket
registration
free_entry
walk_in
mixed
unknown
```

A subscription/pass is never copied into an individual Event's `ticket_url`.

### T7 — publication/lifecycle state

```text
announced_dates_only
programme_partial
programme_published
sales_open
changed
postponed
cancelled
completed
unknown
```

Research completeness is stored separately as field/item coverage. A festival
can be `sales_open` while a venue or participant is still unknown.

### Versioned taxonomy registry

The seven axes, topic hierarchy and seven programme-item dispositions are
host-owned data, not prompt prose. A mounted immutable registry has this
minimum shape:

```json
{
  "schema_version": "festival-taxonomy-registry-v1",
  "taxonomy_id": "kenigevents-festivals",
  "taxonomy_version": "1.0.0",
  "axes": {
    "identity_kind": {
      "cardinality": "exactly_one",
      "values": ["festival", "festival_cycle", "civic_programme", "holiday_programme", "fair_or_market", "competition_or_showcase", "not_festival", "unknown"]
    },
    "programme_profile": {
      "cardinality": "exactly_one",
      "values": ["identity_only", "single_compound_event", "standalone_events", "schedule_only", "hybrid", "continuous_experience", "distributed_cycle", "unknown"]
    },
    "topic": {
      "cardinality": "one_primary_and_zero_or_more_secondary",
      "nodes": [{
        "node_id": "music",
        "parent_node_id": null,
        "label_ru": "Музыка",
        "definition": "source-backed music-led festival content",
        "aliases": [],
        "status": "active"
      }]
    },
    "temporal_profile": {"cardinality": "exactly_one", "values": ["..."]},
    "spatial_profile": {"cardinality": "exactly_one", "values": ["..."]},
    "access_profile": {"cardinality": "one_or_more", "values": ["..."]},
    "lifecycle_state": {"cardinality": "exactly_one", "values": ["..."]}
  },
  "item_dispositions": [{
    "value": "create_event_candidate",
    "definition": "independently attendable item with event-grade evidence",
    "allowed_apply_action": "smart_update_only"
  }]
}
```

The authoritative manifest stores the registry file path, byte
`content_sha256`, schema version and approval metadata; the candidate pins both
`taxonomy_version` and that hash. Patch versions may add aliases or correct
wording, minor versions may add values/nodes, and semantic split/merge requires
a major version. A value/node ID is never reused. Agent-proposed gaps stay
`unmapped` until an operator approves a new immutable registry version.

## 4. Seven programme-item dispositions

Every extracted programme row receives exactly one locally validated
`disposition`. The edition-level profile is reconciled from the accepted set;
it must not be guessed first and then forced onto rows.

| Disposition | Meaning | Apply action |
|---|---|---|
| `link_existing_event` | row is an independently attendable event already present | create stable edition↔Event relation |
| `create_event_candidate` | independently attendable event with sufficient source evidence | send only through Smart Update |
| `schedule_slot` | named/timed part of a shared admission/container | render in schedule; no Event |
| `programme_only` | meaningful activity without event-grade logistics/identity | render in programme block; no Event |
| `continuous_activity` | zone, exhibition, fair, installation or service available over a range | render with hours/range; no automatic Event |
| `service_information` | doors, break, transport, accreditation, ticket desk or other logistics | render only in service context |
| `reject` | stale edition, duplicate, unrelated, unsupported or navigation noise | persist rejection/evidence; never publish as programme |

`create_event_candidate` requires the existing LLM-first festival programme
contract: explicit date, meaningful identity and sufficient logistics, plus a
strong independence signal such as its own ticket/registration, venue, start,
format or explicit standalone announcement. A local keyword list cannot promote
an item.

## 5. Canonical persistence model

Fly SQLite remains canonical for operational and approved festival truth.
Existing Supabase Storage bucket `festival-parsing` stores large immutable raw
artifacts. The personalization Supabase project is not used.

### Taxonomy registry persistence

```text
festival_taxonomy_version
  id, taxonomy_id, taxonomy_version, schema_version, artifact_path,
  content_sha256, state, approved_by, approved_at, created_at,
  UNIQUE(taxonomy_id, taxonomy_version), UNIQUE(content_sha256)

festival_taxonomy_node
  id, taxonomy_version_id, axis_id, node_id, parent_node_id,
  label_ru, definition, aliases_json, cardinality, status,
  UNIQUE(taxonomy_version_id, axis_id, node_id)

festival_taxonomy_assignment
  id, revision_id, subject_kind, subject_key, axis_id, node_id,
  is_primary, decision_id, claim_ids_json, status
```

Runtime reads only an approved version whose stored artifact hash matches the
mounted bytes. New agent labels never write these tables directly.

### `festival_series`

Stable brand/series identity:

```text
id
slug UNIQUE
canonical_name
aliases_json
identity_status          # active | merged | archived | review
merged_into_series_id
identity_version
created_at
updated_at
```

### `festival_edition`

One concrete edition:

```text
id
series_id FK
edition_key UNIQUE       # stable opaque/product key, not a display title
slug UNIQUE
edition_label
year
season
ordinal
identity_kind
timezone
start_date
end_date
date_precision
programme_profile
primary_topic_id
topic_ids_json
temporal_profile
spatial_profile
access_profiles_json
lifecycle_state
taxonomy_version
effective_revision_id
public_status            # shadow | approved | archived | merged
merged_into_edition_id
created_at
updated_at
```

A nullable year is valid. Different explicit years never share an edition.

### `festival_edition_revision`

Immutable researched candidate/approval boundary:

```text
id
edition_id FK
revision_no
schema_version
contract_version
prompt_version
taxonomy_version
input_fingerprint
candidate_sha256
candidate_json
source_manifest_json
quality_json
state                    # shadow | needs_review | approved | superseded | rejected
approved_by
approved_at
created_at
UNIQUE(edition_id, revision_no)
UNIQUE(edition_id, candidate_sha256)
```

Only an `approved` revision may become `effective_revision_id`.

### Research provenance

Operational runs remain in `festival_web_research_run/item/source`. Durable
accepted evidence is copied/referenced through:

```text
festival_edition_source
  id, edition_id, canonical_url, source_role, authority_status,
  edition_status, first_seen_at, last_seen_at, current_snapshot_id

festival_source_snapshot
  id, edition_source_id, content_sha256, artifact_path, retrieved_at,
  content_type, fetch_mode, http_status, run_uid

festival_claim
  id, revision_id, snapshot_id, subject_kind, subject_key, field_path,
  raw_value_json, normalized_value_json, normalization,
  verbatim_quote, quote_start, quote_end, normalizer_version,
  context_quote, status

festival_decision
  id, revision_id, decision_key, decision_kind, subject_kind, subject_key,
  selected_value_json, alternatives_json, evidence_claim_ids_json,
  reason_codes_json, status, actor_kind, contract_version, created_at,
  UNIQUE(revision_id, decision_key)
```

A claim quote is valid only when the recorded offsets reproduce it under the
pinned normalizer against its exact snapshot hash. Substring-only validation is
insufficient when a date/name occurs in several sections of one page.

Every `decision_id` in an edition, classification, item or relation resolves to
one `festival_decision` in the same revision. Decisions never substitute for
claims: selected semantic classifications cite their evidence claims, and a
manual override records actor/reason without fabricating source evidence.

### Programme

```text
festival_programme_section
  id, revision_id, stable_key, parent_section_id, section_kind,
  title, date, venue_key, track_key, display_order, claim_ids_json

festival_programme_item
  id, revision_id, stable_key, section_id, disposition, item_kind,
  title, date_precision, access_json, participant_refs_json, description_facts_json,
  source_claim_ids_json, identity_hash, display_order, status

festival_programme_occurrence
  id, programme_item_id, occurrence_key, start_at, end_at, date_precision,
  venue_refs_json, access_override_json, source_claim_ids_json, status,
  UNIQUE(programme_item_id, occurrence_key)

festival_programme_item_event
  programme_item_id, programme_occurrence_id, event_id, relation_status,
  decision_id, evidence_claim_ids_json, created_at, updated_at
```

`stable_key` is generated from accepted edition-local identity evidence and is
preserved across revisions when the same item is matched. It is not a hash of
mutable description copy.

### Venues, parties, offers and media

The approved revision JSON is authoritative initially. Query-heavy relations
may be projected into dedicated tables without changing the JSON contract:

```text
festival_edition_venue
  id, revision_id, venue_key, role, canonical_venue_id, name, address,
  city, latitude, longitude, claim_ids_json, status, display_order

festival_edition_party
  id, revision_id, party_key, party_kind, canonical_party_id, name,
  roles_json, claim_ids_json, status, display_order

festival_edition_offer
  id, revision_id, offer_key, scope, offer_kind, programme_item_key,
  url, price_min, price_max, currency, is_free, availability,
  claim_ids_json, status

festival_edition_media
  id, revision_id, asset_key, role, source_id, source_url, storage_path,
  content_sha256, width, height, rights_status, rights_text,
  claim_ids_json, status, display_order
```

Event-specific offers remain on Event/programme items. Festival-level offers
must declare `scope=edition|pass|subscription|registration`.

### Compatibility projections

- legacy `festival` is updated from the approved edition projection during
  migration, not written directly by Antigravity;
- `event.festival` remains a temporary display compatibility string;
- stable links use `festival_programme_item_event` (or an edition↔Event view);
- `festival.activities_json` becomes a lossy compatibility projection of
  accepted non-Event items;
- `festival_calendar_item` remains the reviewed calendar projection and gains
  a stable `edition_id` in a later migration;
- no existing row or public output is deleted during shadow rollout.

## 6. Canonical `festival-edition-v2` JSON

Agent files are untrusted inputs. The following candidate projection is
produced locally only from validated claims. The same schema may remain
`shadow`/`needs_review`; only an approved revision may become the public
projection.

```json
{
  "schema_version": "festival-edition-v2",
  "taxonomy_id": "kenigevents-festivals",
  "taxonomy_version": "1.0.0",
  "taxonomy_sha256": "...",
  "revision": {
    "revision_no": 3,
    "candidate_sha256": "...",
    "effective_at": null,
    "status": "shadow"
  },
  "evidence_manifest": {
    "claims_ref": "festival-parsing/.../claims.ndjson",
    "claims_sha256": "...",
    "claim_count": 81,
    "source_snapshot_count": 1
  },
  "identity": {
    "series_key": "opaque-series-key",
    "edition_key": "opaque-edition-key",
    "series_title": {"value": "...", "claim_ids": ["C001"]},
    "edition_title": {"value": "...", "claim_ids": ["C002"]},
    "edition_label": {"value": null, "claim_ids": ["C080", "C081"], "status": "conflict"},
    "year": {"value": 2026, "claim_ids": ["C003"]},
    "aliases": []
  },
  "classification": {
    "identity_kind": {
      "value": "festival",
      "claim_ids": ["C004"],
      "decision_ids": ["D001"],
      "status": "supported"
    },
    "programme_profile": {
      "value": "hybrid",
      "claim_ids": ["C030", "C031", "C032"],
      "decision_ids": ["D002"],
      "status": "supported"
    },
    "primary_topic_id": {
      "value": "music",
      "claim_ids": ["C005"],
      "decision_ids": ["D001"],
      "status": "supported"
    },
    "secondary_topic_ids": {
      "values": ["performing_arts"],
      "claim_ids": ["C006"],
      "decision_ids": ["D001"],
      "status": "supported"
    },
    "raw_topic_labels": [{
      "value": "...",
      "claim_ids": ["C005"]
    }],
    "unmapped_topic_labels": [],
    "temporal_profile": {
      "value": "consecutive_range",
      "claim_ids": ["C010", "C011"],
      "decision_ids": ["D001"],
      "status": "supported"
    },
    "spatial_profile": {
      "value": "campus_multi_stage",
      "claim_ids": ["C040", "C041"],
      "decision_ids": ["D001"],
      "status": "supported"
    },
    "access_profiles": {
      "values": ["festival_pass", "per_event_ticket"],
      "claim_ids": ["C050", "C051"],
      "decision_ids": ["D001"],
      "status": "supported"
    },
    "lifecycle_state": {
      "value": "programme_published",
      "claim_ids": ["C052"],
      "decision_ids": ["D001"],
      "status": "supported"
    }
  },
  "dates": {
    "timezone": "Europe/Kaliningrad",
    "start_date": {"value": "2026-08-20", "claim_ids": ["C010"]},
    "end_date": {"value": "2026-08-22", "claim_ids": ["C011"]},
    "date_precision": "exact",
    "display_label": null
  },
  "summary_facts": [
    {"value": "...", "claim_ids": ["C020"], "status": "supported"}
  ],
  "parties": [
    {
      "party_key": "party:1",
      "party_kind": "organization",
      "name": {"value": "...", "claim_ids": ["C060"], "status": "supported"},
      "website": {"value": "https://...", "claim_ids": ["C062"]},
      "canonical_party_id": null
    },
    {
      "party_key": "party:2",
      "party_kind": "music_group",
      "name": {"value": "...", "claim_ids": ["C033"], "status": "supported"},
      "website": {"value": null, "claim_ids": [], "status": "unknown"},
      "canonical_party_id": null
    }
  ],
  "organizers": [{
    "party_ref": "party:1",
    "role": "organizer",
    "claim_ids": ["C061"]
  }],
  "venues": [{
    "venue_key": "venue:1",
    "role": "programme_venue",
    "name": {"value": "...", "claim_ids": ["C040"], "status": "supported"},
    "address": {"value": "...", "claim_ids": ["C041"], "status": "supported"},
    "city": {"value": "Калининград", "claim_ids": ["C042"]},
    "geo": {"latitude": null, "longitude": null, "status": "unknown"},
    "canonical_venue_id": null
  }],
  "programme_sections": [
    {
      "section_key": "day-1",
      "kind": "day",
      "title": "...",
      "claim_ids": ["C030"],
      "items": [
        {
          "item_key": "opaque-item-key",
          "disposition": "create_event_candidate",
          "item_kind": "performance",
          "title": {"value": "...", "claim_ids": ["C031"]},
          "occurrences": [{
            "occurrence_key": "occurrence:1",
            "starts_at": {"value": "2026-08-20T18:00:00+02:00", "claim_ids": ["C032"]},
            "ends_at": {"value": null, "claim_ids": [], "status": "unknown"},
            "date_precision": "exact",
            "venue_refs": ["venue:1"],
            "status": "scheduled"
          }],
          "participants": [{
            "party_ref": "party:2",
            "role": "performer",
            "claim_ids": ["C033"]
          }],
          "access": {"offer_refs": ["offer:single:1"]},
          "event_relations": [{
            "occurrence_ref": "occurrence:1",
            "status": "candidate",
            "event_id": null
          }],
          "decision_ids": ["D010"]
        }
      ]
    }
  ],
  "offers": [{
    "offer_key": "offer:single:1",
    "scope": "programme_item",
    "programme_item_ref": "opaque-item-key",
    "offer_kind": "ticket",
    "url": {"value": "https://...", "claim_ids": ["C050"]},
    "price": {
      "min": {"value": 500, "claim_ids": ["C053"]},
      "max": {"value": null, "claim_ids": [], "status": "unknown"},
      "currency": "RUB",
      "is_free": {"value": false, "claim_ids": ["C053"]}
    },
    "availability": {"value": "available", "claim_ids": ["C054"]}
  }],
  "media": [{
    "asset_key": "media:1",
    "role": "cover",
    "source_id": "S001",
    "source_url": "https://...",
    "storage_path": "festival-parsing/...",
    "content_sha256": "...",
    "width": 1600,
    "height": 900,
    "rights": {"status": "source_provided", "claim_ids": ["C070"]},
    "status": "supported"
  }],
  "sources": [{
    "source_id": "S001",
    "canonical_url": "https://...",
    "source_role": "official_program",
    "edition_status": "accepted",
    "content_sha256": "...",
    "normalizer_version": "festival-text-normalizer-v1",
    "retrieved_at_utc": "2026-07-30T00:00:00Z",
    "snapshot_ref": "snapshot:S001:...",
    "claim_ids": ["C001", "C002", "C003", "C080", "C081"]
  }],
  "decisions": [
    {
      "decision_id": "D001",
      "decision_kind": "edition_classification_bundle",
      "subject_ref": "edition:opaque-edition-key",
      "selected_value": {
        "identity_kind": "festival",
        "primary_topic_id": "music",
        "secondary_topic_ids": ["performing_arts"],
        "temporal_profile": "consecutive_range",
        "spatial_profile": "campus_multi_stage",
        "access_profiles": ["festival_pass", "per_event_ticket"],
        "lifecycle_state": "programme_published"
      },
      "alternatives_rejected": [],
      "evidence_claim_ids": ["C004", "C005", "C006", "C010", "C011", "C040", "C041", "C050", "C051", "C052"],
      "reason_codes": ["a_b_evidence_compatible"],
      "status": "supported",
      "actor_kind": "antigravity_a_b_agreement"
    },
    {
      "decision_id": "D002",
      "decision_kind": "programme_profile",
      "subject_ref": "edition:opaque-edition-key",
      "selected_value": "hybrid",
      "alternatives_rejected": ["standalone_events", "schedule_only"],
      "evidence_claim_ids": ["C030", "C031", "C032"],
      "reason_codes": ["mixed_independent_and_schedule_items"],
      "status": "supported",
      "actor_kind": "antigravity_a_b_agreement"
    },
    {
      "decision_id": "D010",
      "decision_kind": "programme_item_disposition",
      "subject_ref": "programme_item:opaque-item-key",
      "selected_value": "create_event_candidate",
      "alternatives_rejected": ["schedule_slot", "programme_only"],
      "evidence_claim_ids": ["C031", "C032", "C033", "C050"],
      "reason_codes": ["independent_identity", "own_start", "own_ticket"],
      "status": "supported",
      "actor_kind": "antigravity_a_b_agreement"
    }
  ],
  "conflicts": [{
    "conflict_id": "CF001",
    "json_path": "/identity/edition_label",
    "alternatives": [
      {"value": "X фестиваль", "claim_ids": ["C080"], "source_ids": ["S001"]},
      {"value": "XI фестиваль", "claim_ids": ["C081"], "source_ids": ["S001"]}
    ],
    "blocking": true,
    "status": "conflict"
  }],
  "unknowns": [{
    "json_path": "/programme_sections/0/items/0/occurrences/0/ends_at",
    "reason_code": "not_stated",
    "needed_evidence": "direct programme or ticket page with an end time"
  }],
  "quality": {
    "publishable": false,
    "field_coverage": {},
    "programme_item_counts": {},
    "a_b_agreement": {},
    "violations": []
  }
}
```

`revision.candidate_sha256` is reproducible, not a hash of bytes containing
itself. The host makes a deep copy, sets `candidate_sha256=null`, omits mutable
workflow metadata `revision.status`, `revision.effective_at`, serializes the
remaining projection as RFC 8785 canonical JSON UTF-8, then stores SHA-256 of
those bytes in both the JSON and `festival_edition_revision`. Any semantic
field change changes the hash; approval/status transitions do not.

### Object contracts and cardinality

The authoritative JSON Schema uses `additionalProperties=false` at every
object, explicit enum/length/array bounds and these reference rules:

| Path | Cardinality and required fields |
|---|---|
| `evidence_manifest` | exactly one hash/count-bound claims ledger for the revision; no inline raw pages |
| `identity` | exactly one series and edition key; every non-null public scalar has `claim_ids` |
| `classification` | exactly one value for each of the seven axes; multi-value axes use `values`; every semantic mapping has `decision_ids` and evidence claims |
| `parties` | zero or more unique `party_key`; requires kind and claim-backed name; organizers and item participants reference an existing party |
| `organizers` | zero or more edition-scoped `(party_ref, role)` relations with claims |
| `venues` | zero or more unique `venue_key`; requires role/name status; unresolved address/geo stays null/unknown |
| `programme_sections` | zero or more stable-keyed tree nodes; every item belongs to exactly one section, has exactly one disposition and zero or more uniquely keyed occurrences |
| `offers` | zero or more unique `offer_key`; requires scope/kind; item-scoped offer references one existing programme item; unknown price never means free |
| `media` | zero or more hash/provenance-bound assets; requires role, source, dimensions when known and rights status |
| `sources` | at least one for a data-ready revision; unique `source_id`, canonical URL, snapshot hash, normalizer version, role and edition status |
| `decisions` | zero or more unique `decision_id`; all referenced IDs resolve inside the same revision and cite existing claims |
| `conflicts` | alternatives cite existing claims/sources; unresolved blocking conflict makes `publishable=false` |
| `unknowns` | machine path, reason code and needed evidence; never filled with a guessed default |

All `party_ref`, `venue_ref`, `offer_ref`, `source_id`, `decision_id`,
programme item and Event references are validated as a closed graph inside the
projection. Every `claim_id` resolves against the revision's hash-bound
`claims.ndjson` from `evidence_manifest`. Empty arrays mean “none found in
accepted evidence”, not “known absent”, unless a claim/decision explicitly
supports absence.

`display_label` and public narrative copy are later projections. Antigravity
may extract source wording and facts, but it does not write unsupported public
copy into canonical facts.

## 7. Antigravity collection topology

Normal cost remains two agent interactions per festival group; the hard cap is
three. No stage below creates an extra provider request.

### Operational correction from the live 2026-07-29 probe

The first 2+1 experiment proved factual value but did **not** prove the
checkpoint contract:

| Call | Role | Status | Actual tokens | Structured semantic checkpoint |
|---|---|---:|---:|---|
| A | primary | `incomplete` | 78,948 | none |
| B | checker | `incomplete` | 27,198 | no ledger/result; raw sources only |
| C | adjudicator | `incomplete` | 44,804 | no adjudication result; raw sources only |

The three known errors were recoverable by local/manual review of saved source
evidence, not by parsing completed source/claim/taxonomy checkpoints. A also
used four searches and explored ticket implementation details; the effective C
packet was roughly 42 KB, included excessive candidate/excerpt material and
performed fetches. Those behaviours violate the intended narrow topology.

Therefore “write checkpoints immediately” must become an executable protocol:
append-only per-source decision/claim files, atomic rename, host-side schema
validation and inventory conservation. A terminal answer or raw source folder
alone does not count as a successful research result.

### Call A — primary evidence researcher

Fresh environment receives:

- target/queue manifest;
- bounded normalized snapshots of all grouped seed URLs;
- taxonomy and JSON contracts;
- no legacy parser result and no previous approved narrative copy.

Required checkpoint sequence:

```text
/workspace/festival_research/state.json
/workspace/festival_research/source_ledger.json
/workspace/festival_research/source_reviews/S001.json
/workspace/festival_research/claims/S001.jsonl
/workspace/festival_research/programme/S001.jsonl
/workspace/festival_research/taxonomy_a.json
/workspace/festival_research/candidate_a.json
```

Rules:

1. write `state.json` before the first network call;
2. inspect each seed source independently;
3. use at most one search query only when seed sources lack a credible current
   edition source;
4. use at most six sources and checkpoint immediately after each fetch;
5. decide source role/edition before extracting claims;
6. extract source-local programme rows without cross-source merging;
7. assign an item disposition only with claim/decision evidence;
8. reconcile entities and programme after all accepted source-local files exist;
9. derive all seven taxonomy axes from accepted rows/claims;
10. never return model confidence or publishability;
11. execute the mounted schema validator after each checkpoint for feedback,
    while accepting that the host rerun outside the sandbox is authoritative.

For every programme disposition A writes a decision record:

```json
{
  "decision_id": "D010",
  "item_subjects": ["S001:item:3"],
  "disposition": "schedule_slot",
  "evidence_claim_ids": ["C031", "C032", "C033"],
  "reason_codes": ["shared_admission", "shared_container", "not_independently_attendable"],
  "alternatives_rejected": ["create_event_candidate"]
}
```

Reason codes are prompt vocabulary, not a deterministic semantic classifier.
Local code only validates their references and allowed enum values.

### Call B — independent classifier/checker

A new environment receives the same target and seed snapshots, but not
candidate A, A taxonomy decisions or A programme dispositions.

B performs one alternative search query, fetches at most four pages and writes:

```text
/workspace/festival_research_check/state.json
/workspace/festival_research_check/source_ledger.json
/workspace/festival_research_check/claims/S001.jsonl
/workspace/festival_research_check/taxonomy_b.json
/workspace/festival_research_check/item_dispositions_b.jsonl
/workspace/festival_research_check/counter_evidence.json
```

B must independently decide:

- identity kind and current edition;
- programme profile;
- topic/temporal/spatial/access/lifecycle axes;
- whether each critical programme subject is independent Event material,
  schedule-only, continuous activity, service information or rejected;
- stale edition, unsupported title modifier and ticket scope challenges.

B does not reproduce the full rich candidate. Local comparison matches B's
source-local subject signatures to A's accepted item clusters.

### Local comparison

A result proceeds without C only when:

- A/B agree on identity kind and programme profile;
- controlled topic mapping has no critical unmapped primary label;
- temporal/spatial/access classifications are compatible;
- every `create_event_candidate`/`link_existing_event` has an independently
  compatible B disposition or direct official single-item evidence;
- every source-local programme subject from the union of A/B inventories has a
  disposition; a B omission is unresolved, not agreement;
- neither side found stale-edition, ticket-scope or event-identity conflict;
- all claims, quotes, hashes and references pass local validation.

Agreement is evidence compatibility, not identical prose.

### Call C — conditional adjudicator

C receives only the conflicting classification/item values, exact quotes and
claim/source hashes. It receives no full pages/candidates and has no
search/network/fetch tool. If the bounded packet is insufficient or too large,
the run goes to operator review rather than silently truncating or refetching.

C returns exactly one selection-only decision per input conflict:

```json
{
  "schema_version": "festival-adjudication-v1",
  "decisions": [{
    "conflict_id": "CF001",
    "choice": "existing-alternative-id|unknown|conflict",
    "supporting_claim_ids": ["C001"],
    "reason_code": "host-vocabulary-value"
  }]
}
```

It cannot introduce a third value, new fact or reconstructed candidate.
Unresolved `identity_kind`, `programme_profile` or Event/programme-only dispute
makes the revision `needs_review`.

### Incomplete recovery

Antigravity has no native structured-output guarantee and token limits are
best-effort. Therefore:

- each JSON/JSONL checkpoint is parsed and schema-validated independently;
- authoritative schemas use `additionalProperties=false`, enum and size bounds;
- a terminal response is optional if the required checkpoints exist;
- a partial source never confirms a critical fact;
- missing late candidate files are reconstructed locally only from valid early
  claims/decisions, without semantic repair;
- no automatic fourth interaction is allowed.

## 8. Local quality gates

### Evidence

- 100% of non-null critical scalars reference accepted claims;
- every quote is found in the exact hashed snapshot;
- rejected/ambiguous sources contribute no accepted value;
- explicit different years never merge;
- copied/syndicated pages do not count as independent corroboration.

### Taxonomy

- all axes contain only vocabulary values for approved
  `kenigevents-festivals@1.0.0`;
- the taxonomy version and hash exactly match the host-mounted registry;
- raw/unmapped topics are quarantined;
- A/B programme-profile disagreement requires C or review;
- `not_festival` cannot produce an edition;
- `unknown` never silently becomes `festival`, `identity_only` or `programme_only`.

### Programme materialization

- only `link_existing_event` and approved `create_event_candidate` may affect
  Event relations;
- every Event candidate passes existing Smart Update independently;
- schedule/programme/continuous/service rows never become Event through local
  keyword rules;
- one subscription/pass URL never becomes a single Event ticket;
- duplicate source rows reconcile to one stable programme item;
- the union of accepted A/B item inventories is conserved: every subject is
  linked, retained, rejected with evidence, or explicitly unresolved;
- a profile change that changes Event count or item disposition requires
  operator review during preproduction.

### Revision/apply

- shadow runs change no Festival/Event/public projection;
- approved revision activation is atomic;
- same input fingerprint/candidate hash is idempotent;
- failure never replaces the last approved revision;
- legacy/calendar compatibility projections are generated after approval, not
  treated as evidence sources.

## 9. Evaluation plan

### Recovery audit

At exact `940fea2e`, the 21 reviewed TypeScript rows test summary
identity/topic/media migration:

- exact 2026 calendar inventory: 21;
- optional `internalEventId`: 4;
- no `festivalId`, structured programme or typed date precision;
- free-text topic labels: 16;
- current state distribution: 9 announced, 8 programme-pending, 4
  date-pending;
- seven derived date-label/status combinations, none declared as a taxonomy.

The later DB-backed successor adds:

- `festival_id`: 9/21;
- `internal_event_id`: 4/21;
- either backend link: 12/21;
- date precision: 17 exact, 2 month, 1 month-range, 1 start-only.

All 21 must be represented as edition candidates without inventing programme
items or exact dates. Unlinked rows remain identity-review work, not automatic
new series.

### Programme golden pack

At least 14 source bundles, two per programme profile:

1. identity/dates only;
2. one compound day with internal timetable;
3. multiple separately ticketed events;
4. one-ticket stage schedule;
5. hybrid programme;
6. continuous fair/exhibition/zones;
7. distributed seasonal cycle.

Each pair includes one adversarial variant: stale year, generic ticket shell,
subscription vs single ticket, same title on different dates, incomplete PDF,
conflicting times, a current page with a separate historical section or an
ordinary event falsely branded as a festival.

### Acceptance metrics

- unsupported critical claims: `0`;
- stale-edition leakage: `0`;
- programme-profile exact match against reviewed gold: `100%` before apply;
- item-disposition macro precision: `>= 0.98` in shadow;
- Event-candidate auto-apply precision: `1.00`;
- schedule/programme item loss: `0` for source-grounded accepted rows;
- ticket-scope mismatch: `0`;
- exact-quote coverage of accepted critical fields: `100%`;
- 21-row calendar identity/date precision preservation: `100%`;
- typical Antigravity calls/group: `2`, hard cap `3`;
- median actual agent tokens/group: `<= 60k`, p95 `<= 90k`;
- terminal or checkpoint-recoverable output: `>= 95%`;
- zero public mutation in shadow.

Recall is measured and reported, but precision wins: an unresolved Event item
stays in programme/review instead of being guessed into the public Event table.

### Request and payload budgets

`max_total_tokens` is best-effort and is not the limiter reservation:

| Role | Sources/fetches | Search | Agent target | Conservative reservation |
|---|---:|---:|---:|---:|
| A | 6 / 8 | 1 | 18–20k | 45–50k |
| B | 4 / 4 | 1 | 10–12k | 25–30k |
| C | 0 / 0 | 0 | 6–8k | 15–20k |

Normalized source text is capped at 12k characters/source and 60k characters
per mounted packet; one quote is capped at 500 characters; C receives at most
20 conflicts and 10k characters. Calls remain sequential, and actual finalized
usage is checked before the next reservation.

## 10. Migration and rollout

### Phase 0 — contract/shadow

- implement schemas, validators and offline fixture replay;
- collect `festival-edition-v2` candidates in object storage/run tables;
- no production schema mutation and no apply.

### Phase 1 — identity foundation

- add series, edition, revision and stable edition↔Event relation tables;
- backfill legacy Festival rows as reviewable series/edition candidates;
- import 21 calendar rows as reviewed edition candidates with preserved date
  precision and raw topic labels;
- do not auto-link name-only collisions.

### Phase 2 — evidence and programme

- add durable source/snapshot/claim and programme tables;
- convert `activities_json` to `programme_only` candidates only when source
  provenance survives; otherwise mark review;
- run Antigravity shadow over bounded non-social queue groups.

### Phase 3 — approval-gated activation

- operator reviews sources, taxonomy, dispositions and candidate diff;
- approved revision atomically becomes effective;
- only approved Event candidates enter Smart Update;
- refresh legacy and calendar compatibility projections.

### Phase 4 — data readiness for future detail pages

An edition is detail-data-ready when:

- stable series/edition identity exists;
- current revision is approved and source-backed;
- date precision/lifecycle are honest;
- programme profile is supported or explicitly `identity_only`;
- every programme item has a disposition and provenance;
- organizers/venues/offers/media declare scope and source evidence;
- conflicts/unknowns are preserved;
- a bounded public JSON projection can be generated deterministically.

This gate does not require choosing or implementing a page template.

## Definition of Done for this design

- the calendar branch is treated as a reviewed projection, not hidden canonical
  truth;
- programme structure and thematic category are separate taxonomies;
- seven programme profiles and seven item dispositions have explicit behavior;
- series/edition/revision/source/claim/programme/Event relations are defined;
- Antigravity A/B/C has bounded, checkpoint-first responsibilities;
- quality gates fail closed on disagreement and unsupported facts;
- normal cost remains two calls, maximum three;
- future detailed pages can consume one versioned approved JSON without parsing
  sources or guessing semantics.
