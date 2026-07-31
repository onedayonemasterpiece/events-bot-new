# Festival Data Model v2: discovery topology, evidence and Event materialization

Status: `proposed preproduction contract`; no production migration or public
festival detail pages are included in this change.

Scope: the canonical JSON and persistence model needed to collect enough
source-backed data for future detailed festival pages. Page layouts and the
number of visual page templates are explicitly out of scope.

Related contracts:

- current festival model and queue: [`README.md`](README.md);
- Antigravity-primary non-social research:
  [`../source-parsing/sources/festival-parser/preproduction-web-research.md`](../source-parsing/sources/festival-parser/preproduction-web-research.md);
- reusable evidence-first prompts:
  [`../../llm/antigravity-festival-research.md`](../../llm/antigravity-festival-research.md);
- fresh acceptance cohort and «Балтийская Ухана» expected dispositions:
  [`../source-parsing/sources/festival-parser/antigravity-primary-evaluation.md`](../source-parsing/sources/festival-parser/antigravity-primary-evaluation.md);
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

The remembered seven-type model is the discovery/UI topology developed for the
static festival page research on `2026-07-23`:

```text
series_season
lineup
grid_showcase
territory
market
route_promenade
network_pass
```

It classifies a festival by the user's primary **discovery unit**, not by genre,
duration or Event count. `route_promenade` has two substantially different UI
subtypes — curated route and free promenade — so seven taxonomy values may
produce eight UI archetypes. `unknown` is a research state, not an eighth
topology. The earlier `identity_only|single_compound_event|...` proposal is
retained below only as a separate `programme_structure` representation axis;
it is not the seven-type festival taxonomy and does not choose UI by itself.

A later successor (`0abe04ab`, not a descendant of `940fea2e`) moves the
calendar into `festivalTimelineSeed.json`/`festival_calendar_item`, adds the
four date-precision values and optional `festival_id`. In the current reviewed
seed those links cover 9/21 Festival rows and 4/21 Event rows; one item has both,
so only 12/21 have either backend link. Both histories prove a good
calendar/provenance contract, not a complete series/edition/programme model.

The to-be design must not replace those dimensions with one overloaded enum.
It uses:

1. stable **series** and **edition** identity;
2. exactly one seven-value `primary_topology`, plus optional secondary
   topologies and an explicit discovery unit;
3. a separate programme representation mode and orthogonal
   identity/topic/time/space/access/mechanics/lifecycle/completeness facets;
4. a seven-value item disposition that decides whether a programme row becomes
   an Event, stays a schedule row, or is rejected;
5. atomic source claims and immutable revisions;
6. generated index/detail/manifest JSON projections from one effective
   revision as the only future page inputs.

Antigravity is the planned primary collector for the new non-social web
pipeline from its first implementation. It remains disabled and approval-gated
until its own acceptance gates pass; “primary” describes routing, not automatic
public writing. The built Kaggle+Gemma parser has never been production-run and
is not part of this implementation/acceptance plan. The intended end-state
assigns it the fallback role, but enabling that route is a separate future
repair/conformance/acceptance project. Local code owns input bounds, schemas,
exact-quote/reference validation, reconciliation, revisioning and the
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

The built but production-disabled Kaggle+Gemma Universal Festival Parser is
capable of writing UDS v1 directly into the flat Festival row. If invoked, it
matches by `source_url` or case-insensitive name and updates non-null values.
Its URL programme rows would be stored wholesale in `activities_json`; unlike
the VK programme path, they are not routed through Smart Update. This unproven
code path is future fallback material, not a dependency of Antigravity, and it
cannot express:

- series vs edition identity;
- several editions with stable redirects;
- multiple sources and immutable snapshots;
- a fact-level conflict or its evidence;
- a programme section/track/day/stage hierarchy;
- an item that is simultaneously a schedule row and a link to an Event;
- independent programme, ticket, venue and media freshness;
- an approved revision distinct from a newly researched candidate;
- a controlled taxonomy version.

At `940fea2e` the existing reasoning prompt does ask for one of eight theme
strings (`music|film|theater|art|literature|food|sport|other`). However
`UDSFestival` does not declare that field; its default nested model behaviour
discards the undeclared value, while the outer `UDSOutput.extra="allow"` does
not make it a typed Festival field. The server upsert does not persist it.
This is neither the remembered seven-value taxonomy nor durable
classification.

## 3. Canonical discovery topology and orthogonal structure

The page research defines the seven values of `primary_topology`. The
classifier asks **what the visitor chooses first**. Duration, genre, number of
venues, access price and the organizer's use of the word “event” are only
supporting evidence.

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

### T2 — seven-value primary discovery topology

| Value | First user choice | Typical structure | Default Event guardrail |
|---|---|---|---|
| `series_season` | independent event, topic, speaker or format | dates spread over weeks/months | eligible child items usually become Events when the independent-action gate passes |
| `lineup` | day, artist, concert block or composition | predominantly sequential programme | festival day/ticketed concert block may be Event; individual artist slots under shared admission are schedule rows |
| `grid_showcase` | session, performance, section or programme block | parallel streams/venues, competition or showcase | separately attendable session/block may be Event; constituent works stay nested |
| `territory` | common visit to a bounded site and its experiences | continuous zones plus timed anchors | zones/workshops/general attractions are not Events; only separately actionable anchors may become Events |
| `market` | participant, product, dish or offer | participant/product catalogue plus supporting stage | participants/products are never Events; an independently bookable workshop/performance may be |
| `route_promenade` | event/place/route point in meaningful geography | curated route or open promenade | separately actionable event-place pair may be Event; permanent objects/route points are not |
| `network_pass` | institution or route through an institution network/pass | independent institution schedules under common rules | institutions are not Events; exact-time separately actionable institution programmes may be |
| `unknown` | evidence insufficient or conflicting | no safe topology | no automatic Event materialization |

`route_promenade` has `route_subtype=curated|free_promenade|unknown`. This is
why the future renderer can have eight UI archetypes while the canonical
primary taxonomy still has seven values.

A festival may also have `secondary_topologies[]`. They add catalogue, map,
schedule or pass modules but never override the primary discovery path. The
primary topology is selected by the module that most quickly lets the visitor
make the central decision.

### T3 — discovery and programme mechanics

The topology is stored with the fields used by the research model:

```text
discovery_unit
  event | day | artist | program_block | zone | participant | product |
  place | institution | pass | unknown

time_mode
  independent_dates | sequential | parallel | continuous_with_anchors |
  open_hours | route_based | institution_schedules | unknown

space_mode
  single_venue | bounded_site | multi_venue_city | regional_route |
  linear_public_space | institution_network | unknown

access_mode[]
  free | registration_per_event | ticket_per_event | day_ticket |
  festival_ticket | festival_pass | mixed | unknown

program_mechanics[]
  competition | culmination | quest | festival_pass | archive_recordings |
  repeating_sessions | all_day_activities | transfer_required | age_routes |
  participants_award | program_tracks

data_completeness
  confirmed_full | confirmed_partial | preliminary | schedule_pending |
  conflicting
```

`discovery_unit` is one primary value in the canonical projection. Alternative
supported units are stored as secondary topology evidence rather than as an
ambiguous list in the primary field.

### T4 — programme structure is not the seven-type taxonomy

`programme_structure` describes how accepted subjects are represented. It is
reconciled **after** item dispositions and may change when the programme is
published:

```text
identity_only
single_compound_event
standalone_events
schedule_only
hybrid
continuous_experience
distributed_cycle
unknown
```

These values determine required storage capabilities, not page taxonomy:

| Structure | Stored shape |
|---|---|
| `identity_only` | edition identity, dates/lifecycle/sources; no invented programme |
| `single_compound_event` | one common visit/admission with an internal timetable |
| `standalone_events` | independently actionable items/occurrences and Event relations |
| `schedule_only` | ordered shared-admission slots without child Event materialization |
| `hybrid` | Event-linked items plus schedule/programme/continuous subjects |
| `continuous_experience` | zones/activities/opening windows and optional timed anchors |
| `distributed_cycle` | non-contiguous occurrences, venues/tracks and Event relations |
| `unknown` | insufficient/conflicting programme evidence; review only |

There is deliberately no one-to-one crosswalk. For example, `lineup` can be
`schedule_only` or `hybrid`; `territory` can be `continuous_experience` or
`hybrid`; `series_season` commonly becomes `standalone_events` or
`distributed_cycle`.

### T5 — controlled topics

Free research/calendar labels are retained as `raw_labels` and mapped into
versioned controlled IDs. A festival may have one `primary_topic_id` and
several `secondary_topic_ids`; music/food/history is never used as a topology.
Unknown Antigravity proposals remain in `unmapped_topic_labels` until an
operator approves a taxonomy version.

### T6 — date, place, access and lifecycle facts

Exact dates, times, coordinates, venue relations, offers and access scopes are
stored as claims. The following derived values remain useful for filtering and
compatibility:

```text
temporal_profile
  single_day | consecutive_range | non_contiguous_dates |
  recurring_schedule | continuous_range | seasonal_window | unknown

spatial_profile
  single_venue | campus_multi_stage | city_multi_venue |
  regional_distributed | touring | online_or_hybrid | unknown

lifecycle_state
  announced_dates_only | programme_partial | programme_published | sales_open |
  changed | postponed | cancelled | completed | unknown
```

`date_precision` remains
`exact|month|month_range|start_only|year|unknown`. Access is scoped to edition,
day, pass, programme item or occurrence. Missing price never means free; a
festival pass/subscription is never copied to an individual Event ticket.
`access_mode[]` plus scoped offers are canonical; no duplicate
`access_profile` taxonomy is stored.

### Versioned taxonomy registry

The host-owned registry contains at least:

```json
{
  "schema_version": "festival-taxonomy-registry-v2",
  "taxonomy_id": "kenigevents-festivals",
  "taxonomy_version": "2.0.0",
  "axes": {
    "identity_kind": {"cardinality": "exactly_one", "values": ["festival", "festival_cycle", "civic_programme", "holiday_programme", "fair_or_market", "competition_or_showcase", "not_festival", "unknown"]},
    "primary_topology": {"cardinality": "exactly_one", "values": ["series_season", "lineup", "grid_showcase", "territory", "market", "route_promenade", "network_pass", "unknown"]},
    "route_subtype": {"cardinality": "zero_or_one", "values": ["curated", "free_promenade", "unknown"]},
    "secondary_topologies": {"cardinality": "zero_or_more", "values_ref": "primary_topology"},
    "discovery_unit": {"cardinality": "exactly_one", "values": ["event", "day", "artist", "program_block", "zone", "participant", "product", "place", "institution", "pass", "unknown"]},
    "programme_structure": {"cardinality": "exactly_one", "values": ["identity_only", "single_compound_event", "standalone_events", "schedule_only", "hybrid", "continuous_experience", "distributed_cycle", "unknown"]}
  },
  "item_dispositions": [
    {"value": "create_event_candidate", "allowed_apply_action": "smart_update_only"}
  ]
}
```

The manifest stores the registry path, byte hash, schema version and approval
metadata. Patch versions may add aliases/wording; added values require a minor
version; semantic split/merge or meaning change requires a major version. Agent
proposals never mutate the registry.

## 4. Programme subject inventory and Event materialization

### Entity roles

Organizer wording often calls unlike things “events”. Antigravity first assigns
each extracted subject an entity role:

```text
child_event
programme_block
temporal_anchor
activity_or_zone
participant
work
route_point
product_or_offer
service_information
```

- a programme block may become a child Event while its films/works remain
  nested;
- a temporal anchor is useful in the festival-day timeline but does not need a
  separate page by default;
- activities/zones, participants, works, route points and products are stored
  in their own catalogue/spatial/programme structures and are not mass-created
  as Events.

### Seven action dispositions

Every programme item receives exactly one disposition:

| Disposition | Meaning | Apply action |
|---|---|---|
| `link_existing_event` | independently attendable item already exists | stable edition/item/occurrence→Event relation |
| `create_event_candidate` | independently attendable item passes the normative gate below | Smart Update only after approval |
| `schedule_slot` | named/timed part of common admission/container | schedule/timeline, no Event |
| `programme_only` | meaningful subject without Event-grade independence/logistics | programme block, no Event |
| `continuous_activity` | zone/exhibition/fair/installation over a range | hours/range, no automatic Event |
| `service_information` | doors, transfer, registration desk, break or logistics | service context only |
| `reject` | stale, duplicate, unrelated or unsupported | keep evidence/reason; never publish as programme |

### Normative child Event gate

Topology is a prior/guardrail, never a shortcut. A subject may be proposed as
`create_event_candidate` only when gates 1–6 pass and the evidence-validation
part of gate 7 passes. Candidate collection may leave operator approval
`pending`; no Event relation or write is allowed until **all seven** gates pass:

1. **Current-edition identity:** source and subject belong to the researched
   edition; not an archive, another year, navigation card or duplicate.
2. **Independent public choice:** a visitor can intentionally choose this unit
   independently of the rest of the festival. Evidence is an item-specific
   ticket/registration, an item-specific official detail/anchor with its own
   call to action, or an explicit standalone announcement. Shared festival
   admission alone is not enough.
3. **Event-grade occurrence:** explicit date/session and start time plus a
   source-backed venue/route point, which may be inherited from the edition only
   when the source explicitly places the item there.
4. **Meaningful identity:** a stable title/topic/programme block exists beyond
   only an artist name, zone name or generic label such as “мастер-классы”.
5. **Access compatibility:** ticket/registration scope matches this item;
   festival/day/pass/subscription URLs are not misrepresented as item tickets.
6. **Topology guardrail:** the result is compatible with the chosen topology
   and entity role; e.g. an artist slot in a shared lineup, a market participant,
   a museum in a pass network or a permanent art object cannot become Event.
7. **Evidence and apply authority:** claims/decisions pass the host validator;
   then an operator approves the candidate and it independently passes Smart
   Update. Before those apply steps its state is
   `candidate_pending_approval`, not an Event.

Duration `>=45m`, a named performer, an explicit format, a venue or a start
time is supportive evidence but none is independently sufficient. This
supersedes the older broad “date + time + location + one strong signal” rule,
which could over-create artist slots and internal festival activities.

The edition page itself is not automatically duplicated into `event`. An
optional `umbrella_event` compatibility relation may be approved when the
ordinary Event calendar genuinely needs one bookable/attendable compound
visit; it is separate from child Event decisions.

### Topology-specific materialization defaults

| Topology | Expected Event layer |
|---|---|
| `series_season` | approved independent lectures/concerts/tours/screenings |
| `lineup` | ticketed/independently announced festival day or concert block; never every artist slot |
| `grid_showcase` | separately attendable session/performance/programme block; works nested |
| `territory` | rare separately actionable anchor; zones/all-day activities remain non-Event |
| `market` | independently bookable workshop/performance only; participants/products remain catalogue entities |
| `route_promenade` | separately actionable event-place pair; permanent points/objects remain spatial entities |
| `network_pass` | separately actionable exact-time institutional programme; institution/pass visit itself is not Event |


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
primary_topology
route_subtype
secondary_topologies_json
discovery_unit
time_mode
space_mode
access_modes_json
program_mechanics_json
data_completeness
programme_structure
primary_topic_id
topic_ids_json
temporal_profile
spatial_profile
lifecycle_state
taxonomy_version
taxonomy_sha256
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
collector_manifest_json # Antigravity A/B/C interactions, checkpoints and failures
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

Provider-neutral operational runs remain in
`festival_web_research_run/lane_run/item/source`. Durable accepted evidence is
copied/referenced through:

```text
festival_edition_source
  id, edition_id, canonical_url, source_role, authority_status,
  edition_status, first_seen_at, last_seen_at, current_snapshot_id

festival_source_snapshot
  id, edition_source_id, content_sha256, artifact_path, retrieved_at,
  content_type, fetch_mode, http_status, run_uid

festival_claim
  id, revision_id, lane_run_id, snapshot_id, subject_kind, subject_key, field_path,
  raw_value_json, normalized_value_json, normalization,
  verbatim_quote, quote_start, quote_end, normalizer_version,
  context_quote, status

festival_decision
  id, revision_id, origin_lane_run_id NULL, decision_key, decision_kind,
  subject_kind, subject_key,
  selected_value_json, alternatives_json, evidence_claim_ids_json,
  reason_codes_json, status, actor_kind, contract_version, created_at,
  UNIQUE(revision_id, decision_key)
```

`origin_lane_run_id` is set only for a decision made wholly inside one
collector lane. It is `NULL` for host reconciliation/operator decisions, whose
`evidence_claim_ids_json` may intentionally cite claims from several lane runs;
`actor_kind` distinguishes `lane_model`, `host_reconciler` and `operator`.

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
  id, revision_id, stable_key, section_id, entity_role, disposition, item_kind,
  title, date_precision, access_json, participant_refs_json, description_facts_json,
  source_claim_ids_json, decision_ids_json, event_gate_json,
  identity_hash, display_order, status

festival_programme_occurrence
  id, programme_item_id, occurrence_key, start_at, end_at, date_precision,
  venue_refs_json, access_override_json, source_claim_ids_json, status,
  UNIQUE(programme_item_id, occurrence_key)

festival_programme_item_event
  programme_item_id, programme_occurrence_id, event_id, relation_status,
  decision_id, evidence_claim_ids_json, created_at, updated_at

festival_edition_event
  edition_id, event_id, relation_kind, relation_status, decision_id,
  evidence_claim_ids_json, created_at, updated_at
```

`stable_key` is generated from accepted edition-local identity evidence and is
preserved across revisions when the same item is matched. It is not a hash of
mutable description copy. `festival_edition_event.relation_kind=umbrella_event`
is optional compatibility, never an automatic consequence of creating an
edition.

`event_gate_json` records gates 1–6 and the evidence-validation part of gate 7
as `pass|fail|unknown|not_applicable` with claim/decision references. It expands
the apply-authority part of gate 7 into host-owned `operator_approval` and
`smart_update`; both remain `pending` on a collected candidate and only those
states can advance an Event relation to approved/applied.

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

festival_publication_bundle
  id, schema_version, revision_set_sha256, state,
  index_artifact_path, index_sha256, manifest_artifact_path, manifest_sha256,
  created_at, activated_at

festival_publication_artifact
  id, bundle_id, edition_id, revision_id, artifact_kind,
  artifact_path, content_sha256, schema_version, readiness_json,
  UNIQUE(bundle_id, edition_id, artifact_kind)
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
  "taxonomy_version": "2.0.0",
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
    "primary_topology": {
      "value": "lineup",
      "claim_ids": ["C030", "C031", "C050"],
      "decision_ids": ["D001"],
      "status": "supported"
    },
    "route_subtype": null,
    "secondary_topologies": {
      "values": ["territory"],
      "claim_ids": ["C030", "C040"],
      "decision_ids": ["D001"],
      "status": "supported"
    },
    "discovery_unit": {
      "value": "day",
      "claim_ids": ["C030", "C050"],
      "decision_ids": ["D001"],
      "status": "supported"
    },
    "time_mode": {"value": "sequential", "claim_ids": ["C030"], "decision_ids": ["D001"], "status": "supported"},
    "space_mode": {"value": "bounded_site", "claim_ids": ["C040", "C041"], "decision_ids": ["D001"], "status": "supported"},
    "access_modes": {"values": ["day_ticket", "festival_ticket"], "claim_ids": ["C050", "C051"], "decision_ids": ["D001"], "status": "supported"},
    "program_mechanics": {"values": ["program_tracks"], "claim_ids": ["C030"], "decision_ids": ["D001"], "status": "supported"},
    "data_completeness": {"value": "confirmed_full", "claim_ids": ["C030", "C040", "C050"], "decision_ids": ["D001"], "status": "supported"},
    "programme_structure": {
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
          "entity_role": "child_event",
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
          "event_gate": {
            "current_edition": {"status": "pass", "claim_ids": ["C003"]},
            "independent_choice": {"status": "pass", "claim_ids": ["C050"]},
            "event_grade_occurrence": {"status": "pass", "claim_ids": ["C032", "C040"]},
            "meaningful_identity": {"status": "pass", "claim_ids": ["C031"]},
            "access_compatibility": {"status": "pass", "claim_ids": ["C050"]},
            "topology_guardrail": {"status": "pass", "claim_ids": ["C030", "C031"]},
            "evidence_validation": {"status": "pass", "claim_ids": ["C003", "C030", "C031", "C032", "C040", "C050"]},
            "operator_approval": {"status": "pending", "decision_ids": []},
            "smart_update": {"status": "pending"},
            "apply_status": "candidate_pending_approval"
          },
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
        "primary_topology": "lineup",
        "secondary_topologies": ["territory"],
        "discovery_unit": "day",
        "time_mode": "sequential",
        "space_mode": "bounded_site",
        "access_modes": ["day_ticket", "festival_ticket"],
        "program_mechanics": ["program_tracks"],
        "data_completeness": "confirmed_full",
        "primary_topic_id": "music",
        "secondary_topic_ids": ["performing_arts"],
        "temporal_profile": "consecutive_range",
        "spatial_profile": "campus_multi_stage",
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
      "decision_kind": "programme_structure",
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
  "serving": {
    "render_profile": "lineup",
    "render_profile_version": "festival-render-profile-v2",
    "index_ready": true,
    "detail_ready": false,
    "event_apply_ready": false,
    "blocking_reasons": ["CF001"]
  },
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
| `identity` | exactly one series and edition key; every non-null source-derived public identity scalar has `claim_ids` |
| `classification` | one primary topology plus the required orthogonal fields; multi-value fields use `values`; every semantic mapping has `decision_ids` and evidence claims |
| `parties` | zero or more unique `party_key`; requires kind and claim-backed name; organizers and item participants reference an existing party |
| `organizers` | zero or more edition-scoped `(party_ref, role)` relations with claims |
| `venues` | zero or more unique `venue_key`; requires role/name status; unresolved address/geo stays null/unknown |
| `programme_sections` | zero or more stable-keyed tree nodes; every item belongs to exactly one section, has exactly one disposition, an explicit Event-gate ledger and zero or more uniquely keyed occurrences |
| `offers` | zero or more unique `offer_key`; requires scope/kind; item-scoped offer references one existing programme item; unknown price never means free |
| `media` | zero or more hash/provenance-bound assets; requires role, source, dimensions when known and rights status |
| `sources` | at least one for a data-ready revision; unique `source_id`, canonical URL, snapshot hash, normalizer version, role and edition status |
| `decisions` | zero or more unique `decision_id`; all referenced IDs resolve inside the same revision and cite existing claims |
| `conflicts` | alternatives cite existing claims/sources; unresolved blocking conflict makes `publishable=false` |
| `unknowns` | machine path, reason code and needed evidence; never filled with a guessed default |
| `serving` | derived render hint and independent index/detail/Event readiness; never a source fact |

Stable keys, schema/taxonomy/hash metadata, host-normalized date/time
components, controlled enums selected by an evidence-backed decision and
deterministic serving/quality values are not source facts and need not use the
scalar claim wrapper. Their schema rules still require the corresponding
decision/claim references or a pinned host derivation. This is why, for
example, `disposition` is decision-backed while `serving.detail_ready` is
host-derived.

All `party_ref`, `venue_ref`, `offer_ref`, `source_id`, `decision_id`,
programme item and Event references are validated as a closed graph inside the
projection. Every `claim_id` resolves against the revision's hash-bound
`claims.ndjson` from `evidence_manifest`. Empty arrays mean “none found in
accepted evidence”, not “known absent”, unless a claim/decision explicitly
supports absence.

`display_label` and public narrative copy are later projections. Antigravity
may extract source wording and facts, but it does not write
unsupported public copy into canonical facts.

## 7. Serving projections for the index and future detail pages

The current static `/festivali/`, Telegraph festival index and Telegraph
festival detail compute from different representations. The static index reads
`festival_calendar_item`; Telegraph joins the flat Festival row to Events by
the string `Event.festival`; `activities_json` is not rendered. V2 must publish
all future surfaces from one effective edition revision plus a small reviewed
index-presentation overlay.

### `festival-index-v2.json`

Top-level `schema_version=festival-index-v2`; one compact row per index-visible
approved edition:

```text
seriesKey, editionKey, seriesSlug, editionSlug, detailPath
title, shortSummary
date(start, end, precision, label, sortDate, timezone)
lifecycleState, primaryTopology, secondaryTopologies, programmeStructure
discoveryUnit, primaryTopicId, secondaryTopicIds
spatialSummary(cities, venueCount, placeLabel)
accessSummary
cover(publicUrl, width, height, alt, rightsStatus)
counts(events, scheduleSlots, continuousActivities)
indexReady, detailReady, lastVerifiedAt
effectiveRevision, taxonomyId/version/hash
officialFallbackUrl
```

The index row never contains the programme tree, raw claims or source text.
Its card destination is `detailPath` only when the same publish manifest
contains a validated detail artifact; otherwise it uses the explicit official
fallback URL rather than generating a broken internal link.

### `festival-details/<edition-slug>.json`

Each artifact has `schema_version=festival-detail-v2`. This is the public
bounded projection of `festival-edition-v2`, not a second truth model. It
contains:

- series/edition identity and related-edition references;
- the seven-value `primary_topology`, secondary topologies, discovery unit and
  orthogonal programme/time/space/access/mechanics fields;
- `programme_structure` and a derived/versioned `render_profile`;
- dates, timezone, lifecycle and supported summary facts;
- organizers/parties, venues, scoped offers and public media;
- sections → items → occurrences with every item disposition;
- approved Event IDs/paths for linked occurrences;
- public source/freshness summary and conflict/unknown status without raw
  operator quotes or full source text;
- schema, taxonomy, revision and artifact hashes.

All seven discovery topologies and every programme structure use this one
superset schema. Page templates are out of scope; the future renderer may
choose or compose blocks from `render_profile` without changing persistence.
`render_profile` is host-derived and versioned: it normally equals the primary
topology; `route_promenade` expands to `route_curated` or
`route_free_promenade`. Thus the seven canonical topology values support eight
known UI archetypes without adding an eighth semantic festival type.

### `festival-manifest-v2.json`

Uses `schema_version=festival-manifest-v2` and maps `editionKey` to edition
slug, `detailPath`, effective revision hash,
artifact hash and readiness. Export is atomic:

```text
read one committed effective revision set
-> build index + detail artifacts + manifest in a temp candidate
-> validate schema, closed references and hashes
-> switch the whole set together
```

The general index cannot point at a detail artifact from another revision or a
file that was not published in the same manifest.

### Event compatibility projection

Static Event JSON gains a stable relation while retaining the current string:

```json
{
  "festival": "legacy compatibility name",
  "festivalRelation": {
    "seriesKey": "opaque-series-key",
    "editionKey": "opaque-edition-key",
    "name": "...",
    "path": "/festivali/.../",
    "relationStatus": "approved"
  }
}
```

### Independent readiness

- `index_ready`: stable edition, source-backed title, honest date/lifecycle,
  sort key, destination and public cover/fallback. Full programme is optional.
- `detail_ready`: effective approved revision, supported primary topology and
  programme structure (including honest `identity_only`), complete public
  programme disposition coverage,
  resolved references, scoped offers, public media rights and no blocking
  conflict.
- `event_apply_ready`: every Event candidate independently passes the existing
  Smart Update contract. A detail page may be ready while some unresolved
  Event candidates remain withheld.

`festival_calendar_item` remains a reviewed ordering/feature/cover overlay but
must eventually reference stable `edition_id`; it is not canonical dates,
taxonomy or programme truth.

## 8. Antigravity-primary collection topology

The new non-social pipeline has one planned collector: **Antigravity**. It is
`primary` from the first implementation because all eligible URL groups route
to it; it still starts disabled, collect-only and operator-approved. This is not
“shadow behind Kaggle”. Public mutation is withheld because Antigravity has not
yet passed acceptance, not because another URL parser is production authority.

Kaggle+Gemma is explicitly outside the current implementation plan. The code
exists but has never been production-run and is neither healthy standby nor
required baseline. A later project may implement a strict collect-only
Kaggle→v2 adapter and fallback acceptance. Until then, Antigravity technical
failure means retry/recovery/review while the last approved revision remains
serving.

Normal cost is two Antigravity interactions per grouped festival edition; hard
maximum is three:

```text
A — primary evidence researcher in a fresh environment
B — independent fresh-environment topology/Event checker
local compare
C — optional no-network adjudicator for a bounded valid conflict only
```

No stage creates an automatic fourth interaction. A and B never see one
another's result. C cannot research new facts, open URLs or rebuild the
candidate.

### Operational correction from the 2026-07-29 probe

The probe proved Interactions API reachability, remote tools, environment
persistence/download and factual value. It did not prove operational readiness:

| Interaction | Status | Actual tokens | Semantic checkpoint |
|---|---:|---:|---|
| A | `incomplete` | 78,948 | none |
| B | `incomplete` | 27,198 | raw sources only |
| C | `incomplete` | 44,804 | raw sources only |

All three known factual errors were recovered by manual/local inspection, not a
terminal agent result. Token budgets overshot, an unsupported `labels` field
caused HTTP 400 in another attempt, and the old artifact wrapper conflated
provider accounting finalization with semantic success. Therefore the next
implementation must first provide a real Interactions wrapper, immediate typed
checkpoints and host validation. No successful Antigravity festival extraction
is claimed yet.

### Call A — primary evidence researcher

A receives the frozen edition target, all grouped non-social seed URLs, bounded
normalized snapshots, registry/schema/validator and no previous candidate
narrative. It may perform one discovery query only when seed coverage is
insufficient and uses at most six accepted sources.

Mandatory checkpoint order:

```text
state.json
source_ledger.json
source_reviews/<source>.json
claims/<source>.jsonl
subjects/<source>.jsonl
topology_a.json
programme_inventory_a.jsonl
candidate_a.json
run_summary.json
```

A must classify identity, current edition, seven-value primary topology,
secondary topologies, discovery/time/space/access/mechanics/completeness,
programme structure, every extracted subject role and every programme-item
disposition. It writes after each source, not only at the end.

### Call B — independent checker

B receives the same frozen target/snapshots but not A's claims, topology,
dispositions or candidate. It uses at most one alternative search query and
four accepted pages. It independently returns:

- edition/currentness and source-role challenges;
- primary/secondary topology and discovery-unit decision;
- an independently conserved subject inventory;
- Event vs schedule/programme/continuous/service/reject dispositions;
- challenges for stale edition, unsupported title modifiers, access scope and
  false festival identity.

Omission is unresolved, not agreement.

### Local comparison and Call C

Host comparison requires evidence compatibility on edition identity, primary
topology and every critical disposition. It conserves the union of A/B subject
inventories. Field authority is source-specific: current official edition and
programme sources beat aggregators; item ticket/detail sources own only their
item; later explicit cancellation/change may supersede an older schedule.

C runs only when A and B provide two schema-valid, evidence-backed conflicting
alternatives. It receives values, claim IDs, exact quotes and source hashes;
network is disabled. It chooses an existing alternative or returns
`unknown|conflict`.

### Incomplete and technical failure

Usage is always finalized in the quota ledger, but semantic state is separate:

- terminal schema-valid result -> validate normally;
- `incomplete` with all mandatory schema-valid checkpoints -> recover locally;
- raw sources/state without semantic checkpoints -> `needs_review/retryable`;
- HTTP/auth/quota/runtime/snapshot failure -> retryable technical failure;
- low evidence, unknown topology or semantic conflict -> review, not technical
  fallback.

No Kaggle invocation is permitted by this design version.

## 9. Local quality gates

### Evidence and identity

- every accepted source is current-edition classified and hash-bound;
- exact quotes reproduce at recorded offsets under the pinned normalizer;
- candidate source facts exist in claims;
- old editions/navigation/aggregator leakage cannot contribute final scalars;
- title ordinal/status modifiers require current official evidence;
- a date conflict is preserved, never resolved by newest crawl time alone.

### Topology

- `primary_topology` is one of the seven registry values or `unknown`;
- classification cites discovery-unit evidence rather than genre/duration
  shortcuts;
- `route_promenade` declares its route subtype;
- secondary topologies do not replace the primary module;
- programme structure is derived from the accepted inventory, not treated as
  the seven-value UI taxonomy;
- unknown/unmapped classifications remain quarantined.

### Programme and Event materialization

- every A/B subject is linked, retained, rejected with evidence or unresolved;
- every programme item has entity role, disposition and decision evidence;
- only approved `link_existing_event`/`create_event_candidate` may affect Event
  relations;
- all seven normative child Event gates pass before Event relation/apply;
- lineup artists, market participants/products, pass institutions, route
  objects and territory zones never become Events by keywords;
- programme blocks may become Events while their constituent works remain
  nested;
- shared pass/day/festival/subscription access never becomes an item ticket;
- every Event candidate passes Smart Update independently;
- profile/topology/disposition disagreement is operator-reviewed in
  preproduction.

### Revision/apply

- no Antigravity collection mutates Festival/Event/public artifacts directly;
- only one approved revision/apply lock feeds public projections;
- same fingerprint/candidate hash is idempotent;
- failure never replaces the last approved revision;
- index/detail/manifest artifacts publish atomically from one revision set.

## 10. Evaluation plan

The dated acceptance cohort is specified in
[`../source-parsing/sources/festival-parser/antigravity-primary-evaluation.md`](../source-parsing/sources/festival-parser/antigravity-primary-evaluation.md).
At cutoff `2026-07-31`, production read-only inventory contains 31 defensibly
current pending non-social URL rows, grouped into 22 likely edition targets.
The cohort covers all seven topologies, multi-URL grouping, child-vs-parent
relations and false-positive festival identity. «Балтийская Ухана» is added
from its fresh official 2026 website/PDF as a high-value territory/lineup
materialization case.

### Acceptance metrics

- primary-topology exact match against reviewed gold: `100%` before apply;
- route subtype exact match on route cases: `100%`;
- unsupported critical claims: `0`;
- stale-edition leakage: `0`;
- child Event precision and recall against reviewed gold: `1.00` before any
  automatic apply;
- programme-item disposition macro precision: `>=0.98`;
- source-grounded subject loss: `0`;
- ticket/access-scope mismatch: `0`;
- exact-quote coverage of accepted critical facts: `100%`;
- typical calls/group: `2`, hard maximum `3`;
- median actual tokens/group: `<=60k`, p95 `<=90k` after wrapper accounting;
- terminal or mandatory-checkpoint-recoverable output: `>=95%`;
- `0` public mutations during collect-only acceptance.

The previous manual recovery of three errors is baseline evidence only, not a
passing run.

### Request budgets

Initial design targets:

| Role | Search/fetch | Target tokens | Conservative limiter reservation |
|---|---:|---:|---:|
| A | <=1 query, <=6 sources | 20k | 50k |
| B | <=1 query, <=4 sources | 12k | 30k |
| C | no network | 8k | 20k |

Feature cap remains 12 RPD, concurrency one. The implementation must enforce
this cap in addition to the registered shared safe limit. Actual finalized
usage, not requested `max_total_tokens`, controls the next reservation.

## 11. Planned rollout — implementation is a separate command

This document does not implement or launch the pipeline.

### Phase 0 — contracts and offline harness

- implement generic Interactions API wrapper with idempotent create/poll/resume;
- expose safe limiter lease/finalize without treating `incomplete` as semantic
  success;
- implement checkpoint manifest extraction, hashes, redaction and validators;
- freeze taxonomy v2 and canonical JSON Schema;
- build reviewed fixtures from the 22-group cohort and «Балтийская Ухана»;
- no provider/public mutation where saved artifacts suffice.

### Phase 1 — manual Antigravity-primary collect-only

- one current group at a time;
- A+B; C only on a valid conflict;
- operator reviews source ledger, topology, complete subject inventory and every
  Event disposition;
- require five diverse successful live groups including a no-site/weak-site
  case and «Балтийская Ухана»;
- no public apply.

### Phase 2 — scheduled approval-gated canary

- maximum two changed groups/run, concurrency one;
- seven consecutive days;
- quota/checkpoint/latency reports;
- explicit retries and unchanged-fingerprint zero-call reuse;
- every candidate manually approved/rejected.

### Phase 3 — unified approved apply

- activate one immutable approved revision;
- route approved Event candidates through Smart Update;
- publish compatibility and atomic index/detail/manifest projections;
- keep auto-apply disabled until the precision gates remain perfect on a larger
  reviewed sample.

### Future independent project — Kaggle+Gemma fallback

Not part of Phases 0–3 and not an Antigravity acceptance prerequisite. It would
require its own strict UDS/schema repair, source-bound claims, collect-only
adapter, full-v2 conformance, live health canary and forced failover tests before
being called a reserve. The target end-state then routes an Antigravity
technical failure or quality-gate rejection to this accepted fallback; it does
not let either collector direct-write or turn cross-collector disagreement into
automatic truth. Until that separate project is accepted, Antigravity failure
cannot route to Kaggle.

## Definition of Done for this design

- the correct seven discovery topologies are canonical and distinct from
  programme structure, topics and Event dispositions;
- topology-specific Event guardrails and one normative child Event gate are
  explicit;
- one superset edition/revision/evidence/programme model supports all types;
- Antigravity is the sole planned primary collector, bounded to A+B(+C);
- actual probe limitations are stated without claiming readiness;
- the fresh non-social queue cohort and «Балтийская Ухана» ground the future
  acceptance pack;
- Kaggle+Gemma is neither falsely called production nor included in current
  implementation work;
- index/detail/Event readiness remain independent and atomic serving projections
  consume one approved revision;
- no runtime implementation, queue mutation or provider run is claimed in this
  design-only change.
