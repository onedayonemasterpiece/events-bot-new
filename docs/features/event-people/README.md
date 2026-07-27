# Event people: артисты, спикеры, хедлайнеры и «К нам едут»

> **Status, 2026-07-27:** product/architecture analysis; production person
> registry, public likes and collection are not implemented. Existing
> person-related slices are inventoried below.
>
> **Related:** [event token medallions](../static-site-pages/event-token-medallions.md),
> [reaction counters](../static-site-pages/reaction-counters.md),
> [personalization ownership](../../architecture/personalization-data-ownership.md),
> [guide excursions](../guide-excursions-monitoring/README.md).

## Product decision

The event detail page may show a source-grounded avatar medallion for an artist,
speaker, host or other public participant. The person/group can be liked; the UI
must explain the effect truthfully: future events with the liked participant
will receive a bounded ranking boost. A like is not called a subscription and
does not promise a notification.

The public visiting-participant collection is called **«К нам едут»**. On
mobile it belongs to a nested `Подборки` plane rather than adding another
top-level row to the global menu. Local public figures can appear on event
details and in personalized feeds, but being local does not qualify them for
«К нам едут».

Use a generic public-figure entity with
`entity_kind=person|group|project`, not a person-only table: event headliners
also include bands, ensembles and creative projects.

## What already exists

| Slice | State | Reusable part | Missing part |
|---|---|---|---|
| Event detail medallions | implemented for curated organizer, venue, festival, source and factual tokens | large circle contract, asset manifests, provenance and fail-closed resolution | participant/avatar resolver and interactive person state |
| VK festival celebrity carousel | implemented | LLM extraction of named person+role, operator overrides, image-evidence gate and person cards | no canonical identity, aliases, durable event-person links or site likes |
| Smart Update facts | implemented | named speakers/performers and roles are preserved in grounded facts | no structured canonical `event_participant` projection |
| Festival/source parser UDS | partial/source-specific | `participants[]` exists in the extraction schema | not normalized into the core `Event` model |
| Guide monitoring | implemented separate contour | `guide_profile`, `guide_names_json`, `participant_profiles_json`; production already has a Tatyana Udovenko profile | links are guide-occurrence-specific and are not a generic event-person registry |
| Static reaction counters | event counter/sync slice implemented | Supabase ownership, RLS, build-time baseline plus small fresh manifest pattern | first-party event-like persistence is still follow-up; no person counter/state |
| KGD80 media/data | strong curated seed | names, event mapping, bios/regalia, aliases/social links and visual material | import/provenance pipeline into KenigEvents and two standalone portrait crops |

Therefore this is not a greenfield UI problem. Presentation, source evidence and
personalization ownership are mostly designed, while **canonical identity and
event-person resolution are the missing middle**.

## KGD80 seed audit

The sibling `kdg80` project currently provides:

- `51` festival event-to-speaker rows;
- `37` distinct speakers;
- `36` standalone speaker-photo manifest entries, of which `35` match the
  37-person festival cohort and one is an extra profile (Gennady Kretinin);
- event visual material for the two remaining cohort speakers, Vladimir Chechko
  and Shakhnoza Usmanova, so reviewed portrait crops can complete the set;
- short regalia/bios and richer aliases/social links.

This is enough for the first curated registry seed. Import must preserve
`source_project=kgd80`, the source key, image credit/rights/provenance and a
source revision. Media bytes belong in Object Storage/CDN; databases store only
metadata and stable asset URLs.

Production evidence on 2026-07-27:

- the main canonical event table has no genuine future event for the 37-person
  KGD80 cohort;
- Andrey Levchenkov is present in recent canonical events `6637` (2026-07-09),
  `6172` (2026-06-25, KGD80) and `6237` (2026-06-24);
- Tatyana Udovenko is present in canonical KGD80 event `3894` (2026-04-14);
- the separate guide-occurrence contour already has future Tatyana Udovenko
  occurrences `453` (2026-07-28), `452` (2026-07-30) and `314`
  (2026-08-09).

The split proves that the public registry needs an explicit projection from both
canonical events and guide occurrences; searching only `event.source_text`
would miss useful local-person inventory.

## Current visiting/headliner evidence

The production core contained `362` active current/future events at
2026-07-27 15:49 UTC. Keyword scanning is only candidate discovery because
country words often describe repertoire, biography or a film rather than a
visiting performer. Strong current examples with event-local performer evidence
include:

- `6272`, 2026-07-29 — PUPO, explicitly an Italian singer/composer;
- `5297`, 2026-07-30 — pianist Igor Sidorov (Russia; born in Saint Petersburg,
  currently studies in Moscow);
- `5298`, 2026-07-31 — Can Saraç, explicitly a Turkish pianist from Istanbul;
- `6979`, 2026-07-31 — the Moscow Danilov Monastery Patriarchal Male Choir;
- `7036`, 2026-07-31–2026-08-02 — Chris Galvez and Organ Trio (Chile), Butter
  Funk Family (USA), Lebron Johnson (Italy) and Funk’n’stein (Israel);
- `2863`, 2026-08-28 onward — named festival headliners Olga Peretyatko,
  Nikolai Lugansky, Daniil Kogan and others;
- `7087`, 2026-09-05–06 — the Saint Petersburg Eifman Ballet;
- `7220`, 2026-09-26 — “Romantic Italian Tenors”.

Other obvious touring candidates include Evgeny Petrosyan, Valery Syomin and
Vyacheslav Butusov, but the current schema has no normalized base region or
travel evidence. Nationality alone must not imply a visit: for example, an
event can label an established regional performer by country of origin.

For public «К нам едут» eligibility require:

1. an active future event in the target region;
2. a resolved participant link with event-local evidence;
3. `visitor_status=confirmed` from explicit origin/base/travel evidence, not
   fame or nationality alone;
4. a fresh source/content hash and no cancellation/merge conflict.

## Can Kaggle CPU + BGE replace Gemma/Gemini?

### Short answer

**For matching known identities: yes. For finding arbitrary unknown people from
raw text with BGE alone: no.**

BGE-M3 is a multilingual retrieval/embedding model. It can rank event evidence
against registry profiles, join Cyrillic/Latin descriptions and help with fuzzy
aliases, but it does not return a grounded name span, role or travel fact as a
generative extractor.

The no-provider CPU route is viable as:

```text
changed source/event
  -> cheap exact alias and bounded list/heading candidate mining
  -> optional CPU multilingual NER for unknown name spans
  -> BGE-M3 hybrid retrieval against versioned person profiles
  -> calibrated lexical/role safety head
  -> high-confidence event_participant import or unresolved candidate
```

Rules:

- exact curated alias wins before embeddings;
- BGE is candidate retrieval, not public truth;
- a public link keeps an evidence quote/span and role;
- transliteration and cross-script quality need a project gold set; never assume
  all variants are solved by semantic similarity;
- unknown spans may create a candidate, never a public celebrity profile;
- biographies, composers in repertoire, film casts and people merely quoted in
  prose are hard negatives;
- high-confidence automatic acceptance needs grouped holdout precision; target
  public-link precision is at least `98%`, with abstention preferred to a wrong
  face.

The repository already proves the operational pattern with the Event Age
BGE-M3 worker: a global coalesced job, quiet window, CPU Kaggle batch,
content-hash binding, callback/status ledger, bounded runtime, stale-result
rejection and partial follow-up. Person matching should reuse this control plane
instead of starting one Kaggle run per Smart Update.

### Asynchronous cadence

Person matching is eventually consistent and does not belong inside the
synchronous Smart Update transaction:

- after canonical event/source changes, move one coalesced job to a quiet
  window;
- run changed/missing hashes in a CPU batch every 2–4 hours, with one nightly
  completeness sweep;
- exact matches to already-known aliases may be materialized immediately
  without BGE;
- import rechecks the event hash, registry revision and model/policy hashes;
- a stale batch result cannot overwrite a newer Smart Update;
- target freshness for BGE-only enrichment is six hours; the static site remains
  valid without it.

Smart Update may later emit structured participant evidence in its **existing**
facts call at zero additional model calls. That output improves candidate
mining, but the asynchronous resolver remains independent and can run fully on
CPU.

## Storage decision

Do not choose one database for all concerns.

### Fly SQLite — canonical public facts

Store the small canonical registry and event links with the events they qualify:

- `public_figure(id, slug, entity_kind, display_name, bio_short, base_region,
  base_country, locality_status, avatar metadata, status, provenance...)`;
- `public_figure_alias(figure_id, alias, normalized_alias, script, language,
  provenance, confidence...)`;
- `event_participant(event_id, figure_id, role, billing_order, is_headliner,
  evidence_quote, evidence_source_url, visitor_status, confidence,
  event_content_hash, resolver_policy...)`;
- an equivalent projection/link for `guide_occurrence`.

Why: event lifecycle, merge/cancel repair, static export and public evidence are
already transactional in core SQLite. The expected registry is small, joins are
simple, and moving these links to a remote sidecar creates dual-write and stale
publication risks without a scale benefit.

### Personalization Supabase/Postgres — likes and ranking state

Store:

- current `(actor/user, figure_id)` like state;
- public first-party like counter per figure;
- bounded person affinity/profile inputs and ranking audit;
- a small public counter manifest/RPC protected by grants and RLS.

This follows existing data ownership: browser/user state does not belong in Fly
SQLite. Like toggles update optimistically, while a cached same-origin manifest
refreshes counts without rebuilding all static pages.

### Object Storage/CDN — media and batch artifacts

Store portraits/crops and immutable Kaggle input/output packages here. SQLite
keeps URLs, hashes, dimensions, credits and review state, not image blobs.

### YDB — optional batch sidecar, not source of truth

YDB is useful only if durable high-volume match history is needed:

- `person_match_run`;
- candidate/evidence rows with TTL;
- resolver diagnostics and de-identified analytics.

It should not own canonical identities, event links or likes. The current
catalog does not require a distributed database, YDB adds a network boundary to
the event publication transaction, and its documented vector index is not a
generally available user feature. For the first release, immutable batch
packages plus SQLite import are simpler than adding YDB. Add the YDB sidecar
only when candidate history/volume justifies it.

## UI and ranking contract

### Event detail

- Keep people in a dedicated `Участники` row adjacent to, but not mixed with,
  organizer/festival identity tokens.
- Show the headliner first, then up to two additional primary participants and
  a `+N` disclosure for large programs.
- Avatar opens the participant surface; a separate heart button toggles
  `Нравится` and exposes the count. Do not make one ambiguous tap both navigate
  and like.
- The control has a 44px minimum target, explicit accessible name and a
  one-time explanation: `Будем чаще показывать события с участием …`.
- Missing/uncleared photos fail closed to a neutral initials treatment; no
  scraped social portrait is presented as an approved asset.

### Ranking

A person like is a strong explicit preference, but its boost is capped:

- apply only to active future events with a verified `event_participant` link;
- cap the combined people boost so one prolific participant cannot monopolize
  the feed;
- preserve date relevance, explicit hides, event diversity and exploration;
- do not use popularity/like count as identity evidence.

### «К нам едут»

- Nested mobile route: `Подборки → К нам едут`.
- The collection contains confirmed visiting artists/speakers from both Russia
  and other countries; local people stay on their event/person surfaces.
- Sort primarily by event date and headliner strength, then personalization;
  do not call every foreign-origin resident a visitor.
- Empty/stale output must fail to the ordinary event catalog, not manufacture
  candidates.

## Delivery order

1. Import the 37 KGD80 profiles, complete the two reviewed portrait crops and
   manually link known KGD80/guide occurrences.
2. Add SQLite canonical entity/alias/link tables and static export; render
   read-only participant medallions.
3. Add Supabase person like state/counter plus bounded feed boost.
4. Launch `К нам едут` from manually confirmed visitor links.
5. Run CPU+BGE in shadow mode on changed events, measure precision/coverage and
   only then enable high-confidence automatic links.

This order ships visible value before the ML resolver and keeps wrong identity
or image joins out of the public product.

## External references

- [BAAI BGE-M3 model card](https://huggingface.co/BAAI/bge-m3) — multilingual
  dense/sparse/multi-vector retrieval and model limits.
- [YDB secondary indexes](https://ydb.tech/docs/en/concepts/query_execution/secondary_indexes)
  — current index behavior and vector-index availability note.
- [Supabase Row Level Security](https://supabase.com/docs/guides/database/postgres/row-level-security)
  — browser-facing table authorization contract.
