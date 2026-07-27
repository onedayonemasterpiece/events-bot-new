# Необычные события

> **Status, 2026-07-27:** R15 implementation candidate. Semantic, builder and
> Astro contracts are being integrated behind static/noindex release gates.
> Production-root enablement and a real Kaggle CPU canary are **pending**; this
> document does not claim that either has happened.

`/neobychnoe/` is a static discovery feed for attendable events whose **way of
participating, access, place, practice or combination of formats** is materially
uncommon. It is not a synonym for a rare genre, an exciting title, a fashionable
venue or the word “необычный”. A normal concert, play or screening remains a hard
negative unless the checked event facts establish an unusual experience
mechanism.

This is the canonical product, semantic, build and rollout contract. General
related/search retrieval remains canonical in
[`semantic-vector-retrieval.md`](../unsigned-personalization/semantic-vector-retrieval.md);
Kaggle execution is governed by
[`kaggle-static-site-builder.md`](../../operations/kaggle-static-site-builder.md).

## Product boundary

- Smart Update and Fly SQLite remain authoritative for event identity, title,
  dates, venue and lifecycle. Unusual scoring must not change canonical facts.
- Event/public eligibility is evaluated before unusualness. Service notices,
  work hours, closures, road works, cancelled/merged/private records, invalid or
  elapsed occurrences and other non-events cannot be published even if their
  text is close to a positive prototype.
- Unusualness is an offline, explainable discovery projection. It is not an
  admission, popularity, quality or personalization score.
- Ordinary page views read static manifests only. They never run BGE, an LLM or
  a provider request.
- One presentation concept may have multiple event/occurrence rows. The feed
  shows the concept once, then exposes its valid dates through the normal event
  occurrence contract.

## Fifteen-family taxonomy

The family is evidence for diversity and explanation, not a replacement for the
eligibility or score gates. Multi-family matches are allowed internally; the
manifest publishes one primary family deterministically.

| Family ID | Meaning / positive evidence |
|---|---|
| `open_dialogue` | Direct conversation or public dialogue with reduced stage/audience distance. |
| `participatory` | The visitor plays, votes, solves, performs or otherwise changes the experience. |
| `co_creation` | Participants make one shared work, sound, performance or outcome. |
| `behind_scenes` | Access to normally hidden production, museum, theatre or technical processes. |
| `restricted_access` | A real visit behind a permission/pass/controlled-access boundary. |
| `site_specific` | The event meaning depends on the exact castle, industrial, landscape or other site. |
| `after_hours` | A meaningful evening/night use of a place that is normally unavailable then. |
| `hybrid_format` | Several art or activity forms are deliberately composed into one experience. |
| `living_history` | Participatory reconstruction of historical life, craft or ritual. |
| `field_science` | Observation, sampling or investigation with a scientist outside a standard lecture. |
| `rare_practice` | A specific uncommon craft, instrument, method or curated route practised by visitors. |
| `gastro_experience` | A chef collaboration, guided production/tasting route or participatory culinary format. |
| `sensory_wellbeing` | A deliberately sensory embodied practice, not generic health/wellness advertising. |
| `community_exchange` | A structured swap, repair, volunteer or resource-sharing event. |
| `quirky_ritual` | A bounded playful ritual built around an unexpected everyday object or action. |

A classifier version is incomplete if its prototype bank omits any family. The
current checked-in classifier uses `5` as its versioned minimum diversity
threshold for the top 20. That number is an implementation/calibration
threshold, not an immutable product constant: a future calibrated classifier
may change it only together with its classifier hash and golden evaluation. A
build never pads an under-diverse result with ordinary events.

## Shared BGE: one document, one vector, several consumers

The static pipeline has one BGE encoding boundary:
`site/scripts/static_event_bge.py`.

1. `build_related_v1_document()` reuses the existing canonical `related_v1`
   document builder rather than copying its text policy.
2. `BAAI/bge-m3` is pinned to revision
   `5617a9f61b028005a4858fdac845db406aefb181` and emits normalized 1024-dimension
   dense vectors under `bge_m3_cpu_dense_fp32_l2_v1`.
3. `build_shared_bge_vector_artifact()` compares exact document hashes with the
   previous artifact, copies unchanged normalized rows and sends only changed
   events/prototypes through one model session. Removing one event drops only
   that row; changing one event encodes only that event. The same resulting
   vector is consumed by public static related retrieval, unusual scoring,
   family evidence and presentation concept clustering/support.
4. Consumers call `validate_shared_bge_vector_artifact()` and bind the model,
   revision, dimension, document version, normalization, prototype-bank hash,
   classifier hash and artifact hash. A mismatch means abstain/disable, never
   an implicit recompute in a second scorer.

Gemini `search_v3` may continue to serve authorized Search. Gemini
`related_v1` pgvector may remain only as an explicitly selected
rollback/comparison canary; it is not a concurrent production source for public
related results after the shared-BGE mode is enabled. Its 768-dimension vectors
must never be mixed with BGE vectors. Conversely, unusual/family/concept scoring
must not instantiate another encoder or issue a Gemini/Gemma call. For the R15
static BGE path the required result is exactly:

```text
provider_calls = 0
```

Any non-zero value, missing counter or evidence of a second encode boundary is a
release failure.

The decision cache deliberately does **not** key every event on the complete
artifact SHA: that SHA changes when any row or build receipt changes. Its stable
contract binds model/revision/dimension/document, prototype, classifier and
as-of date, then validates a per-event content/vector/eligibility/concept hash.
Thus an unchanged event retains its decision when an unrelated event changes.

## Scorer, evidence and activation gates

`site/scripts/unusual_event_semantics.py` loads the versioned prototype bank and
classifier, validates the shared vector artifact, applies eventness/public gates
and produces deterministic concept-level output. Prototype similarity proposes
evidence; embeddings do not decide the normative meaning of “unusual” on their
own. The calibrated head must preserve positive and negative evidence, score
margin, primary family, concept identity and a bounded reason code without
copying private source text into public HTML.

The frozen evaluation source is
[`tests/fixtures/unusual_events_golden_v1.json`](../../../tests/fixtures/unusual_events_golden_v1.json).
It was copied from the read-only
`artifacts/codex/unusual-events-20260727` snapshot and checked manual taxonomy.
It includes all 15 families, ordinary hard negatives, explicit non-events and
six repeated-series/concept groups. Historical positive rows test semantic
recall separately from as-of-date publication eligibility. Every row freezes
`eligible` and an editorial `frozen_tier` (`core_unusual`, `adjacent`,
`ordinary`, or `abstain`) so a real encoded canary measures identical-input tier
flips instead of silently leaving that metric undefined.

The hard gate is structured and fail-closed: active/canonical/public/searchable
status, non-silent/non-postponed/non-merged lifecycle, attendable event kind,
future end date and a minimum canonical semantic-text payload are required
before semantic publication. Keyword inference is not used to repair missing
meaning. Prototype evidence always contains the nearest positive,
hard-negative and neutral anchors so the published margins are auditable.

Activation is fail-closed unless the hash-bound evaluation proves all of:

- precision@20 of the **predicted unusual ranked feed** `>= 0.85`, or within
  `0.05` of the frozen `0.88` reference (ordinary controls are not inserted
  into the denominator as if they were predictions);
- hard-negative false-positive rate `<= 0.05`;
- confirmed-positive recall `>= 0.80`;
- no more than one row per concept in the top 20;
- identical-input flip rate `< 0.02` and exact deterministic repeat output;
- the classifier-version diversity threshold (currently five represented
  families);
- zero published ineligible/non-event rows;
- the shared-vector identity contract and `provider_calls=0`.

A report without real encoded BGE output is shadow evidence only. The checked
source fixture is not itself a successful canary.

## Manifest, cache and last-good contract

The builder writes `site/src/data/unusual-events.json` atomically. The public
manifest must bind its schema/policy/classifier/prototype/vector hashes and
snapshot/as-of identity, and expose a concept-ordered list with eligible event
occurrences. It must also state build mode, approval/gate status,
`provider_calls`, whether last-good was used, and the bounded reason for a
fail-closed/empty result. A generated timestamp or build ID is not semantic
identity and must not reroll ordering.

Persistent builder state is kept outside the checked-in Astro source:

- `/data/static_site_builder/static_event_bge_vectors.npz` plus
  `/data/static_site_builder/static_event_bge_vectors.receipt.json` for the
  hash-bound vector cache and receipt;
- `/data/static_site_builder/unusual_events_cache.json` for score/concept reuse;
- `/data/static_site_builder/unusual_events_last_good.json` for the most recent fully
  accepted manifest.

Names may be prefixed by the runner's configured cache directory, but their
roles remain separate. Writes use temporary files plus atomic replace. The Fly
runner persists downloaded cache/receipt files only after the exact Kaggle run
has a complete validated result; a partial, mismatched or failed remote run
cannot overwrite them.

`unusual_events_cache.json` uses the scorer-owned
`unusual-event-score-cache-v1` schema. In addition to per-event records it keeps
at most 512 recently observed concepts with their stable `concept_id`,
`first_published_at`, previous tier/content hash, representative event,
policy/model/prototype/classifier versions and notification eligibility.
Rebuilds preserve `first_published_at`; migration and first-baseline builds are
silent. Only a genuinely new approved `core_unusual` concept after an
established rollout baseline may receive `notify_eligible=true`.

Last-good is allowed only when its model/document/prototype/classifier and
policy contracts match, it is no more than seven days old, and every retained
item still has an active/future event whose freshly rebuilt canonical document
has the exact stored content hash. A fallback retains the original approved
quality metrics, sets `delivery_status=last_good_fallback`, disables all
notifications and replaces event snapshots only after those checks. Otherwise
the feature emits an explicitly disabled/empty unusual manifest. It must never
silently fall back to an unrelated snapshot or to the ordinary related list.
All unusual/vector hashes participate in the static input fingerprint without
feeding newly produced output back into an endless rebuild loop.

## Concept identity and the red dot

The navigation red dot means **a newly first-published, unseen unusual
concept**, not “the site rebuilt” and not “a score changed”. Concept identity is
derived deterministically in this order: curated concept, canonical root,
canonical series, mutually explicit `other_date_ids`/`occurrence_member_ids`/
`linked_event_ids`, conservative BGE presentation-only clustering, then a
stable deterministic presentation hash. The BGE cluster requires matching
venue/type, high title overlap and cosine `>=0.985`; it never writes to or merges
canonical `Event` rows and cannot create a date-family elsewhere.

The manifest carries stable concept identity, `first_published_at` and
`notify_eligible`. Browser state stores the bounded acknowledged concept set
under the versioned same-origin key `ke_unusual_seen_v1`. A concept is
acknowledged only after its card is materially viewed or the user activates the
explicit “mark shown as seen” control; merely rebuilding or opening an empty
feed is insufficient. Reordered rows, changed scores and extra dates in an
already seen concept do not recreate the dot. A storage read failure hides the
dot fail-closed; a failed acknowledgement write leaves it unread rather than
pretending persistence succeeded. Auth is not required and tokens/profile data
are not stored in the marker.

Migration/backfill builds set `notify_eligible=false` for every concept. A
first historical import therefore cannot flood users with “new” state. Only a
later accepted incremental manifest may introduce a notifying concept. Desktop,
mobile menu and footer/navigation consumers read the same state controller;
parallel red-dot implementations are forbidden.

## Static UI contract

- Canonical route: `/neobychnoe/`.
- Cards use the shared `EventCard` and normal event-detail/occurrence links;
  there is no unusual-only card or CTA fork.
- Empty/disabled manifest states are honest. They do not substitute popular or
  related events while retaining the “Необычное” heading.
- The route participates in sitemap/navigation only in the build modes approved
  by the release manifest. A secret candidate remains noindex and cannot cut
  over production root.
- The menu's `Подборки` submenu contains `Детям`, `Необычное`, `Бесплатно` and
  `Клубы`; `Бесплатно` also remains a top-level fast action. Canonical shell
  details live in [`mobile-shell.md`](../static-site-pages/mobile-shell.md).
- Ordering is core before adjacent, nearer 30-day events before later events,
  and then deterministic score/date/id. One concept is shown once; a first pass
  applies soft caps of 6 per family, 4 per venue and 8 per event type before a
  deterministic fill, with an absolute maximum of 30 cards.

## Rollout, Kaggle canary and rollback

Rollout is ordered and cannot be shortened by a green local fixture test:

1. **Offline shadow:** build vectors/scores and evaluation report; do not render
   a public route or dot.
2. **Migration/backfill:** seed cache/last-good with all
   `notify_eligible=false`; compare concept supply and duplicates.
3. **Kaggle CPU canary:** run the existing coalesced StaticSiteBuilder from an
   immutable SQLite snapshot. Evidence must include status-ledger heartbeat and
   terminal result, exact input dataset/snapshot identity, BGE/model/policy
   hashes, one shared encode pass, `provider_calls=0`, cache downloads,
   evaluation metrics, generated manifest, `check:preview` and the dedicated
   Playwright journey.
4. **Immutable noindex candidate:** check desktop/mobile feed, menu/red dot,
   dedup, empty/failure state and noindex containment with real generated data.
5. **Owner acceptance:** only then may the normal production release protocol
   consider root enablement. R15 does not authorize root cutover by itself.

As of 2026-07-27, steps 3–5 have no accepted evidence and remain **pending**.
Do not turn local mocks, an empty consultant output or the read-only data audit
into a canary claim.

Rollback first disables new notification eligibility, then restores the exact
compatible last-good manifest/cache receipt or disables the unusual route. It
never rolls back canonical events, borrows Gemini vectors, runs provider calls
on Fly/page view, or republishes a migration backfill as new. Production release
and rollback continue to follow the atomic/static builder protocol.

## Verification inventory

```bash
/home/dev/.codex/venvs/events-bot-new/bin/pytest -q \
  tests/test_unusual_events_golden_contract.py
node --test site/tests/unusual-events-source-contract.test.mjs
node --check site/tests/unusual-events.playwright.mjs

# On an immutable generated candidate with Playwright installed:
UNUSUAL_EVENTS_BASE_URL=https://… \
  node site/tests/unusual-events.playwright.mjs
```

The Playwright journey uses the shipped runtime at noindex lab fixtures under
`/lab/unusual-unread/<scenario>/` to cover all ten red-dot states: rollout
baseline, new core, adjacent, viewed, reload, same-series date, migration,
manifest failure, exact accessible label and an operable overflow-free mobile
drawer. These fixtures contain no provider code and are not release content.

The live Kaggle canary additionally follows the evidence list above and the
canonical [E2E scenario index](../../operations/e2e-scenarios.md).
