# Необычные события

> **Status, 2026-07-27:** exact code SHA
> `123bcee460112ee9fe0b0a0176f51a07c92eed6a` passed the full Kaggle
> production-candidate pipeline and was published only as an immutable
> noindex candidate. The public product matrix passed at desktop and mobile
> widths; production root was not changed and still requires explicit owner
> acceptance. The older `11d8c984` run remains superseded evidence.

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
   A classifier-only calibration is also cache-compatible: the dense vectors
   do not depend on the lightweight head. The rebuilt artifact reuses all rows,
   binds the **new** classifier SHA in fresh metadata and recomputes its artifact
   SHA. Runtime consumers still reject a vector receipt that was not rebound to
   their current classifier.
4. Consumers call `validate_shared_bge_vector_artifact()` and bind the model,
   revision, dimension, document version, normalization, prototype-bank hash,
   classifier hash and artifact hash. A mismatch means abstain/disable, never
   an implicit recompute in a second scorer.

The Kaggle CPU bootstrap pins `FlagEmbedding==1.4.0`, the
Transformers-5-compatible runtime already used by the repository's BGE
assessment pipeline. Its dependency probe upgrades an incompatible
preinstalled package before loading the frozen BGE-M3 revision.

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
coverage without becoming public candidates. Recall and false-positive rates
are measured on the frozen **publishable** editorial population; structurally
ineligible rows are forced to `abstain` and cannot dilute or improve those
metrics. Every row freezes `eligible` and an editorial `frozen_tier`
(`core_unusual`, `adjacent`, `ordinary`, or `abstain`) as review provenance.
The identical-rebuild flip metric compares two deterministic inference passes
over the same vectors; it does not compare a newly calibrated classifier to
the editorial tier label.

The hard gate is structured and fail-closed. The exporter emits the explicit
canonical eligibility record `canonical-event-semantic-v1`; the scorer requires
all of these fields before semantic publication:

- `record_kind=event` and `eventness_status=event`;
- `lifecycle_status=active` and `identity_status=canonical`;
- `merged_into_event_id` empty and `silent=false`;
- `is_public=true` and `is_searchable=true`;
- a future end date and minimum canonical semantic-text payload.

Missing columns, an untrusted semantic record, a service/work-hours record,
non-event, merge, silent/private/non-searchable state or elapsed occurrence
abstains. Keyword inference is not used to repair absent structured meaning.
Prototype evidence always contains the nearest positive, hard-negative and
neutral anchors so the published margins are auditable.

The scorer also measures distance to a deterministic **ordinary event corpus**
inside the same shared BGE space. The corpus contains at most 128 structured-
eligible rows provisionally classified ordinary, selected by base score and
event ID. For each candidate the feature is
`1 - max(cosine(candidate, ordinary_member))`; the current policy uses it in the
precision-first guardrail only: distance never positively boosts a candidate
into an unusual tier, while proximity can demote core/adjacent decisions that
are too close to ordinary events. This prevents ordinary-distance novelty from
overriding hard-negative evidence. No second encoding occurs. The manifest, evaluation and decision cache
bind both the ordinary-corpus policy SHA and a member/text/vector-bound corpus
SHA; an empty/missing corpus or mismatched receipt makes an otherwise eligible
candidate abstain. Its receipt must also report `provider_calls=0`.

Activation is fail-closed unless the hash-bound evaluation proves all of:

- precision@20 of the **predicted unusual ranked feed** `>= 0.85`, or within
  `0.05` of the frozen `0.88` reference (ordinary controls are not inserted
  into the denominator as if they were predictions);
- hard-negative false-positive rate `<= 0.05`;
- confirmed-positive recall `>= 0.80`;
- no more than one row per concept in the top 20; candidates are concept-
  deduplicated before precision/diversity are measured, matching the public
  presentation contract;
- identical-input flip rate `< 0.02` and exact deterministic repeat output;
- the classifier-version diversity threshold (currently five represented
  families);
- zero published ineligible/non-event rows;
- the shared-vector identity contract and `provider_calls=0`.

A report without real encoded BGE output is shadow evidence only. The checked
source fixture is not itself a successful canary.

The pre-hardening candidate head was calibrated against the pinned real BGE-M3
artifact `a4c72f80…` (332 event vectors, 55 prototypes) and the frozen fixture
`abacf30f…`. Rebinding those immutable vectors to the candidate head produces
precision `0.944444`, publishable confirmed-positive recall `0.833333`,
hard-negative FPR `0.05`, 12 top-feed families, zero published ineligible rows,
zero post-dedup duplicate concepts and deterministic flip rate `0.0`. These
measurements and the `11d8c984` canary are historical/superseded evidence: they
predate the ordinary-corpus and explicit-eligibility contracts above. The final
exact-SHA run must recreate every hash-bound metric; no number in this paragraph
approves the hardened candidate.

The hardened real-vector canary exposed one further boundary regression: an
adjacent probability threshold of `0.65` admitted two conventional-stage hard
negatives and produced FPR `0.10`. Recalibration on that exact pinned-BGE
artifact (`60d370bb…`) selected the smallest separating threshold, `0.725`.
The frozen result is precision `0.944444`, hard-negative FPR `0.05`, confirmed
positive recall `0.80`, 12 top-feed families and zero duplicate or ineligible
publications. This threshold change rebinds the classifier/artifact hashes and
still requires the final exact-SHA runtime gate; it is not an event-id or
keyword exception.

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
established rollout baseline may first receive `notify_eligible=true`. Once
that bit is established for a still-core concept, ordinary non-migration
rebuilds persist it in both manifest and concept cache. Browser-local seen
state suppresses the dot for that browser without mutating the manifest/cache
bit. A migration/backfill manifest always emits `notify_eligible=false`, but
does not erase an already established durable bit from concept state; the
following ordinary rebuild may restore it. Reordering, unrelated content
changes and generated timestamps never reset it.

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
first historical import therefore cannot flood users with “new” state and
cannot erase an existing concept's durable eligibility. Only a later accepted
incremental manifest may introduce a genuinely new notifying concept. Desktop,
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
  applies caps of 6 per family, 4 per venue and 8 per event type, with an
  absolute maximum of 30 cards. Deferred rows do not refill the feed by
  bypassing those caps; an honest underfilled feed is preferred.

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
   ordinary-corpus receipt, evaluation metrics and generated manifest. A
   production-candidate run first executes the legacy `build:preview` plus
   `check:preview` contract as an ephemeral non-published pre-gate, then the
   production-root and secret-candidate builds/checks and their browser-release
   gates.
4. **Immutable noindex candidate:** check desktop/mobile feed, menu/red dot,
   dedup, empty/failure state and noindex containment with real generated data.
5. **Owner acceptance:** only then may the normal production release protocol
   consider root enablement. R15 does not authorize root cutover by itself.

The `11d8c984` canary exercised real pinned BGE, produced zero-provider semantic
artifacts and useful cache/hash evidence. It is **superseded**, not absent and
not release acceptance: it predates the final contracts above, its dedicated
candidate Playwright was coupled to lab routes omitted from the package, and its
upload/public acceptance did not complete.

The final exact run is
`static-site-builder:r15-bge-final5-20260727T221000Z`, build
`production-r15-bge-final5-20260727t221000z`, from snapshot `prod-20260727`
(`043b9f845292f0b974864d232b9e309c616bfee035e11b35a40bd2b41cd68664`).
It exported 326 current events and produced 1,439 pages / 1,697 files. The
legacy preview pre-gate reported `status=ok`, `archived=false`,
`published=false`; root-form and secret-candidate build/check/browser gates
passed, with only the explicitly optional related-freshness check degraded.

The hash-bound unusual result is `approved`, `provider_calls=0`,
`cache_state=hit_reused`, with 30 published concepts. Observed editorial
precision@20 is `1.0`, hard-negative FPR `0.0`, confirmed-positive recall
`0.8`, duplicate concepts `0`, identical-rebuild flip rate `0.0`, family
diversity `12`, and ineligible publications `0`. The public immutable candidate
is:

<https://kenigevents.ru/_review/pp1wRctXBd6boYU1EcnBrod3z8MmKpD7SGEufK1t-xw/>

All 36 route/viewport checks returned `200`, the candidate-wide noindex policy,
no horizontal overflow, no broken images and no page errors. Product-mode
Playwright passed and the stripped lab canary returned `404`. This completes
steps 3–4 only. Step 5 remains an explicit owner decision; none of this evidence
authorizes production-root promotion.

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

# Product acceptance against the immutable package; it never opens lab routes.
UNUSUAL_EVENTS_PLAYWRIGHT_MODE=product \
UNUSUAL_EVENTS_BASE_URL=https://… \
  node site/tests/unusual-events.playwright.mjs

# Separate local noindex red-dot matrix; never use the candidate as this base.
UNUSUAL_EVENTS_PLAYWRIGHT_MODE=lab \
UNUSUAL_EVENTS_LAB_BASE_URL=http://127.0.0.1:4321 \
  node site/tests/unusual-events.playwright.mjs
```

Product mode checks only packaged product routes and is the candidate release
journey. The immutable candidate must not ship the noindex lab matrix;
`/lab/unusual-unread/new-core/` is expected to return `404` there. Lab mode uses
an independently served local Astro tree to cover all ten red-dot states:
rollout baseline, new core, adjacent, viewed, reload, same-series date,
migration, manifest failure, exact accessible label and an operable
overflow-free mobile drawer. `all` mode is developer convenience only and still
requires both independent base URLs.

The live Kaggle canary additionally follows the evidence list above and the
canonical [E2E scenario index](../../operations/e2e-scenarios.md).
