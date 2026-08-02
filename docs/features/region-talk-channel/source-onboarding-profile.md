# Source profile and one-paragraph onboarding

Status: **bounded source-profile recovery is implemented in code; production
import/capture/backfill evidence is tracked separately during release**. Social
profiles now require a dedicated description/pinned/30–80-post capture, while
publisher and journal profiles come from guarded official-evidence sidecars.
Missing or conflicting evidence fails closed before the public-copy Writer.

## Implemented bounded path (updated 2026-08-02)

The production-safe bounded path is implemented across CandidateReport,
`scripts/region_talk_publication_finalizer.py`, the dedicated publisher-profile
import/review commands and Writer vNext:

1. CandidateReport reuses its existing role-scoped Telegram/VK reader. For an
   explicitly requested social source it reads public description, pinned
   evidence and 30–80 recent rows (50 by default), without acknowledgement,
   reaction or media download.
   Recovery operators may set `REGION_TALK_SOURCE_PROFILE_CAPTURE_ONLY=1` for
   this bounded pass. After the online capture writes it emits a terminal
   receipt and skips candidate scoring, embeddings, image queues and every
   publication path; the normal scheduled report keeps the flag disabled.
2. Deterministic safety preprocessing classifies
   `authored|repost|service|ad_like`, keeps at least 20 authored posts, selects
   8–16 diverse recent excerpts and stores a stable
   `source_profile_capture_item`. Reposts, service rows and ads never become
   authored profile evidence.
3. The finalizer accepts only a complete `ready` capture. It persists the
   compact evidence pack as `source_onboarding_evidence_item` and makes one
   profile LLM request only when the capture/evidence fingerprint changes.
   An unchanged reusable or non-ready profile attempt spends zero profile
   calls; a failed attempt is retried only after evidence/prompt fingerprint
   change, not because a new daily budget ID exists.
4. A missing/stale social profile is projected back to the source queue as
   `source_profile_capture_requested=true` and `needs_source_profile=true`;
   this does not change the accepted candidate verdict or grant publication.
   Explicit capture requests own the bounded source-history slots ahead of
   ordinary discovery/rescan rows, including when the source itself was scanned
   recently; otherwise a small `REGION_TALK_MAX_SOURCES` run could repeatedly
   miss the requested source before reaching the capture stage. The durable
   request booleans are authoritative; a leftover `priority_lane` label after
   request completion cannot reopen or reprioritize capture by itself.
   Acquisition and editorial readiness are separate after the bounded read:
   once a current capture has been processed, the finalizer clears
   `source_profile_capture_requested` even when the evidence-backed profile
   remains fail-closed `needs_review`. In that state `needs_source_profile`
   stays true and the source is routed to profile review; the unchanged archive
   is not fetched again merely because the public-copy Writer is still blocked.
5. Editorial/academic sources use separately imported
   `publisher_profile_item` rows. A reusable publisher profile must be
   `ready`, external, public-copy-eligible and contain grounded
   `outlet_identity`, `intended_audience` and `distinctive_value` dimensions.
6. `scripts/region_talk_publisher_profile_import.py` validates exact sidecar
   bytes and schema, strongly rereads all affected keys and atomically writes
   profile/correction/batch/receipt rows. The normal external intake importer
   can only enrich the same publisher identity monotonically.
7. Candidate corrections remain fail closed until
   `scripts/region_talk_publisher_profile_correction_review.py` verifies the
   exact correction, identity and intake snapshot in one serializable
   transaction. That command writes the correction verdict and immutable audit
   only; it never mutates a candidate or grants publication permission.
8. Writer vNext runs only after reusable profile readiness. Paragraph one is
   exactly a grounded current-content hook followed by a compact grounded
   source sentence; paragraph two contains one or two material details. The
   rendered body is revalidated before the deterministic source-aware CTA and
   the separated linked channel footer are appended.
9. Profile and onboarding-writer call caps are accounted separately while all
   provider calls still share the durable Region Talk budget and atomic Google
   limiter. Missing profile budget yields `needs_source_profile`, never generic
   filler copy.

Current limitation: this bounded recovery intentionally does not build a
vector database over each source archive. The longer archive-wide dual-vector
recall design below remains a later product option; the implemented MVP uses a
bounded normalized capture and a single LLM synthesis over selected evidence.

Operational defaults live in `.env.example`:

- `REGION_TALK_SOURCE_PROFILE_CAPTURE_ENABLED=1`;
- `REGION_TALK_SOURCE_PROFILE_SCAN_POSTS=50` (code clamps to 30–80);
- `REGION_TALK_SOURCE_PROFILE_MIN_AUTHORED_POSTS=20`;
- `REGION_TALK_SOURCE_PROFILE_CAPTURE_MAX_SOURCES_PER_RUN=2`;
- `REGION_TALK_SOURCE_PROFILE_MAX_LLM=10`;
- `REGION_TALK_SOURCE_ONBOARDING_WRITER_MAX_LLM=10`.

The last two are stage caps, not new provider quotas. All physical requests
still reserve through the shared atomic limiter and the durable global Region
Talk budget.

## Product result

For every author/channel whose post reaches manual publication review, Region Talk prepares a short onboarding paragraph that helps the reader understand **who is speaking and from what perspective** before following the original post.

For an external editorial/academic publication, the equivalent result is a **publisher reader brief**. It answers: what kind of outlet this is, whom it serves, and what distinguishes its editorial approach. The current article is then summarized separately. An outlet name plus a generic adjective such as “professional” is insufficient.

The paragraph should answer no more than four questions:

1. Who is this: a person, collective author channel, thematic community or media brand?
2. What is the verified relationship to Kaliningrad Oblast or the viewpoint from which the region is described?
3. What does the author/channel mainly do or publish about now?
4. What is the **most relevant and distinctive angle for this concrete post**?

Target length is one paragraph, normally **300–600 characters**. The purpose is orientation and interest, not a full biography or an advertising superlative.

Example shape:

```text
[Name] is [verified self-identification or role], [verified regional/travel context].
The author works on [current activity] and usually writes about [editorial focus].
This post shows Kaliningrad Oblast from the perspective of [supported viewpoint].
```

Foreign origin, specialization or travel across Russia/the world are only illustrative angle candidates, not a fixed taxonomy and not mandatory profile fields. The useful angle may instead emerge from the archive/post combination and may change from one selected post to another.

The wording must preserve epistemic status:

- `Автор называет своей задачей…` only when the author said this explicitly;
- `По содержанию канала основной фокус…` for an editorial inference from authored posts;
- `Автор родом из…`, `иностранный блогер`, `объехал всю Россию/мир` only with direct public evidence;
- use `связан с регионом` or `пишет с позиции…` when birthplace/residence/travel breadth is not established.

## Dynamic emphasis, not a fixed biography template

The paragraph should contain at most one primary angle and, when it materially improves comprehension, one secondary angle. The LLM chooses them from the retrieved evidence and current post context; deterministic code must not force every source through a fixed list.

The following is a **recall palette**, not an output checklist:

- public identity: individual author, collective, project, expert practice or editorial brand;
- professional/expert lens: architecture, history, nature, food, photography, transport, urbanism, culture or another evidenced field;
- lived or geographic perspective: local, newcomer, returning visitor, first-time visitor, cross-regional or international comparison;
- breadth/depth of experience: long-term project, repeated expeditions, many regions/countries, narrow deep specialization;
- distinctive method or format: field notes, archival research, interviews, photo essay, maps/routes, experiments, reviews, diary, explanatory analysis;
- travel mode and constraints relevant to the post: on foot, by bicycle/car/train/boat, solo, with children, budget, accessibility, off-season — only when explicit in the evidence/context;
- notable public work: a named project, book, expedition, profession, award or community initiative when it explains the viewpoint rather than merely adding status;
- declared mission/values, only when explicitly stated;
- observed editorial signature: recurring comparisons, attention to overlooked details, practical route testing, historical context, visual storytelling or another repeated pattern;
- situational relation to the selected post: first impression, return after several years, contrast with another place, changed opinion, unusual access, seasonal observation or a post that differs meaningfully from the channel's normal subject.

This palette is deliberately open. The LLM may propose a new `freeform_angle` when it is more informative than the known families. A new angle is acceptable when it is concise, safe, relevant to the selected post and grounded in retrieved evidence. It does not need to be something the author explicitly declared if it is clearly worded as an editorial observation rather than a biographical fact.

Examples of the distinction:

- hard claim: `Автор работает архитектором` — requires explicit evidence;
- archive-supported observation: `В канале он обычно рассматривает города через архитектуру и детали среды` — may be an LLM synthesis over several authored posts;
- post-situational angle: `В этот раз он сравнивает побережье с маршрутами, которые ранее проходил на Севере` — must be supported by the selected post plus retrieved comparison evidence;
- unsupported embellishment: `Опытный путешественник мирового уровня` — reject unless every meaningful component is supported and the evaluative wording is warranted.

Follower counts, awards, nationality, biography or broad travel claims must not be selected merely because they are available. Prefer the angle that best explains **why this person's/channel's view of this specific Kaliningrad post is interesting**.

## Identity boundary

The system must first decide what entity it is describing:

- `person` — individual author;
- `collective` — several named or unnamed authors;
- `thematic_channel` — channel/community whose topic is clearer than its author identity;
- `media_brand` — editorial organization rather than a personal blog;
- `unknown` — identity is too ambiguous for a biographical paragraph.

Do not turn a collective `мы` into one person's biography. If identity is unclear, generate a channel-focused paragraph from verified channel metadata and editorial focus, or return `insufficient_evidence`.

Cross-platform accounts may share one logical profile only after identity normalization and review. Similar names, avatars or topics are not enough by themselves.

## Evidence sources and precedence

Use public data only:

1. channel/account description and public profile metadata;
2. pinned introduction/about posts;
3. authored posts, with special recall for self-description and retrospective posts;
4. explicit public interviews/about pages when provenance is retained;
5. reposts, quotations and third-party mentions only as weaker leads, never as first-person proof.

External publishers use a narrower evidence path: About/editorial-policy or
navigation evidence captured by the bounded external-research contract,
copy-supported `source_overview`, and the publisher scope attestation. Article
facts may illustrate the current material; they cannot establish a reusable
audience or editorial identity by themselves.

Evidence precedence for consolidation:

```text
direct authored statement > supported editorial inference
authored post/profile > repost/quotation/third-party mention
current dated evidence > stale evidence for current fields
several independent supporting items > one weak mention
```

Every factual phrase in the paragraph must resolve internally to:

```text
onboarding phrase
  -> normalized claim
  -> exact evidence excerpt
  -> source post/profile URL
  -> publication date or observation timestamp
```

An LLM confidence value is only a diagnostic signal; it is not a substitute for provenance, exact evidence and temporal consistency.

## LLM-first processing pipeline

Embeddings inspect the mass archive and retrieve potentially relevant text; they do not decide who the author is or what the final angle should be. The required pipeline is:

```text
public profile + pinned/about posts + authored archive
  -> incremental embeddings + sparse metadata/marker index
  -> broad and post-conditioned vector recall
  -> deduplicated compact evidence pack
  -> one batched LLM profile/angle synthesis request
  -> temporal/evidence-aware claim consolidation
  -> reusable source profile + candidate angles
  -> one final candidate-specific writer/verifier request when needed
  -> human evidence review before first publication
```

### Recall

The archive is embedded/indexed once and updated incrementally. Do not send every post, every field or every retrieval hit to an LLM.

Vector recall should combine:

- a broad reusable profile query bank;
- open-aspect discovery from embedding clusters/recurring authored topics;
- a post-conditioned query derived from the concrete candidate post so situational evidence is not lost;
- lexical/sparse markers for self-description, dates, projects and negation;
- diversity/coverage selection so the evidence pack is not ten near-duplicate snippets from one theme.

The reusable query bank provides recall anchors, not a closed output schema. It should cover at least:

- self-identification and current occupation/activity;
- origin, residence and relationship to Kaliningrad Oblast;
- channel subject and current editorial focus;
- travel breadth (`region`, `Russia`, `international/world`) without inferring it from one trip;
- explicit mission or stated purpose;
- introduction/history markers such as `кто я`, `расскажу о себе`, `новым подписчикам`, `моя история`, `как всё начиналось`, `итоги года`.

It should also retrieve evidence about recurring methods/formats, named projects, comparative geography, repeated visits, audience/use context and unusual constraints. The current candidate post itself acts as an additional semantic query, allowing the final LLM to notice a situational angle that was not anticipated by the bank.

Follow the Region Talk dual-model recall contract (`multilingual-e5-base` + `bge-m3`) and add lexical/sparse recall for self-description markers. Preserve per-model and fused evidence. A cross-encoder reranker may be added after a repository-specific evaluation; do not pick it from public benchmarks alone.

Start with the latest six months for current activity and focus. For missing slow-changing facts, expand adaptively to 12 months, then 24 months or the available archive. Stop when deeper scanning no longer adds supported evidence or candidate angles. Never let an old occupation silently override a newer current statement.

### LLM call budget

Routine target is **one or a small number of point requests after retrieval**, never LLM-over-the-corpus:

1. **Profile synthesis call** — once per new/changed source-profile fingerprint. It receives one compact, deduplicated evidence pack and returns atomic claims, open candidate angles, conflicts and missing information in one JSON response. It is reused across many posts.
2. **Candidate writer/verifier call** — only for a favorite/queued post. It receives the reusable profile, current post, post-conditioned evidence and chooses the primary/optional secondary angle while drafting/verifying the paragraph and platform text.
3. An extra conflict-resolution call is exceptional, budgeted and must end in `needs_review` if it cannot resolve the evidence; it is not part of normal bulk processing.

Default implementation target: no more than two successful LLM calls for a new `source profile + selected candidate` combination, and normally one call for later candidates reusing an unchanged profile. Provider retries do not change the semantic call contract and must remain capped/audited.

### Atomic claim extraction

The first LLM request must process the compact evidence pack in one batch and return atomic statements plus open angle candidates rather than a prose biography. Minimum claim contract:

```json
{
  "claim_id": "stable-id",
  "source_id": "source-id",
  "field": "current_activity|origin|residence|regional_connection|travel_scope|self_identification|explicit_mission|editorial_focus",
  "value": "travel blogger",
  "normalized_value": "travel_blog",
  "subject_matches_profile": true,
  "explicitness": "explicit|editorial_inference",
  "temporality": "current|past|planned|timeless|unknown",
  "negated": false,
  "evidence_source_type": "profile|authored_post|pinned_post|interview|repost|quotation|third_party",
  "evidence_excerpt": "exact short excerpt",
  "evidence_url": "https://...",
  "evidence_published_at": "2026-03-12T00:00:00Z",
  "extractor_model": "...",
  "extractor_policy_version": "..."
}
```

Minimum dynamic-angle contract in the same response:

```json
{
  "angle_id": "stable-id",
  "label": "сравнивает города через архитектуру и детали среды",
  "angle_family": "expert_lens|geographic_perspective|experience_depth|format_method|project|editorial_signature|post_situation|freeform",
  "basis": "explicit_fact|archive_pattern|current_post|cross_evidence_synthesis",
  "scope": "reusable_profile|candidate_specific",
  "supporting_claim_ids": ["claim-id"],
  "supporting_evidence_refs": ["post-id#excerpt-or-profile-ref"],
  "relevance_to_candidate": 0.0,
  "distinctiveness": 0.0,
  "freshness": 0.0,
  "reader_value": 0.0,
  "risk_flags": []
}
```

These numeric values are ranking/debug signals, not probabilities. `angle_family` is a coarse analytics label; `label` remains open-ended.

The extractor must not:

- infer a profession, birthplace, nationality or travel breadth only from post topics;
- treat a mentioned location as birthplace/residence;
- attribute a quotation, repost, fictional first-person narrative or advertisement to the author;
- turn past work into current work;
- ignore negation (`я не журналист`);
- fill missing fields by guessing.

### Consolidation and generation

Consolidation must preserve timelines and conflicts. Deterministic code may normalize, group, deduplicate and apply narrow provenance/recency gates, but semantic resolution and paragraph writing remain LLM-first. Conflicting high-quality claims produce `needs_review`, not an arbitrary winner.

The structured profile should keep at least:

- `entity_type` and normalized identity;
- verified `self_identification`, `current_activity`, `regional_connection`, `travel_scope`;
- `main_topics` / `editorial_focus`, explicitly marked as an editorial inference when applicable;
- `explicit_mission`, only when directly stated;
- open `candidate_angles` with evidence and scope, not only predefined facets;
- `conflicts`, `missing_fields`, claim ids and profile version;
- `onboarding_paragraph_draft`, `onboarding_status`, reviewer decision and timestamps.

The final writer/verifier selects angles using evidence strength, relevance to the current post, distinctiveness, freshness, reader value, safety and recent-channel diversity. It receives only approved factual claims plus evidence-backed editorial/situational observations. Hard factual fragments must map to approved claim ids; archive-pattern or candidate-specific observations must map to evidence refs and retain their scope. If there is not enough evidence, it must return `insufficient_evidence` or a narrower channel-focused paragraph rather than embellish.

An `archive_pattern` or `post_situation` angle does not have to be promoted into a timeless biographical claim. Store and phrase it at the correct scope.

## Review and publication gates

Before the first public use of a profile, the reviewer must be able to inspect `claim -> evidence excerpt -> URL -> date`. Re-review is required when:

- a current occupation, residence, channel ownership or editorial focus changes;
- evidence conflicts;
- the paragraph is older than the configured profile refresh window;
- the source changes from personal to collective/brand positioning;
- the generated paragraph contains a factual phrase with no approved claim id.

Publication must fail closed when the profile is missing or stale: the post may still use neutral source attribution (`Автор канала …`) after manual approval, but must not invent biographical context. A profile is reusable across posts from the same logical source and should not be regenerated for every candidate unless the underlying claims changed.

## Privacy and safety

- Use only intentionally public information relevant to understanding the author's public work.
- Do not publish precise addresses, family details or other unnecessary personal data.
- Do not infer political, religious, medical, ethnic, sexual or other sensitive traits from indirect signals.
- Do not equate language, location or audience with nationality.
- Keep evidence URLs/excerpts for internal review; public copy should remain concise and non-invasive.

## Report and quality acceptance

The candidate workbook should expose a source-profile review surface with the paragraph, profile status, entity type, selected and alternative angles, angle basis/scope, key claims, conflicts, missing fields and clickable evidence links. Suggested quality metrics:

- `Evidence Recall@K` for profile fields;
- `Claim Precision`;
- `Claim Evidence Support Rate`;
- `Temporal Accuracy`;
- `Unsupported Claim Rate` (target: zero in approved public copy);
- `Human Editor Acceptance Rate`.
- `Angle Relevance/Distinctiveness` from editor ratings;
- `Profile Reuse Rate` and LLM calls per selected candidate.

The evaluation set must include traps: reposted first-person text, quotations, sarcasm, negation, past occupation, multiple moves, collective channels, fictional first-person stories, advertisements and unsupported claims of Russia/world travel.

It must also test that different posts from the same source may correctly receive different situational angles, while the reusable factual profile remains stable.

## Research rationale

- [OpenAsp](https://aclanthology.org/2023.emnlp-main.121/) shows why a small predefined aspect list is insufficient for realistic multi-document targeted summaries; Region Talk therefore keeps open `freeform_angle` discovery.
- [MODABS](https://aclanthology.org/2024.findings-acl.165/) explicitly treats the number/content of aspects as input-dependent rather than fixed; Region Talk likewise selects at most the useful angles for the concrete source/post pair.
- [Coarse-to-Fine Query-Focused Multi-Document Summarization](https://aclanthology.org/2020.emnlp-main.296/) supports the retrieval/evidence-estimation/final-summary separation used here.
- [Do Multi-Document Summarization Models Synthesize?](https://aclanthology.org/2024.tacl-1.58/) reports imperfect synthesis and supports diverse candidates plus selection/abstention rather than trusting one unconstrained paragraph.
- [Google ProfilePage guidance](https://developers.google.com/search/docs/appearance/structured-data/profile-page) distinguishes a person from an organization and ties a profile to its authored activity; Region Talk keeps the same identity boundary but uses a richer editorial evidence model.
- [Schema.org Person](https://schema.org/Person) offers broad properties such as occupation, affiliation, skills/knowledge and works. They inform recall coverage only; they are not a mandatory public paragraph template.

## Delivery placement

- **MVP-1 report:** schema/report columns are reserved; no requirement to generate onboarding for every seed.
- **MVP-2 manual approval (bounded slice implemented):** claim/profile evidence, reusable profile, candidate-specific draft and operator-chat rendering are active for final candidates. Evidence-poor rows fail closed to `needs_review`.
- **Before MVP-3 live Telegram publishing:** require an approved or explicitly waived neutral source profile for every publication.
- **Before autonomous publishing:** validate refresh/staleness rules and meet the unsupported-claim gate on a manually labelled evaluation set.
