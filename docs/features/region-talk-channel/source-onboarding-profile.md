# Source profile and one-paragraph onboarding

Status: **bounded MVP-2 implementation is live in the local publication finalizer**. Evidence/profile/paragraph rows are durable in YDB and a supported paragraph is included in the operator-chat candidate message. Profile/about/pinned-post acquisition and archive-wide dual-vector recall remain follow-up work; the current implementation intentionally fails closed when the already collected public evidence is insufficient.

## Implemented bounded path (2026-07-13)

The first production-safe slice is implemented in `scripts/region_talk_publication_finalizer.py` and `scripts/region_talk_goal_notify.py`:

1. When a post reaches the finalizer, it consolidates the canonical source row, the verified external-blogger registry fields and up to eight distinct authored excerpts already present in candidate memory.
2. The compact evidence pack is stored as `source_onboarding_evidence_item` with stable `source_profile_id`, evidence ids, URLs/dates and `evidence_fingerprint`.
3. A Gemini profile request is made only for a new/changed evidence fingerprint. Its atomic claims and angles are rejected unless all referenced evidence ids exist. The resulting `source_onboarding_profile_item` is reusable across candidates.
4. A second candidate-specific request writes a 300–600 character paragraph. Deterministic validation requires valid claim/evidence references and the length contract; otherwise status is `needs_review` and no paragraph is shown.
5. `publication_candidate_item` retains the profile/writer fingerprints, references, entity type and paragraph. The notifier adds `О блогере: …` only for `source_onboarding_status=ready`.
6. Profile and writer calls share the same durable daily Region Talk budget and Supabase provider limiter as the final verifier. A new source/candidate therefore uses at most two extra point requests; an unchanged source profile needs only the writer request.

Current limitation: this bounded path does not issue additional Telegram profile/full-channel requests and does not crawl an archive solely for biography. It uses evidence already collected by the product pipeline, avoiding new Telegram pressure and unsupported claims. The broader recall design below remains the target for evidence-poor sources.

## Product result

For every author/channel whose post reaches manual publication review, Region Talk should prepare a short onboarding paragraph that helps the reader understand **who is speaking and from what perspective** before following the original post.

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
