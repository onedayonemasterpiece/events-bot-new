# Внешние издания и научные публикации

Status: **contract + importer + guarded CandidateReport consumer + diversified queue implemented**. Only intake rows explicitly marked `ready_for_region_talk_scoring` can enter the existing E5/BGE/image/final-verifier funnel; manual-review and blocked research rows remain outside it. This is an extension of **Region Talk**, not a separate channel or crawler.

## Product decision

Region Talk covers two origins under one editorial promise — «как о Калининградской области говорят за пределами региона»:

1. `external_social` — posts by nonregional Telegram/VK authors;
2. `editorial_publication` / `academic_publication` — substantive work in a nonregional publication, journal, professional platform, or cultural outlet.

The outlet must remain external, supraregional, federal, or international. A local Kaliningrad outlet/university does not become external merely because an author is from elsewhere. Author affiliation, outlet scope, research object, and publication language are stored separately.

This lane is for analysis, criticism, research, explanatory features, interviews, essays, reviews, and substantive project descriptions. It is not a back door for current news, politics, military/security material, incidents, press releases, event cards, tours, native advertising, SEO pages, or incidental mentions.

The attached 2025–2026 research is treated as a **map of discovery contours**, not an allowlist and not exhaustive frequency data. Military/security examples remain research evidence only and are a hard product exclusion.

## Lightweight architecture

```text
one broad external web-research request
  → strict region_talk_external_research.v1 JSON
  → local validation + DOI/canonical-URL dedupe
  → YDB external_publication_intake_item + external_publication_source_item
  → same Region Talk E5 + BGE-M3 text evaluation
  → same RegionTalkImageDiagnostic for direct article images
  → editorial/academic final verifier
  → links + visible scores in operator chat
  → operator decision
  → on-demand MMR queue with semantic anti-adjacency
```

No domain crawler, RSS farm, permanent list of publications, or automatic public publishing is added. The external model searches the web; Region Talk owns validation, state, evaluation, review, and ordering.

### Trust boundary

The external model returns **research evidence**, not a publication verdict. Its maximum `downstream_readiness` is `candidate_report`. Import never writes `publication_candidate_item`, never marks `llm_confirmed`, and never publishes.

Keep three independent fields:

- `research_match` — useful lead found by the broad research task;
- `product_policy_match` — compatible with current Region Talk policy;
- `downstream_readiness` — `candidate_report | manual_review_required | blocked`.

Thus an English paper may be retained as research but fail the current Russian-language product policy; an otherwise rigorous military paper is hard-blocked; an abstract-only or undated page stays manual review.

## Selection rubric

Every candidate is scored 0–4, with evidence, on source authority for this concrete material, evidence depth, editorial independence, originality, Kaliningrad centrality, broad public interest, and accessibility to a general reader.

Scientific rigor and public interest are separate. A technically sound paper is not automatically a channel candidate. It needs a comprehensible insight: what was studied, what was found within the stated limits, and why a resident or curious reader might care. Preserve methods/sample limits, uncertainty, corrections, retractions, funding/conflict disclosures, and association-versus-causation boundaries.

Region Talk additionally prioritizes materials that form an **evidence-based positive image of the region**: discovery, culture, heritage, nature, science, architecture, creative work, civic initiative, distinctive places, and achievements. This is not a demand for praise or PR. Neutral and problem-focused work may remain eligible when it is respectful, balanced, explanatory, and has a constructive reader value. A dominantly hostile, stigmatizing, contemptuous, sensational, or catastrophizing portrayal is not sought and is blocked as `sharp_negative_region_image`; uncertainty about the dominant effect requires manual review rather than optimistic guessing.

### Text-only and rights policy

- Default article-image policy is `link_only` + `score_only_no_reuse`.
- RegionTalkImageDiagnostic may evaluate a direct hero/OG image for quality, but evaluation permission is not reuse permission.
- `reuse_verified` is allowed only for the exact asset with verifiable licence/permission.
- A paper/PDF/paywalled page without a suitable direct image remains in Candidate Report/manual review; it does not receive a fake image score.
- A future deterministic link-card renderer may create owned visuals, but that is a separate decision.

## Machine contract and importer

Canonical JSON Schema: [`external-publication-research.schema.json`](external-publication-research.schema.json), version `region_talk_external_research.v1`. Importer version: `region_talk_external_publication_import.v1`.

Dry-run validation:

```bash
python3 scripts/region_talk_external_publication_import.py research-result.json
```

Explicit YDB staging write:

```bash
python3 scripts/region_talk_external_publication_import.py research-result.json --execute
```

The importer:

- accepts JSON only;
- validates the complete Draft 2020-12 JSON Schema with format checking; candidate-local schema failures enter the row error ledger, while invalid run/coverage metadata aborts the batch;
- strips URL fragments and tracking parameters;
- rejects non-HTTP schemes, private/local/reserved hosts, and non-web ports;
- prefers normalized DOI identity, then canonical URL;
- upserts stable IDs, so replay is idempotent;
- reports invalid rows independently instead of aborting the valid batch;
- requires every non-empty editorial-copy surface to have evidence-backed `copy_support`, and every referenced evidence ID to resolve;
- forbids a clean candidate with regional/unknown source scope, out-of-window or snippet-only date, news/sales classification, hard exclusion, language mismatch, unverified product-policy match, non-full-text access, or unverified source externality;
- requires a clean candidate to have `strong|credible` quality tier and at least `2/4` for regional centrality, broad public interest, and accessibility; scholarly rows additionally require verified peer review with no correction/retraction flag;
- validates direct candidate media URLs as public HTTP(S) URLs before any future image handoff;
- never fetches pages and therefore is not an SSRF-capable crawler;
- writes `external_publication_intake_item`, a compact publisher attestation as `external_publication_source_item`, row errors as `external_publication_import_error_item`, and `external_publication_import_batch`;
- does not itself write `candidate_memory_item`, `image_queue_item`, or `publication_candidate_item`: only CandidateReport may promote a strictly ready row through the normal gates.

### CandidateReport handoff

`RegionTalkCandidateReport` reads `external_publication_intake_item` beside the
existing row-level Region Talk state. Its adapter is fail-closed and admits a
row only when all of these remain true at read time:

- `decision.import_status=ready_for_region_talk_scoring`;
- `decision.downstream_readiness=candidate_report`;
- `product_policy_match=true` and there is no hard exclusion;
- no later operator override blocks the row.

The adapter creates a `platform=web` scoring projection, never a synthetic
Telegram/VK post. It preserves `content_origin_type`, canonical publisher key,
research quality, source overview, diversity topics and rights fields. A direct
article image may be sent to ImageDiagnostic only as
`score_only_no_reuse`; an image score is not permission to republish it.

The E5 and BGE-M3 semantic bank contains separate
`ko_editorial_publication` and `ko_academic_publication` positive classes. The
final verifier uses the origin-aware `region_talk_final_verifier_v6` contract:
editorial/academic work needs attributed analysis/research, an evidence or
expert basis, a concrete public-interest insight and a memorable useful detail;
it does not need a first-person trip. Sharp negative regional framing remains a
hard rejection. The social lane keeps its former firsthand/emotion rules.

For a bounded canary that does not touch Telegram/VK acquisition, launch
CandidateReport with `REGION_TALK_EXTERNAL_PUBLICATIONS_ONLY=1`. This changes
acquisition scope only; YDB, dual-vector, image, final-verifier and operator
gates remain the same.

## Ready-to-run external prompt

Copy the prompt below into a web-capable research model. Replace only the values in `RUN INPUTS`. Attach the JSON Schema file if the model supports attachments/schema-constrained output.

```text
ROLE
You are a cautious web researcher and editorial verifier for the existing Region Talk / «О Калининграде говорят» project. Find substantive publications about Kaliningrad Oblast made by nonregional publications, journals, professional platforms, or cultural outlets. Do not build a source allowlist and do not optimize for raw result count. Optimize for verified quality and diversity of source/content types.

RUN INPUTS
- request_id: {{REQUEST_ID}}
- as_of_date: {{YYYY-MM-DD}}
- window_start: {{YYYY-MM-DD}}
- window_end: {{YYYY-MM-DD}}
- target_region: Калининградская область, Россия
- output_language: ru
- research_languages: [ru, en]
- product_language_policy: ru_or_mostly_ru
- maximum_candidates: {{MAX_CANDIDATES}}
- maximum_candidates_per_contour: {{MAX_PER_CONTOUR}}
- already_seen_canonical_urls_or_dois: {{SEEN_LIST_OR_EMPTY_ARRAY}}
- blocked_domains: {{BLOCKED_DOMAINS_OR_EMPTY_ARRAY}}

OUTPUT CONTRACT
Create a UTF-8 file named region-talk-external-research-result-{{REQUEST_ID}}.json and return it as a downloadable attachment. When file attachment is supported, do not paste the large JSON payload into chat. The file must contain one JSON object and nothing else: no Markdown, commentary, citations outside JSON, or code fence. It must conform exactly to JSON Schema region_talk_external_research.v1 and parse successfully with a standard strict JSON parser. Escape literal quotation marks inside query strings and other JSON string values. Do not invent hashes or candidate IDs; Region Talk computes them after import. Use null only where the schema allows it. Preserve uncertainties instead of guessing. If the interface truly cannot create a file, return the same single strict JSON object as the entire response so it can be saved without editing.

OPERATIONAL DEFINITIONS
1. A source is external/nonregional only when both are true: (a) its editorial scope is federal, supraregional, national, or international; (b) Kaliningrad is one subject/case among others, not the core of the outlet.
2. A local edition of a federal network remains regional. A Kaliningrad outlet or university remains regional regardless of an external author.
3. A substantial publication contains research, analysis, criticism, an interview, essay, review, explanatory guide, or original professional project description. A snippet, listing, press release, event card, copied text, SEO page, or incidental mention is not substantial.
4. research_match and product_policy_match are different. A useful lead can be retained for research while blocked from the product.
5. Your maximum downstream_readiness is candidate_report. Never output ready_for_queue, approved, confirmed, or autopublish.

BROAD DISCOVERY — DO NOT START FROM A FIXED DOMAIN LIST
Run separate broad searches for every contour below, then use domain diversity, related links, references, DOI/citation chains, authors, institutions, and topic synonyms to expand beyond the first search results:
- peer-reviewed natural science;
- social science, geography, regional studies, migration, economy, and identity, excluding current political/military analysis;
- architecture and urban criticism;
- substantive professional project descriptions;
- history, memory, heritage, Kant, Königsberg/East Prussia when the modern region is materially connected;
- film/festival criticism and cultural analysis;
- cultural features and explanatory long reads;
- intellectual interviews, essays, and reviews;
- conditional editorial travel/city guides with original substance and no tour/booking sales.

Search in every configured research language. Use not only “Калининград” / “Калининградская область”, but relevant anchors such as Кёнигсберг/Königsberg, Куршская коса/Curonian Spit, Вислинский/Калининградский залив/Vistula Lagoon, Baltic coast, Kant, amber, Planet Ocean, festival «Короче», regional identity, migration, architecture, archaeology, ecology, marine science, museums, and heritage. These are discovery hints, not automatic relevance proof.

Record actual queries and opened domains in coverage[]. The result is a bounded qualitative research run. Never describe its counts as the complete number or frequency of publications on the web.

PAGE VERIFICATION
For every candidate, open the primary article/paper page; a search snippet is discovery evidence only. Verify title, author(s), original publication date, genre, access state, outlet identity/scope, how central Kaliningrad is, commercial status, and whether the page is the original rather than a syndication/aggregator copy. Open an About/editorial-policy/journal-policy page when source scope or peer-review status is unclear. Prefer DOI/publisher originals; relate preprints, syndications, and commentary in related_items.

Distinguish original publication date from modification, indexing, issue, or search-snippet dates. Unknown/uncertain date, source scope, sponsorship, or primary-page access cannot be a clean candidate.

HARD PRODUCT EXCLUSIONS
Set product_policy_match=false, downstream_readiness=blocked, and a precise hard_exclusion_code for:
- regional/local outlet or local branch;
- current news without a durable analytical layer;
- politics_conflict, war_military, defence/security analysis;
- incident_crime;
- press release without independent analysis;
- sponsored/native advertising, affiliate copy, tour/excursion/booking sales;
- pure event/listing/service card;
- SEO filler or copied/syndicated text without original value;
- Kaliningrad as secondary/episodic/incidental mention;
- retracted paper;
- unsafe or deceptive material.

REGIONAL IMAGE AND TONE POLICY
Prioritize evidence-based materials whose dominant reader effect is positive: they reveal something valuable, distinctive, beautiful, intellectually interesting, inventive, or human about the region. Especially prefer science and discovery with a clear popular-science insight; culture and heritage; nature and the Baltic coast; thoughtful architecture and urban projects; creative communities; responsible travel; and achievements that connect Kaliningrad with a wider Russian or international context.

Do not search for or promote sharply negative portrayals. If the dominant framing stigmatizes the region or its residents, presents it chiefly as hopeless, dangerous, hostile, backward, ugly, absurd, or merely a threat, uses contempt/ridicule, or relies on sensational/catastrophizing language, do not include it in candidates[]. Put it in excluded[] with reason_codes=["sharp_negative_region_image"].

Do not confuse honest caveats with sharp negativity. A neutral or problem-focused scientific/editorial material can remain eligible only when it is respectful and balanced, explains causes/evidence/limits, offers a meaningful or constructive reader takeaway, and does not leave the region with a dominantly negative image. Mark such a row with boundary_flags=["constructive_neutral_image"] or ["mixed_region_image"]. `mixed_region_image` or uncertain dominant effect requires research_decision=needs_review and downstream_readiness=manual_review_required.

Positive priority never permits propaganda, invented achievements, hidden advertising, suppressed scientific limitations, or removal of material caveats. Factual accuracy and evidence remain mandatory. Within otherwise equally strong candidates, order candidates[] by: positive regional-image effect, public interest, evidence quality, originality, source diversity, then accessibility.

QUALITY TRACKS
Assign exactly one: scholarly, professional_editorial, popular_editorial, reference_or_project_catalog. Score each required quality dimension 0..4 with a reason and evidence_refs. Scores are diagnostic integers, not probabilities and not an automatic accept formula.

For scholarly works separately verify publication_status, explicit peer-review basis, study type, methods/data/sample scope, stated limitations, funding/conflicts, and correction/retraction status. A DOI or famous publisher alone is not peer-review or quality evidence. A preprint requires manual review. A retraction is blocked. Corrections or expressions of concern require a visible caveat and manual review.

PUBLIC-INTEREST TEST
A material should be interesting beyond a narrow specialist circle. State the concrete understandable insight, why it matters to a resident/visitor/curious reader, and the expected jargon barrier. For science, preserve study scope and limitations; do not turn association into causation and do not say “scientists proved” unless the study design supports it. A rigorous but inaccessible or non-actionable paper may remain research_match=true while product_policy_match=false or needs_review.

EDITORIAL PACK
Write in Russian: a neutral short title; a 1–2 sentence teaser explaining what the material contributes and why one might open it; a short source_overview describing outlet type and scope, not prestige; reader_takeaway; why_selected; and a caveat when needed.

Do not invent outlet reputation, biography, affiliations, peer review, methods, findings, or image rights. Every factual sentence in teaser, source_overview, reader_takeaway, why_selected, and caveat must have a copy_support entry whose evidence_refs resolve to evidence[]. Use paraphrases; quote_short is optional and must be very short.

MEDIA AND RIGHTS
Default to rights_policy=link_only, media_reuse_allowed=false, media_gate_status=not_evaluated. Include only direct candidate image URLs visible on the primary page. Set reuse_verified only when the exact asset licence/permission is explicitly evidenced. Image quality will be evaluated later by Region Talk; do not predict or fabricate its score.

FINAL SELF-CHECK
- The downloadable UTF-8 .json file was created, parses with a strict standard JSON parser, and contains no prose or Markdown outside the object.
- JSON conforms exactly to the supplied schema.
- Every candidate primary page was opened.
- Every evidence reference resolves.
- Every clean candidate is external, central/substantial, in-window, non-news, noncommercial, language-compatible, and has no hard exclusion.
- Every clean candidate has a positive or clearly constructive-neutral dominant effect on the image of the region; sharp-negative material is excluded and mixed/uncertain effect is manual review.
- Unknowns remain needs_review/unresolved.
- Deduplicate DOI, then canonical URL, then normalized title+authors; preserve relationships.
- Keep excluded and unresolved rows so the next run does not rediscover the same noise.
```

## Operator chat and ranking

Candidate messages now show the original link, source link, overall score, image score, postcardness, verifier reason, and `О публикации` for editorial/academic origin. Automatic confirmed-candidate delivery remains idempotent.

An explicit one-shot request renders a **read-only** queue without marking candidates `sent_to_chat`:

```bash
python3 scripts/region_talk_goal_notify.py --queue --limit 20 --dry-run
# remove --dry-run to send the snapshot to the pinned Region Talk operator chat
```

Queue policy `region_talk_mmr_adjacency_v1` starts with highest quality, then greedily maximizes `quality - diversity_weight × max_similarity` against selected items and durable publication history. It prefers a next item below the similarity-to-previous threshold and compares only matching `model_id + encoder_contract + embedding_dim + encoding` BGE vectors. Missing/incompatible vectors use a disclosed source/topic/content fallback. If every remaining item is too similar, the queue continues with `adjacency_relaxed=true` and a visible reason.

The chat snapshot includes policy version, stable snapshot ID, rank score, quality score, maximum similarity, fallback marker, and unavoidable adjacency relaxation. This fixes heuristic-only ordering for on-demand review; it does not silently pretend missing vectors are semantic evidence.

## Release boundary

Implemented now:

- versioned external-search contract and ready-to-run prompt;
- fail-closed, idempotent YDB staging importer with row-level error ledger;
- review-card numeric evaluation fields and editorial wording;
- read-only on-demand queue renderer with compatible-vector MMR and anti-adjacency;
- strict intake-to-CandidateReport projection and external-publications-only canary mode;
- editorial/academic E5+BGE positive prototypes and origin-aware final verifier;
- compact publisher attestation joins in CandidateReport before image admission
  and again in the finalizer, without adding web publishers to the Telegram/VK
  scan queue, plus rights-field propagation through candidate/image/publication
  state. A row reactivated from the old missing-attestation defer is explicitly
  eligible for the next image batch even if the legacy cursor had already
  advanced past its queue order. The metadata-only media sidecar is merged over
  that authoritative text/source row and cannot erase an existing fused-vector
  accept merely because the sidecar omits vector fields; its later no-op merge
  also preserves the same-run defer-to-fetch transition used by changed-only
  YDB persistence and cursor recovery. ImageDiagnostic consumes
  `selected_for_next_image_batch=true` ahead of older unselected backlog, so
  the CandidateReport handoff is operational rather than display-only. For a
  web publication it downloads only the direct image URL supplied by the
  research contract, scores the decoded bytes, and preserves
  `link_only`/`score_only_no_reuse`; it does not invent a platform fallback or
  grant reuse rights.

Still required for each imported external-publication candidate to become confirmed:

- a real E5 pass followed by the isolated BGE-M3 pass for the same current text hash;
- actual-image diagnostics when a direct article image exists; missing/weak imagery remains review/deferred rather than receiving an invented score;
- final-verifier acceptance and human operator review before any queue/publication decision;
- ongoing source-rights review if the teaser will use anything beyond a link and owned text.

Import alone never makes a row confirmed. Manual-review/blocked rows remain staging evidence; strictly ready rows can advance only by satisfying every normal downstream gate above.
