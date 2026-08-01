# Внешние издания и научные публикации

Status: **contract + importer + guarded CandidateReport consumer + diversified queue implemented**. Only intake rows explicitly marked `ready_for_region_talk_scoring` can enter the existing E5/BGE/image/final-verifier funnel; manual-review and blocked research rows remain outside it. This is an extension of **Region Talk**, not a separate channel or crawler.

## Product decision

Region Talk covers two origins under one editorial promise — «как о Калининградской области говорят за пределами региона»:

1. `external_social` — posts by nonregional Telegram/VK authors;
2. `editorial_publication` / `academic_publication` — substantive work in a nonregional publication, journal, professional platform, or cultural outlet.

The outlet must remain external, supraregional, federal, or international. A local Kaliningrad outlet/university does not become external merely because an author is from elsewhere. Author affiliation, outlet scope, research object, and publication language are stored separately.

This lane is for analysis, criticism, research, explanatory features, interviews, essays, reviews, and substantive project descriptions. It is not a back door for current news, politics, military/security material, incidents, press releases, event cards, tours, native advertising, SEO pages, or incidental mentions.

The attached 2025–2026 research is treated as a **map of discovery contours**, not an allowlist and not exhaustive frequency data. Military/security examples remain research evidence only and are a hard product exclusion.

The saved research prompt also has an explicit diversity/completion gate.
External research must not stop after the first easy scholarly results: it
searches every declared contour, targets a broad 20–30-item pool when evidence
and the live registry limits permit, normally keeps scholarly work below
25–30%, and aims for at least half professional/popular editorial material.
These are anti-bias and search-depth guardrails, never a reason to admit a weak
page or manufacture a quota. Exhausted contours are reported with their actual
queries and reasons instead.

## Lightweight architecture

```text
saved broad external web-research prompt
  → fetch current public read-only registry generated from YDB
  → strict region_talk_external_research.v1 JSON
  → local validation + live-YDB DOI/canonical-URL dedupe
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

### Article media and provenance policy

- Region Talk natively carries one publisher-associated source hero with a
  prominent source name and exact original link; legacy `link_only` /
  `score_only_no_reuse` values remain research provenance only.
- RegionTalkImageDiagnostic proves article association and visual suitability;
  it must not substitute a site logo, related-content thumbnail or stock image.
- A paper/PDF/paywalled page without a materializable suitable image receives a
  recorded terminal system-link-preview fallback; it does not receive a fake
  image score or an unrelated card.
- A deterministic owned card remains a separate future format and never
  replaces a source image while pretending to be source media.

## Machine contract and importer

Canonical JSON Schema: [`external-publication-research.schema.json`](external-publication-research.schema.json), version `region_talk_external_research.v1`. Importer version: `region_talk_external_publication_import.v1`.

Dry-run validation:

```bash
python3 scripts/region_talk_external_publication_import.py research-result.json \
  --report artifacts/codex/region-talk-external-publication-import.json
```

Explicit YDB staging write:

```bash
python3 scripts/region_talk_external_publication_import.py research-result.json \
  --execute
```

The importer:

- accepts JSON only;
- validates the complete Draft 2020-12 JSON Schema with format checking; candidate-local schema failures enter the row error ledger, while invalid run/coverage metadata aborts the batch;
- strips URL fragments and tracking parameters;
- rejects non-HTTP schemes, private/local/reserved hosts, and non-web ports;
- prefers normalized DOI identity, then canonical URL;
- upserts stable IDs, so replay is idempotent;
- reads the current durable YDB ledger itself for every executing import and
  rejects already-known URL/DOI identities even if the external agent used an
  older registry snapshot;
- persists candidates, exclusions and unresolved leads into
  `external_publication_seen_item`, then automatically republishes the stable
  public registry so future launches suppress both accepted work and
  previously checked noise;
- reports invalid rows independently instead of aborting the valid batch;
- requires every non-empty editorial-copy surface to have evidence-backed `copy_support`, and every referenced evidence ID to resolve;
- forbids a clean candidate with regional/unknown source scope, out-of-window or snippet-only date, news/sales classification, hard exclusion, language mismatch, unverified product-policy match, non-full-text access, or unverified source externality;
- requires a clean candidate to have `strong|credible` quality tier and at least `2/4` for regional centrality, broad public interest, and accessibility; scholarly rows additionally require verified peer review with no correction/retraction flag;
- validates direct candidate media URLs as public HTTP(S) URLs before any future image handoff;
- never fetches pages and therefore is not an SSRF-capable crawler;
- writes `external_publication_intake_item`, a compact publisher attestation as `external_publication_source_item`, row errors as `external_publication_import_error_item`, and `external_publication_import_batch`;
- does not itself write `candidate_memory_item`, `image_queue_item`, or `publication_candidate_item`: only CandidateReport may promote a strictly ready row through the normal gates.

### Autonomous server-side research stage

`scripts/region_talk_external_research_autorun.py` is the bounded control-plane
adapter for the saved research prompt. It uses the live public registry, asks a
grounded model to search with both Google Search and URL Context, parses one
strict JSON result, and then invokes the same schema validation, live YDB
duplicate guard, idempotent importer and registry publication used by an
operator-supplied file. It never opens or receives a Telegram session.

The scheduled wrapper can execute this stage before the ordinary queue
orchestrator so newly staged articles can enter CandidateReport during the same
90-minute session. The stage is intentionally independent from Telegram/VK
discovery: a web-provider error is recorded but cannot stop the social
Candidate/BGE/Image/finalizer chain. A durable six-hour success marker prevents
duplicate paid research calls when scheduler slots overlap or are retried.

The production switch is `REGION_TALK_EXTERNAL_RESEARCH_ENABLED=1` and is
**off by default until a search-enabled provider project passes a live smoke**.
Ordinary text-generation quota does not imply web-grounding quota. On
2026-07-31 the configured redacted `GOOGLE_API_KEY3` lane returned provider
`429 RESOURCE_EXHAUSTED` for both `gemini-3-flash-preview` and the stable
`gemini-3.1-flash-lite` when `google_search` + `url_context` were enabled. A
bounded production recheck on 2026-08-01 returned the same `429` for both the
`GOOGLE_API_KEY3` and separately reserved `GOOGLE_API_KEY4` lanes before any
research result was generated. Therefore the switch remains off: enabling
three guaranteed failures per day would be false autonomy. This is a
Search-grounding project-quota blocker, independent from the model-capability
and prompt/schema contracts. The worker now requests
`gemini-3.5-flash-lite` first and uses `gemini-3.1-flash-lite` only after a
model-scoped limiter block. It remains disabled until a search-enabled provider
project passes the bounded live smoke. This is a production discovery worker;
Flash/Lite output must never be represented as a Gemini Pro consultant verdict.

Provider references: [Google Search grounding](https://ai.google.dev/gemini-api/docs/google-search),
[structured outputs with tools](https://ai.google.dev/gemini-api/docs/structured-output),
[Gemini 3.5 Flash-Lite capabilities](https://ai.google.dev/gemini-api/docs/models/gemini-3.5-flash-lite),
and [Gemini 3.1 Flash-Lite capabilities](https://ai.google.dev/gemini-api/docs/models/gemini-3.1-flash-lite).

### CandidateReport handoff

`RegionTalkCandidateReport` reads `external_publication_intake_item` beside the
existing row-level Region Talk state. Its adapter is fail-closed and admits a
row only when all of these remain true at read time:

- `decision.import_status=ready_for_region_talk_scoring`;
- `decision.downstream_readiness=candidate_report`;
- `product_policy_match=true` and there is no hard exclusion;
- no later operator override blocks the row.

After the strict intake handoff, editorial and academic publications first use
the media-first integrity path: ImageDiagnostic extracts publisher image
candidates, verifies article association and selects one hero; JS-only pages
go through the bounded browser materializer. Research `rights_policy` remains
provenance metadata and cannot downgrade an associated source image to a
premature link-only post. The source, region, fused E5+BGE-M3, exact
external-article vector and controlled Gemini gates remain mandatory.

Only an explicit terminal media result (`not_reviewable_no_media`, exhausted
browser/fetch failure, or another terminal system-preview recommendation) may
enter `region_talk_external_article_terminal_link_preview_fallback_v2`. An
untouched candidate-memory row cannot use the fallback. The finalizer records
`media_review_mode=system_link_preview_terminal_fallback` instead of pretending
a picture was reviewed.

The adapter creates a `platform=web` scoring projection, never a synthetic
Telegram/VK post. It preserves `content_origin_type`, canonical publisher key,
research quality, source overview, diversity topics and rights fields. Direct
article images are source-media candidates: ImageDiagnostic must prove their
association and quality, while the public renderer carries prominent source
and original links rather than using the research rights field as a format
gate.

The strict research/import commerciality decision also takes precedence over
the recall-oriented social ad regex before Gemini. A ready external row must
already be `non_news`, `independent|institutional_noncommercial`, product-policy
matched and free of hard exclusions. CandidateReport therefore retains any
regex hit as diagnostic evidence but does not let wording in an editorial
caveat (for example, a mention of a hotel selection) tombstone the article.
This exception cannot apply to Telegram/VK rows or to
`sponsored|sales|unknown` research classifications; Gemini still performs the
final semantic ad/editorial decision.

### Evidence-backed operator review

Rows held in `manual_review_required` are promoted only through an explicit,
auditable review file and remain fail-closed by default:

```bash
python3 scripts/region_talk_external_publication_review.py review.json
# inspect the dry-run report, then:
python3 scripts/region_talk_external_publication_review.py review.json --execute
```

Contract `region_talk_external_publication_review.v1` requires the exact intake
ID, reviewer and timezone-aware review time, every currently blocking
`resolved_reason_codes` value, public evidence URLs with field-level support,
and only narrow allowlisted field corrections. Approval rechecks the original
product gates: exact in-window date, full text, external source, language and
product-policy match, no hard exclusion, sufficient quality scores and, for
scholarly work, peer review plus checked correction/funding/conflict status.
The command updates the intake and seen ledger and writes a separate
`external_publication_review_item`; it never writes a publication candidate or
publishes. `--execute` is required for any YDB change.

The E5 and BGE-M3 semantic bank contains separate
`ko_editorial_publication` and `ko_academic_publication` positive classes. The
final verifier uses the origin-aware
`region_talk_final_verifier_v7_grounded_draft` contract:
editorial/academic work needs attributed analysis/research, an evidence or
expert basis, a concrete public-interest insight and a memorable useful detail;
it does not need a first-person trip. Sharp negative regional framing remains a
hard rejection. The social lane keeps its former firsthand/emotion rules.

For a bounded canary that does not touch Telegram/VK acquisition, launch
CandidateReport with `REGION_TALK_EXTERNAL_PUBLICATIONS_ONLY=1`. This changes
acquisition scope only; YDB, dual-vector, image, final-verifier and operator
gates remain the same.

## Ready-to-run external prompt

Canonical stable prompt: [`external-publication-research.prompt.txt`](external-publication-research.prompt.txt).
It is the **only file the operator needs to save and launch**. Do not edit it
and do not generate an attachment before a run. At execution time the agent
must fetch the exact stable URL of the live read-only registry:

`https://static.kenigevents.ru/region-talk/external-publications/research-registry.json`

The exact result-schema URL is written directly into the saved prompt as well:

`https://static.kenigevents.ru/region-talk/external-publications/result.schema.json`

The registry contains non-secret policy plus the current projection of prior
candidates, exclusions and unresolved leads. It also points to the current
result JSON Schema. If the registry or result schema is unavailable, the agent
must stop rather than search without duplicate protection.

Do not append a cache-busting query parameter. The object is already served
with `Cache-Control: no-cache, no-store, must-revalidate`; keeping the exact URL
also satisfies research environments that only permit URLs already present in
the prompt. The agent must additionally verify that the schema URL and version
declared by the live registry equal the literal schema URL and contract version
in the prompt. This avoids relying on a URL discovered only inside a fetched
JSON document, which some restricted research environments refuse to open.

The registry is rebuilt from YDB and published automatically after every
successful `--execute` import by
`scripts/region_talk_external_research_registry.py`. The old generated request
sidecar and `--request-input` remain accepted only for compatibility with a
research run that was already started; they are not part of the normal launch
workflow.

The search engine may still display a known URL, but the agent skips it before
detailed page verification. The importer is the independent second layer: it
re-reads live YDB at import time and rejects a URL/DOI that appeared after the
agent fetched its registry snapshot.


## Operator chat and ranking

Candidate messages now show the original link, source link, overall score, image score, postcardness, verifier reason, and `О публикации` for editorial/academic origin. Automatic confirmed-candidate delivery remains idempotent.

### Article galleries and editorial image suitability

External publications are not limited to a single OG preview. After the text
and source gates accept a candidate, ImageDiagnostic downloads a bounded set of
images explicitly declared by the page as its lightbox/gallery
(`data-fancybox`/`data-lightbox`, maximum 20). It does not scrape arbitrary
image URLs from page chrome, recommendations or navigation. The imported
direct image URL remains a fallback.

Visual evaluation is genre-aware. Architecture, interiors, museums,
exhibitions, science and documentary editorial photography do not have to look
like an outdoor travel postcard. Their composition, light, space, material,
detail, technical quality and teaser value are considered by the selective
visual adjudicator. This is a review/routing correction, not an uncalibrated
automatic-score threshold reduction.

Human `approve_visual` feedback is supported as a durable, manifest-bound
visual attestation. It never bypasses regional scope, nonlocal-source,
anti-ad/news/negative-image rules, dual-vector evidence, exact attribution or
the final Gemini publication verifier. Research rights fields remain auditable
metadata but do not downgrade the native attributed source-media format.

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
- stable public live registry and one-file saved-prompt launch, with automatic
  registry refresh after imports and an authoritative live-YDB import guard;
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
  web publication it downloads only publisher-declared article-image evidence,
  scores the decoded bytes, preserves legacy provenance fields for audit, and
  emits an exact refetch manifest for native attributed delivery. It does not
  invent an unrelated platform fallback or substitute another asset.

The finalizer normally treats candidate memory as the refreshable text source.
For an external-publication canary that intentionally stopped after the early
image handoff, a newer image row's explicit Region Talk scope attestation wins
over older candidate-memory scope fields; a later memory refresh supersedes it
again. This prevents the canary shortcut from reviving a stale pre-fix
`kaliningrad_oblast_only_scope=false` value.

Still required for each imported external-publication candidate to become confirmed:

- a real E5 pass followed by the isolated BGE-M3 pass for the same current text hash;
- actual-image diagnostics when a direct article image exists; missing/weak imagery remains review/deferred rather than receiving an invented score;
- final-verifier acceptance and human operator review before any queue/publication decision;
- ongoing source-rights review if the teaser will use anything beyond a link and owned text.

Import alone never makes a row confirmed. Manual-review/blocked rows remain staging evidence; strictly ready rows can advance only by satisfying every normal downstream gate above.
