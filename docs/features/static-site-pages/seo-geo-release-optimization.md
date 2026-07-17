# Final SEO/GEO release optimization and independent audit

> **Status:** mandatory late pre-release gate; planning only, not started.
> **Hard dependency:** this work begins **only after the public UI and UX are frozen and accepted** and all release-scope features that can change public HTML/navigation/content are integrated on an immutable `origin/main`-reachable SHA/build. It must not run in parallel with unresolved navigation, layout, content hierarchy, interaction design or public page-family work.
> **Goal:** make the canonical public static site maximally transparent and machine-readable for search/answer systems while remaining fast, truthful and friendly to ordinary search-engine results and human users.

## Why this is a separate late stage

SEO/GEO findings depend on the final information architecture, headings, visible facts, internal links, responsive DOM, image behavior and page templates. Auditing an unstable UI produces disposable evidence and encourages metadata fixes against pages that no longer exist.

Therefore:

1. F5/UI/UX, responsive navigation, medallions, final content-block hierarchy and all release-scope features that affect public HTML are accepted first.
2. The accepted UI/UX SHA and full static preview become the immutable audit baseline.
3. Only then do the three independent SEO/GEO audits start.
4. Non-visual fixes may proceed against that baseline. Any fix or late feature change that changes visible copy, block order, navigation, interaction, page families or layout reopens the affected UI/UX acceptance, after which the SEO/GEO audit is rerun on the newly accepted SHA.
5. Root `noindex` is not removed and public indexing is not enabled until the final SEO/GEO acceptance pass is complete.

Existing SEO-supporting implementation may remain in the codebase, but it is not accepted as release evidence before this stage.

## Scope

The audit covers the full canonical public surface, including at least:

- home/discovery and full-catalog entry points;
- today, tomorrow, weekend, popular and other approved date/listing pages;
- event-detail pages for every supported event shape;
- public category, venue, festival and normalized saved-search tag pages that exist in the RC;
- sitemap, robots, error pages, redirects, canonical URL variants, assets and structured-data endpoints;
- public share/OG previews insofar as they affect search and entity understanding;
- expired/cancelled/postponed/rescheduled event lifecycle behavior;
- CDN/HTTP behavior for HTML, WebP/SVG assets and crawler access.

Private or bearer surfaces are audit targets only to prove exclusion:

- forwarded personal secret pages remain `noindex` and absent from sitemaps;
- auth callbacks, admin pages, private search candidates, user-specific states and preview builds must not become indexable;
- personalization must not create crawlable query-parameter duplicates or different canonical facts.

This gate does not promise a ranking position or AI citation. It proves that the site exposes correct, accessible and unambiguous evidence and removes avoidable technical/content barriers.

## Mandatory three-agent audit

All three lanes receive the same immutable evidence pack and audit the complete scope independently before seeing one another's conclusions. Each lane may emphasize a specialty, but no lane is a substitute for another.

### Lane A — Codex / primary repository auditor

Codex owns the reproducible factual audit and final synthesis:

- crawl/build inventory and indexability matrix;
- rendered HTML, canonical, robots, sitemap, redirect and status checks;
- JSON-LD/schema consistency against visible canonical event facts;
- Playwright/no-JS/mobile inspection, link graph and duplicate-page analysis;
- performance/CDN/image/metadata checks;
- repository-to-public-output traceability;
- consolidated finding ledger, implementation routing and regression reruns.

Codex must separate observed evidence from recommendations and cannot mark its own implementation fixed without rerunning the relevant public-output check.

### Lane B — independent `agy` Gemini Pro audit

Run an independent audit through Antigravity/`agy` using Gemini Pro High. The evidence record must contain the command, provider-visible model/resolved model id, prompt, raw output, timestamp and exit/provider status.

Accepted consultant class is limited to:

- `gemini-3.1-pro-preview`; or
- `gemini-3-pro-preview`.

An `agy --model "Gemini 3.1 Pro (High)"` label is accepted only when the artifact proves it resolved to the approved Pro-class model. Flash, Flash-Lite, Lite, Gemma or an unspecified fallback may be useful as supplementary probes but cannot satisfy this lane.

Gemini's lead focus is adversarial discovery and GEO: query-intent coverage, entity clarity, passage-level answerability, AI crawler accessibility, citation/source transparency and cross-page ambiguity. It still reviews the full technical SEO contract.

### Lane C — independent `a-opus` audit

Run `a-opus` through Antigravity/agy with the project policy of Opus and high effort. Preserve the exact prompt, raw answer, command/status, timestamp and tool/model evidence.

Opus's lead focus is semantic and architectural integrity: information hierarchy, canonical/entity model, lifecycle edge cases, visible-versus-structured-data consistency, misleading or thin templating, source/freshness communication and whether proposed optimizations harm human UX. It still reviews the full SEO/GEO surface.

### Independence and blocker policy

- The first-pass prompts share facts/artifacts, not another agent's conclusions.
- Raw reports are immutable inputs to synthesis; later discussion is saved separately.
- Provider failure, empty output, timeout, quota/capacity or authorization failure is recorded with exact evidence and blocks completion of that lane.
- A failed Gemini Pro lane is not replaced by a lower Gemini model; a failed `a-opus` lane is not represented as Opus-complete.
- Because the product owner explicitly requires all three audits, one successful external consultant cannot replace the other for this gate.

## Common immutable evidence pack

Before the independent prompts are sent, produce a neutral pack without audit conclusions:

- accepted UI/UX SHA, build id and public immutable preview URL;
- full generated URL inventory grouped by page/template/event shape;
- rendered HTML samples plus machine-readable crawl results for the full public output;
- robots, sitemap(s), headers, redirect map, canonical map and error-page behavior;
- JSON-LD extracted per page shape and representative visible fact snapshots;
- internal-link graph, crawl depth, orphan/duplicate candidate report;
- desktop/mobile/no-JS screenshots of representative page families;
- WebP/SVG/image metadata and performance evidence;
- current indexability policy for preview, production, personal and admin surfaces;
- a representative Russian-language search/answer query pack without expected consultant conclusions.

Suggested ignored artifact layout:

```text
artifacts/codex/seo-geo-release/<rc-sha>/
  evidence/
  prompts/codex.md
  prompts/agy-gemini-pro.md
  prompts/a-opus.md
  raw/codex-audit.md
  raw/agy-gemini-pro.md
  raw/a-opus.md
  provider-status/
  finding-ledger.json
  synthesis.md
  fixes-and-reruns.md
  final-acceptance.md
```

## Detailed SEO audit

### Crawlability and index control

- Every intended public canonical URL returns the correct status and is reachable through static HTML links.
- Preview, secret personal, auth, admin, private candidate and parameter-duplicate surfaces are excluded consistently through routing, canonical, robots/meta and sitemap policy.
- `robots.txt` does not accidentally keep the production site out of ordinary search after GO and does not expose sensitive paths as a discovery mechanism.
- Sitemap contains only indexable canonical URLs, uses truthful `lastmod`, scales deterministically and never contains preview or bearer URLs.
- Redirects, trailing slashes, legacy/Telegraph coexistence, renamed slugs, merged events and HTTP/HTTPS/host variants converge without chains or loops.
- Real 404/410/lifecycle responses do not masquerade as indexable soft-404 event pages.

### Page metadata and search snippets

- Unique useful `title`, meta description, H1 and canonical exist for every approved page family without keyword stuffing or template collisions.
- OG/share metadata agrees with canonical visible facts and never leaks private/profile data.
- Dates, timezone, venue, city, admission, availability and lifecycle status are consistent across visible text, metadata, sitemap and structured data.
- Search controls and personalized UI use `data-nosnippet`/non-link semantics where appropriate without hiding the canonical event content.
- Expired, cancelled, postponed and rescheduled pages follow one documented retention/index/redirect policy.

### Structured data and entity integrity

- Validate the page-type-appropriate Schema.org graph from the rendered RC, not only source templates.
- Event identity, occurrences/series, organizers, performers, Place/address, Offer/availability, images, dates/timezone, status and canonical URL agree with visible facts and canonical DB projection.
- Breadcrumbs and site/organization identity are consistent across page families.
- JSON-LD contains no invented fields, stale ticket status, contradictory dates, private identifiers or URLs that users/crawlers cannot access.
- Rich-result warnings are triaged separately from factual errors; unsupported/deprecated schema is not added for cosmetic scores.

### Internal linking and content quality

- Approved date/category/venue/festival/tag hubs give each indexable event a bounded crawl path and do not create infinite calendar/facet combinations.
- Anchor text is human-readable and specific; orphan pages, duplicate hubs and near-empty/thin programmatic pages fail closed from indexation.
- Canonical event pages contain sufficient unique event-local facts rather than repeating generic city/category boilerplate.
- Related events and other-date links do not confuse separate events, series occurrences or canonical identity.
- Heading order, visible facts and page text remain useful without JavaScript.

### Performance and media

- Audit current Core Web Vitals and search-engine guidance from primary sources at execution time; record both lab and available field data without substituting one for the other.
- HTML remains CDN-fast and usable with personalization/search services unavailable.
- Runtime media follows the lightweight WebP/safe-SVG release contract, has stable dimensions/aspect behavior and does not cause layout shift.
- Hero/gallery lazy loading, preload, alt text and image URLs balance first paint with discoverability; essential content is not hidden behind interaction-only hydration.
- Compression, cache headers, MIME types, TLS, mobile behavior and error responses are correct on the canonical CDN domain.

### Search-engine evidence

- Validate Google, Yandex and Bing-relevant technical behavior using their current official documentation/tools available at audit time.
- Record index coverage and canonical observations when webmaster-console access is available; absence of access is explicit, not replaced with assumptions.
- Decide on IndexNow or other submission mechanisms only from current official support and the site's actual publication cadence.

## Detailed GEO / AI-search transparency audit

GEO is treated as truthful SEO and entity transparency for answer systems, not as hidden text, crawler-specific content or guaranteed citation manipulation.

### Static answerability

- The initial HTML gives an unambiguous answer to: what happens, where, when, who organizes/performs, admission/ticket state and current lifecycle status.
- Important facts appear in visible, self-contained sections with descriptive headings and can be understood without executing JavaScript or following an opaque client request.
- Page text distinguishes event facts from recommendations, inferred personalization and third-party discussion signals.
- Dates, locations, prices and status have explicit labels and machine-readable forms; ambiguous prose is not the only carrier of critical facts.

### Provenance, freshness and transparency

- Canonical pages expose an approved human-readable source/freshness model without leaking private ingestion internals.
- Updated time/`lastmod` is truthful and tied to meaningful event changes, not every rebuild.
- Organizer, venue, festival, performer and event-series entities use stable names/aliases and consistent URLs/identifiers where available.
- Derived discussion/popularity/personalization material is labeled so an answer system cannot confuse it with source-confirmed logistics.
- Conflicting or stale facts fail closed rather than remaining in hidden metadata after visible content changes.

### AI crawler and licensing policy

- Re-check current official crawler names and purposes at execution time; keep search/user-fetch access decisions distinct from model-training access decisions.
- Verify the chosen policy in `robots.txt`, CDN/WAF behavior and real access logs where available.
- Evaluate `llms.txt`/machine-readable guidance as an optional transparent directory, not as a replacement for crawlable HTML, sitemap, structured data or source quality and not as an assumed ranking lever.
- Document content-use/licensing signals only after product/legal approval; do not imply permissions the project has not granted.

### Citability and entity discovery

- Audit whether representative event, venue, date/category and city pages contain concise, attributable and independently understandable fact blocks.
- Check entity consistency across page title, headings, visible facts, JSON-LD, breadcrumbs, internal anchors and social metadata.
- Avoid machine-targeted filler, fabricated authority/author bios, mass FAQ boilerplate, duplicate location text or invisible AI-only passages.
- Record genuine brand/source references and first-party unique data transparently; do not manufacture mentions or reviews.

### Representative query pack

The frozen query pack covers at least:

- what is happening today/tomorrow/weekend in Kaliningrad and major regional cities;
- event-type and audience needs such as concerts, exhibitions, children/family, free/paid and accessible transport;
- named performer, organizer, venue and festival queries;
- exact event queries asking date, time, location, price, tickets and how to get there;
- changed/cancelled/rescheduled event queries;
- equivalent paraphrases used by ordinary search and answer systems.

For each query, record which canonical page/fact block should be discoverable, whether the answer is supported by visible HTML and whether tested search/AI systems cite or retrieve the correct URL. External ranking/citation observations are measurements, not release promises.

## Synthesis and remediation

Codex builds one finding ledger after all blind reports are saved:

```text
finding_id
surface/urls
seo | geo | both
observed evidence
severity = critical | high | medium | low
codex verdict
gemini verdict
opus verdict
consensus = 3/3 | 2/3 | unique | conflict
primary-source/runtime validation
owner/task
fix SHA
rerun evidence
final status
```

Rules:

- Consensus is prioritization evidence, not a vote that overrides observed facts.
- A unique critical/high finding is reproduced or disproved explicitly; it is not dropped because two agents missed it.
- Conflicts about crawler/schema/search behavior are resolved using current primary documentation and live rendered evidence.
- SEO/GEO work is implemented in separate scoped tasks; the audit document does not become an unreviewable mega-patch.
- Critical and High findings are release blockers. Medium/Low findings require explicit disposition and backlog routing, not silent omission.
- After fixes, Codex reruns the full deterministic audit; Gemini Pro and `a-opus` independently review the final diff/evidence and regression risks.

## Release acceptance

- [ ] UI/UX is frozen and owner-accepted, and every release-scope feature that can change public HTML is integrated, before the SEO/GEO stage starts; audit artifacts name that immutable baseline.
- [ ] Full public/negative-surface inventory is complete; no page family or generated URL class is silently sampled away.
- [ ] Independent Codex, approved Gemini Pro through `agy`, and Opus through `a-opus` reports exist with raw prompts/output/provider evidence.
- [ ] One reconciled finding ledger accounts for every recommendation and disagreement.
- [ ] Zero unresolved Critical/High SEO or GEO findings and zero factual structured-data/visible-content contradictions remain.
- [ ] Sitemap/canonical/robots/status/redirect checks pass for 100% of intended public URLs; preview, personal secret, auth/admin/private and duplicate parameter URLs do not leak into the indexable set.
- [ ] Core event facts and approved source/freshness signals are present in static HTML and consistent with JSON-LD/metadata across all supported event shapes.
- [ ] No-JS crawlability, internal-link reachability, mobile/CDN/media/performance gates and current search-engine validation pass.
- [ ] AI crawler/search-versus-training policy is explicit, current, tested at the CDN and does not rely on `llms.txt` as a substitute for the site.
- [ ] Representative search/answer query evidence is recorded with correct target URLs and supported fact blocks; ranking/citation uncertainty remains explicit.
- [ ] Any visible fix has reopened and passed the affected UI/UX visual acceptance, followed by a fresh final SEO/GEO run.
- [ ] Final three-agent acceptance is bound to the release SHA that will be promoted; an older preview report cannot authorize a changed build.

## Related documentation

- [Static-site pages](README.md)
- [Release UI contract](release-ui-contract.md)
- [Astro preview](astro-preview.md)
- [CDN asset delivery](cdn-asset-delivery.md)
- [Static release readiness checklist](../../reports/static-personal-announcements-release-readiness-2026-07-11.md)
- [Historical static-event-pages backlog](../../backlog/features/static-event-pages/README.md)
- [External consultant and Codex tooling policy](../../tools/codex-cli.md)
