# Финальный SEO/GEO-аудит перед публикацией

> **Статус:** обязательный поздний pre-release gate. Он запускается только после
> owner-accepted UI/UX freeze на immutable release candidate.

## Почему поздно

SEO/GEO зависит от окончательных headings, visible facts, navigation, internal
links, image behavior, lifecycle pages и structured data. Проверка нестабильного
макета создаёт одноразовое evidence и не разрешает снять `noindex`.

Порядок:

```text
feature/UI integration
-> owner UI/UX acceptance
-> immutable full static build
-> deterministic audit + independent semantic reviews
-> fixes
-> complete rerun
-> indexing decision
```

Любая поздняя правка visible copy, hierarchy, navigation или page family снова
открывает затронутый UI/UX gate и требует нового SEO/GEO run.

## Обязательный scope

- home, date/weekend/category/popular/collection/festival/venue pages;
- event detail для всех поддерживаемых event shapes;
- sitemap, robots, canonical/redirect/error/lifecycle behavior;
- JSON-LD и видимые факты;
- OG/share metadata и media URLs;
- mobile/desktop/no-JS output;
- private preview, bearer, auth/admin/profile surfaces как negative inventory.

## Три независимых lane

1. **Deterministic repository/runtime audit:** crawl inventory, statuses,
   canonical, redirects, sitemap, robots, JSON-LD parity, internal link graph,
   no-JS, performance/media and exact-SHA traceability.
2. **Independent GEO/adversarial review:** query intent, entity clarity,
   passage answerability, crawler accessibility, source/freshness transparency.
3. **Independent semantic/architecture review:** visible-versus-structured truth,
   lifecycle edge cases, thin/duplicate templating and human UX risk.

Raw prompts, outputs, provider/model status and timestamps remain immutable.
Failed/empty consultant lane is blocker, not substituted by a lower model.

## Hard rules

- critical event facts are visible in initial HTML;
- JSON-LD never contradicts visible canonical facts;
- preview/personal/auth/admin URLs do not enter sitemap/indexable set;
- stale/cancelled/rescheduled pages have one documented lifecycle policy;
- sitemap `lastmod` reflects meaningful change, not every rebuild;
- programmatic thin/empty pages fail closed from indexing;
- `llms.txt` may be a transparent directory but does not replace HTML, sitemap,
  structured data or provenance;
- GEO is truthful entity clarity, not hidden AI-only text or fabricated authority.

## Acceptance

- [ ] 100% intended public URLs have correct status/canonical/indexability.
- [ ] Zero private/preview/duplicate URL leaks.
- [ ] Zero visible/JSON-LD/ICS factual contradictions.
- [ ] Full no-JS/internal-link/mobile/media/performance gate passes.
- [ ] Every Critical/High finding is fixed or explicitly disproved with evidence.
- [ ] Final acceptance is bound to the exact promoted SHA/build.
- [ ] Root `noindex` is removed only after this gate is green.
