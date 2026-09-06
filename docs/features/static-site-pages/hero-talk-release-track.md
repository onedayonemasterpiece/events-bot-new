# Hero-talk track в плане релиза статического сайта

> **Статус:** составная часть static-site release plan; research дополнен 2026-09-06, не production acceptance.  
> **Канонический продуктовый документ:**
> [`../hero-talk/README.md`](../hero-talk/README.md).  
> **Полный staged release contract:**
> [`../hero-talk/release-plan.md`](../hero-talk/release-plan.md).  
> **GitHub Actions strategy:**
> [`../hero-talk/testing.md`](../hero-talk/testing.md).

Этот track добавляет Hero-talk в общий план релиза статического сайта, не
ослабляя существующий `NO-GO` event-page/root promotion и не объявляя
историческую typed-briefing лабораторию production-ready.

## Автонаполнение и управление владельцем — исследование #642

Конкретный проект продолжает существующий Hero owner, без второго page-end CMS:

- [MVP: источники, Writer/Reviewer, расписание, актуальность, CDN/PWA, стоимость](../hero-talk/autofill-mvp.md);
- [EventsBot MCP: чтение, статистика, exact copy/цепочки, картинки и изменения](../hero-talk/owner-mcp-mvp.md);
- [Постановка на реализацию и HT-AF acceptance matrix](../hero-talk/autofill-implementation-prompt.md).

Связи: [исследование #642](https://github.com/onedayonemasterpiece/events-bot-new/issues/642),
[главная #641](https://github.com/onedayonemasterpiece/events-bot-new/pull/641),
[существующий Hero-talk #291](https://github.com/onedayonemasterpiece/events-bot-new/pull/291).
Новые интервалы, model routing и сгруппированный implementation order —
предложения для review; после принятия их нужно синхронизировать с release/testing
owners и доставить в main нормальным PR, не оставлять только в этом stack.

Проверенный 6 сентября main — `6fddf14aeb983f97bde96e5963e1c9a9ddf72590`.
Августовская ветка #291 используется для продолжения документов, не как база
новой runtime-разработки и не как поручение merge всей старой ветки.

## Release status

| Область | Статус |
|---|---|
| Текущий `HomeHeroTalk` static skeleton | прочитан в main; не полный chain runtime |
| Каноническая product/chain model | подготовлена |
| Приветствия и local-identity families | зафиксированы |
| Связь с standard-user onboarding | зафиксирована на уровне contracts |
| Deep research narrative chains | исходный широкий prompt сохранён; #642 не выдаётся за выполнение всех его вопросов |
| AutoFill home/page-end + owner MCP | конкретный MVP-проект и implementation handoff сохранены; реализация не выполнена этим исследованием |
| Production phrase packs | этим исследованием не создавались и не проверялись live |
| Generation-time LLM pipeline / compiler / page-end | нужны реализация и acceptance; docs не доказательство готовности |
| Cross-device thread/return state | только через готовые owner contracts; не prerequisite базового auto/verbatim MVP |
| `promo_activity.surface=hero_talk` | planned; существующие promo services переиспользуются |
| Image mosaic production integration | нужна проверка на общем Astro/SoT/Penpot корпусе |
| Video mosaic | отдельный noindex experiment, не AutoFill MVP |
| GitHub Actions Hero-talk scenarios | testing strategy + HT-AF-01…22; не заявлены PASS |
| Public rollout | `NO-GO` до соответствующих gates и owner authorization |

## Dependencies

Hero-talk release зависит от:

- согласованного chain/schema и редакционного стиля;
- onboarding capability registry/state для соответствующих hints;
- personalization/profile contracts для соответствующих personalized paths;
- current catalog/lifecycle/fact provenance;
- static build/public route receipts, immutable packs и проверяемой актуальности;
- central static-site QA registry/control plane;
- owner visual/editorial acceptance.

Generic/owner-verbatim/собственные редакционные цепочки не должны ждать всей
cross-device narrative memory. Голос, return delta, артефакты/клуб и paid promo
не рекламируются без собственной фактической готовности.

## Release mapping

Исходный staged track остаётся в [release-plan](../hero-talk/release-plan.md):

```text
HT-0 docs/research/schema
→ HT-1 static single-scene baseline
→ HT-2 handwritten chain compiler
→ HT-3 generation-time LLM phrase packs
→ HT-4 page-end context
→ HT-5 onboarding integration
→ HT-6 personalization/return/cross-device
→ HT-7 own editorial campaign activity
→ HT-8 image mosaic candidate
→ HT-9 video mosaic noindex experiment
→ HT-10 controlled product experiment/canary
```

Для ограниченного AutoFill MVP предложена продуктовая группировка, подлежащая
согласованию, а не молчаливой отмене всех прежних gates:

```text
A: domain + owner verbatim/MCP
B: shared renderer + home/page-end + image + fresh delivery
C: generation-time AutoFill
D: own promo + real statistics + end-to-end candidate
```

Это позволяет получить проверяемый продукт без ожидания video/cross-device
расширений. Каждая группа включает свои tests; таблицы/tools/list сами по себе
не закрывают ни общий MVP, ни owner product journey.

## Static-site gate

Hero-talk не является отдельным разрешением на production root promotion.
Любой static-site RC, включающий Hero-talk, обязан доказать:

- полезный static/no-JS fallback **внутри общего renderer**, не возврат legacy
  HomeHeroTalk как отдельной архитектуры;
- отсутствие runtime LLM;
- exact phrase/program/manifest/source hashes;
- отсутствие неподтверждённых facts;
- chain graph/coherence и owner-verbatim acceptance;
- first-click/a11y/motion acceptance;
- page-end route matrix, если она включена;
- отзыв после pause/cancel, CDN/SW/BFCache/expiry и validated rollback;
- сохранность quick navigation, Search, feed, canonical content, NPS и footer;
- Astro ↔ executable UI SoT ↔ Penpot на одних fixtures;
- owner MCP journey и честный analytics coverage/readout.
