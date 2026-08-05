# Hero-talk track в плане релиза статического сайта

> **Статус:** составная часть static-site release plan.  
> **Дата:** 2026-08-03.  
> **Канонический продуктовый документ:**
> [`../hero-talk/README.md`](../hero-talk/README.md).  
> **Полный staged release contract:**
> [`../hero-talk/release-plan.md`](../hero-talk/release-plan.md).  
> **GitHub Actions strategy:**
> [`../hero-talk/testing.md`](../hero-talk/testing.md).

Этот track добавляет Hero-talk в общий план релиза статического сайта, не
ослабляя существующий `NO-GO` event-page/root promotion и не объявляя
историческую typed-briefing лабораторию production-ready.

## Release status

| Область | Статус |
|---|---|
| Текущий `HomeHeroTalk` static skeleton | существует в `main` |
| Каноническая product/chain model | подготовлена |
| Приветствия и local-identity families | зафиксированы |
| Связь с standard-user onboarding | зафиксирована на уровне contracts |
| Deep research narrative chains | prompt готов, исследование не выполнено |
| Production phrase packs | отсутствуют |
| Generation-time LLM pipeline | отсутствует |
| Chain compiler | отсутствует |
| Page-end placement | отсутствует |
| Cross-device thread/return state | отсутствует |
| `promo_activity.surface=hero_talk` | planned |
| Image mosaic production integration | не реализована в current `main` |
| Video mosaic | noindex experiment only, fixture отсутствует |
| GitHub Actions Hero-talk scenarios | спроектированы, не зарегистрированы/реализованы |
| Public rollout | `NO-GO` |

## Dependencies

Hero-talk release зависит от:

- завершения deep research смысловых цепочек;
- согласованного глобального редакционного стиля;
- onboarding capability registry/state;
- personalization activation/profile contracts;
- current catalog/lifecycle/fact provenance;
- static build manifest and last-good publication;
- central static-site QA registry/control plane;
- owner visual/editorial acceptance.

## Required order

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

Конкретные GO/NO-GO, flags, rollback и evidence описаны в каноническом
[`Hero-talk release plan`](../hero-talk/release-plan.md).

## Static-site gate

Hero-talk не является отдельным разрешением на production root promotion.
Любой static-site RC, включающий Hero-talk, обязан доказать:

- полезный static/no-JS fallback;
- отсутствие runtime LLM;
- exact phrase/program/manifest hashes;
- отсутствие неподтверждённых facts;
- chain graph/coherence acceptance;
- first-click/a11y/motion acceptance;
- page-end route matrix, если она включена;
- last-good and independent feature rollback;
- сохранность quick navigation, feed, canonical content, NPS и footer.
