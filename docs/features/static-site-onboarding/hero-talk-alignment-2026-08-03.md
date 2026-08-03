# Согласование стандартного онбординга с каноникой Hero-talk

> **Статус:** нормативная корректировка strategy v0.2 → v0.3.  
> **Дата:** 2026-08-03.  
> **Источник Hero-talk:** `docs/features/hero-talk/README.md` в stacked PR
> [#291](https://github.com/onedayonemasterpiece/events-bot-new/pull/291),
> ветка `agent/hero-talk-chain-research-20260803@9a1908078261a69b22ecc55e6f11d0e3d8858afc`.  
> **Обновляемые документы:** [каноническая onboarding strategy](README.md) и
> [варианты стратегии](strategy-options.md).

## 1. Итог

Utility-first вывод двух глубоких исследований сохраняется: стандартному
пользователю не нужен обязательный тур, checklist, missions или admission wall.
Однако первоначальная strategy v0.2 слишком широко определяла сам Hero Talk и
ошибочно относила typed briefing и смысловые цепочки только к более
выразительному варианту B.

Каноника Hero-talk устанавливает другую границу:

```text
onboarding strategy
  owns capability truth:
  eligibility, competency, success, dismissal, mastery, suppression

Hero-talk
  owns contextual delivery:
  typed briefing, cursor, tile media, chain graph, page context,
  bounded thread memory, phrase packs and served plan
```

Следовательно, различие вариантов A/B проходит не по наличию настоящего
Hero-talk. Оба варианта используют его каноническую грамматику. Они различаются
интенсивностью программ, длиной и памятью цепочек, персональным/редакционным
контекстом и release stage.

## 2. Исправленные расхождения

| Было в onboarding v0.2 | Каноника Hero-talk | Корректировка v0.3 |
|---|---|---|
| Cold Hero трактовался как отдельное постоянное static service promise | Текущий `HomeHeroTalk` — временное наполнение зоны, целевая миграция ведёт к shared Hero-talk renderer | Сохраняется полезная статическая первая сцена и узнаваемая ориентация, но не отдельный параллельный продукт |
| Typed briefing и narrative dynamics относились к challenger B | Typed briefing, конечный курсор, optional tile media и chain-first — определяющие признаки Hero-talk | Они обязательны для любого production Hero-talk; вариант A использует консервативные single/short chains |
| `hero_talk` и `page_end_talk` были двумя placements одного onboarding registry | Hero-talk — один продукт; placements: `home_hero` и семейство `*_page_end` | Onboarding поставляет `intent=feature_discovery` и `capability_id` в канонический Hero-talk compiler |
| «Не более одного proactive message» могло означать одну фразу/node | Базовая единица Hero-talk — coherent chain | Ограничение означает одну продвигаемую capability и один CTA-path на page journey; chain может иметь несколько связанных nodes |
| Page-end в целом требовал explicit antecedent value | Hero-talk page-end может продолжать exact event/festival/club/page context | Antecedent value обязателен для onboarding/continuity upgrade; editorial/contextual continuation может опираться на завершённый page context |
| Onboarding документ дублировал общий Hero priority и message schema | Hero-talk владеет placement/intent/origin, graph nodes, bridges и runtime served plan | Onboarding сохраняет только capability-specific constraints и evidence; общий arbitration делегирован Hero-talk |
| Action echo и Page-end рассматривались раздельно без chain boundary | Onboarding arc допускает `result echo → where to find result → mastered` | Immediate echo остаётся у action owner; Hero-talk может продолжить подтверждённый результат, но не дублировать и не переобещать его |
| Cross-session memory не была явно отделена от onboarding state | Hero-talk хранит bounded `thread_id`, последние nodes, open loop и meaningful watermark | Thread state становится отдельным owner/domain и не является taste, competency или raw conversation log |
| Golden personas ограничивались только длиной/формальностью | Hero-talk использует finite persona phrase packs после activation | Разрешены human-reviewed finite packs, но onboarding semantics, давление, CTA, права и caps остаются инвариантными |
| Hero typed/motion/sequence scenarios находились в onboarding test registry | Hero-talk имеет отдельную testing strategy и HT-0…HT-10 release track | В onboarding остаются только integration scenarios eligibility/suppression/success; presentation tests переходят Hero-talk track |

## 3. Что не меняется

- первая ценность возникает до login, PWA, Push и profile setup;
- inline recovery и немедленный action echo важнее feature promotion;
- exposure не доказывает competence и не становится taste signal;
- одна capability осваивается за раз;
- `Закрыть`, `Не сейчас`, permanent dismissal и permission denial различаются;
- Search, identity, reminder и personalization нельзя продвигать до их release
  dependencies;
- артефакты не входят в onboarding MVP и не являются prerequisite;
- focus-group incentives не переносятся обычному пользователю;
- основной результат — полезное решение по событию, а не Hero-talk CTR.

## 4. Нормативная интеграция

Onboarding capability engine может выдать только кандидат:

```yaml
intent: feature_discovery
origin: system
capability_id: event-share
capability_version: v1
eligibility_receipt: ...
success_contract: ...
suppression_contract: ...
```

Hero-talk compiler затем:

1. объединяет этот кандидат с greeting/current context/editorial/campaign
   кандидатами;
2. применяет canonical page/entity/thread context;
3. строит coherent finite chain и один основной CTA-path;
4. компилирует immutable phrase pack и static served plan;
5. runtime выбирает только готовый plan без LLM-вызова;
6. продуктовая операция возвращает success evidence владельцу capability state.

Immediate error, confirmation и Undo остаются локальными action surfaces.
Hero-talk получает их как подтверждённый context fact и не создаёт второе
противоречащее сообщение.

## 5. Release dependency

Onboarding action echoes и local recovery могут развиваться независимо от
Hero-talk. Доставка feature discovery через Hero-talk разрешается только после
соответствующих этапов канонического release track:

```text
HT-1 static single-scene baseline
→ HT-2 deterministic handwritten chains
→ HT-4 contextual page-end
→ HT-5 onboarding integration
```

Персональные/return chains требуют HT-6, редакционные campaign arcs — HT-7,
полноценное causal сравнение — HT-10. Наличие документации или красивого lab не
является production acceptance.

## 6. Disposition вариантов

- **A — выбранный baseline:** канонический Hero-talk с полезной статической
  первой сценой, консервативными single/short handwritten chains и без
  cross-session personal/editorial feature-selling.
- **B — challenger:** та же грамматика, но более богатые bounded return,
  personalized, editorial и campaign arcs после HT-6/HT-7/HT-10.
- **C — cultural extension:** отдельный artifact track; не определяет Hero-talk
  и onboarding MVP.
