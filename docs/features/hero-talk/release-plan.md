# Hero-talk release track

> **Статус:** нормативный companion к
> [`docs/features/static-site-pages/release-plan.md`](../static-site-pages/release-plan.md).  
> **Дата:** 2026-08-03.  
> **Решение:** текущий статический `HomeHeroTalk` можно сохранять как fallback,
> но production chain engine, page-end placement, generation-time LLM pipeline,
> cross-device thread memory и video mosaic пока `NO-GO`.

## 1. Почему это отдельный release track

Hero-talk влияет одновременно на:

- главный первый экран;
- discovery/navigation;
- персонализацию;
- онбоардинг;
- промо-кампании;
- page-end всех основных page families;
- motion/accessibility;
- статическую сборку и manifests;
- LLM budget/quality;
- cross-device state;
- release telemetry.

Поэтому его нельзя выпускать как локальную замену текста в
`HomeHeroTalk.astro` без отдельного продукта, data contract, compiler и browser
acceptance.

## 2. Текущий baseline

### Recovery candidate after owner audit (2026-08-28)

Draft PR `#596` replaces the Home production consumer with `HeroTalk@2` in
`photo-mosaic` mode and keeps the previous static event-feature component as a
deprecated catalog-only v1. This supplies the bounded HT-1-style static first
scene and a source-bound image-mosaic renderer for owner review. It does not
promote the root, authorize generation-time LLM, claim contextual chains, or
close HT-2…HT-10.

### Уже есть

- `HomeHeroTalk` на главной;
- быстрые ссылки и полезная cold-start лента ниже;
- historical typed-briefing lab;
- semantic-fragment motion research;
- cursor/mosaic prototypes;
- phrase/scenario examples;
- personalization target architecture;
- onboarding strategy draft;
- promo campaign activity model;
- central static-site QA control plane.

### Нет

- канонических Hero-talk schemas;
- chain compiler;
- production phrase packs;
- generation-time Writer/Verifier/Critics;
- page-end renderer;
- contextual event/page packets;
- cross-device thread/watermark state;
- `hero_talk` promo activity;
- qualified exposure ledger;
- Hero-talk GitHub Actions scenarios;
- release-ready image/video mosaic producer;
- доказанной пользовательской ценности coherent chains.

## 3. Release authority

Hero-talk не меняет production root до отдельного owner sign-off.

Любой кандидат обязан фиксировать:

```text
repo SHA
static build ID
catalog revision/hash
Hero-talk schema version
program registry hash
phrase-pack hash
compiler version
profile/golden-fixture revision
campaign revision
thread-state schema version
browser evidence
```

LLM response или красивый lab screenshot не являются release evidence.

## 4. Этапы

### HT-0 — Canonical documentation and research

Должны существовать и быть взаимно согласованы:

```text
README.md
deep-research-prompt.md
release-plan.md
testing.md
static-site onboarding strategy
personalization target
promo campaign contract
static-site release plan
```

GO:

- placement/intent/origin разделены;
- greetings, onboarding и chain-first model не потеряны;
- current `main` baseline описан честно;
- historical lab не назван production;
- open decisions перечислены;
- deep research выполнен и findings reconciled.

### HT-1 — Static single-scene baseline

Реализовать новый renderer поверх текущей главной, но только с заранее
написанными безопасными single-scene packs.

Обязательные families:

```text
greeting/daypart
local identity / «кеска»
today/tomorrow/weekend
service orientation
safe editorial discovery
fallback
```

GO:

- first useful text в HTML;
- no-JS/reduced-motion parity;
- HomeQuickNav и начало ленты остаются сразу доступны;
- links stable/first-tap;
- no provider call;
- existing static fallback retained for rollback.

### HT-2 — Handwritten chain compiler

Добавить finite narrative graph и deterministic compiler без LLM.

GO:

- max 3 nodes;
- graph has no dangling/unreachable nodes;
- bridge/open-loop schemas валидны;
- chain topic continuity проверена на owner-reviewed fixtures;
- terminal cursor semantics correct;
- no random independent phrase rotation under chain mode;
- identical cold/warm compile stable.

### HT-3 — Generation-time LLM pipeline

Добавить protected Writer, fact verifier, chain critic, global-style critic и
pack diversity gate.

GO:

- no runtime LLM;
- every fact/link locked;
- exact model/prompt/schema/style fingerprints;
- provider limiter shared with project gateway;
- no parallel budget bypass;
- invalid output fails to deterministic/last-good pack;
- warm build performs `0` provider sends;
- owner review of Golden-persona phrase-pack sample.

### HT-4 — Page-end contextual placement

Поддержать минимум:

```text
event page
collection
date listing
search results/empty
For Me
clubs
```

GO:

- exact page/entity context;
- event page packet includes lifecycle, logistics, relations and action state;
- page-end does not duplicate Similar/More cards;
- placement is before focus NPS and footer;
- one dominant CTA;
- no autoplay after long content unless separately accepted;
- static/no-JS page remains useful if Hero-talk is absent.

### HT-5 — Onboarding integration

Hero-talk consumes onboarding capability registry/state, but does not redefine it.

GO:

- capability eligibility and success evidence come from onboarding contract;
- one capability at a time;
- result echo and where-to-find-result implemented;
- mastered capability suppresses basic hint;
- dismiss/cooldown works;
- artifact first-hint scenario accessible;
- onboarding exposure does not modify taste profile.

### HT-6 — Personalization and return delta

GO:

- generic before activation;
- Golden-persona/explicit-interest packs after activation;
- no public persona labels;
- exact hide always wins;
- campaign/artifact noise separated;
- `Пока вас не было` count and destination share one `served_delta_id`;
- local anonymous and cross-device authenticated semantics tested;
- profile/transport failure leaves generic first scene.

### HT-7 — Own editorial campaign activity

Add:

```text
promo_activity.surface = hero_talk
```

Initial scope only `system + own editorial`.

GO:

- dynamic festival/program target;
- new eligible event enters next compile;
- expired/cancelled/sold-out policy enforced;
- one campaign does not take whole chain;
- rotation/caps/qualified exposure ledger;
- age rating attached to exact event;
- no partner/paid publication in this stage.

### HT-8 — Image mosaic production candidate

GO:

- exact media provenance;
- role/crop/quality admission;
- atomic copy/CTA/media switch;
- no orphan/stale media;
- terminal media persists;
- mobile/no-data/reduced-motion fallback;
- byte/performance budgets;
- source disappearance fail-closed.

### HT-9 — Video mosaic lab

No production promotion in this stage.

Required:

- read-only search of Telegram/VK source media for current future events;
- one owner-approved horizontal source video;
- exact source ref/hash/provenance;
- noindex lab arms: text, image mosaic, manual video, bounded desktop autoplay;
- poster fallback and zero audio;
- browser/Android/iOS/performance evidence;
- source 404/decode failure/hidden-tab drills.

### HT-10 — Controlled experiment and canary

Variants must separate:

```text
no Hero-talk / existing baseline
single useful static scene
independent scenes control
coherent chain
coherent personalized chain
image mosaic
video mosaic lab only unless separately promoted
```

Primary outcome is downstream useful discovery, not Hero-talk CTR.

GO:

- preregistered hypothesis, eligibility, primary metric, guardrails and stop rule;
- no critical a11y/performance regression;
- no increase in stale/unsupported claims;
- no feed/category displacement beyond accepted bound;
- no material hide/dismiss/fatigue harm;
- Day 7/14 novelty decay reviewed;
- exact rollback to current static Hero-talk.

## 5. Static-site release-plan dependency

Hero-talk does not independently authorize static-site root promotion.

Before root release:

1. current static-site production/candidate/atomic-root gates remain authoritative;
2. Hero-talk track must reach at least accepted HT-1 for generic homepage
   fallback;
3. any dynamic chain feature included in RC must pass its own completed stage;
4. page-end may be released page-family by page-family;
5. Hero-talk failures may not replace useful page content with empty state;
6. last-good Hero-talk manifest and phrase packs are retained;
7. feature flags allow independent rollback of:
   - dynamic chain selection;
   - personalization overlay;
   - page-end placement;
   - campaign source;
   - image mosaic;
   - video experiment.

## 6. Suggested feature flags

```text
PUBLIC_HERO_TALK_ENABLED
HERO_TALK_CHAIN_ENABLED
HERO_TALK_PERSONALIZATION_ENABLED
HERO_TALK_PAGE_END_ENABLED
HERO_TALK_CAMPAIGN_ENABLED
HERO_TALK_IMAGE_MOSAIC_ENABLED
HERO_TALK_VIDEO_MOSAIC_LAB_ENABLED
```

Flags do not bypass schema/fact/expiry/lifecycle gates.

## 7. NO-GO conditions

- Hero-talk is treated as arbitrary hero marketing copy;
- greetings or onboarding disappear from the scenario model;
- chain mode rotates unrelated messages;
- event/page context is not exact;
- runtime LLM call is introduced;
- current counts/dates/weather lack provenance/safe-until;
- persona label is shown to the user;
- clickbait has no guaranteed payoff;
- page-end duplicates existing recommendation feed;
- Hero-talk blocks core page without JS/backend;
- cursor promises continuation that does not exist;
- image/video mismatches copy;
- generation failures erase last-good pack;
- identical warm rebuild spends provider calls;
- campaign interaction trains organic profile without explicit signal;
- return count and destination disagree;
- video loads/plays on unsupported or reduced-motion arm;
- Hero-talk experiment is declared successful only by its own CTR.

## 8. Rollback

Rollback order:

1. disable video and image media overlays;
2. disable campaign source;
3. disable personalization/thread continuation;
4. disable page-end placements;
5. disable chain mode and retain accepted single static scene;
6. restore current `HomeHeroTalk` skeleton if renderer itself is faulty.

Rollback must not delete phrase-pack, exposure or experiment evidence needed for
analysis.
