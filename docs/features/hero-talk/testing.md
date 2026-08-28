# Hero-talk testing strategy

> **Статус:** target GitHub Actions and acceptance contract.  
> **Связано:** [product model](README.md), [release plan](release-plan.md),
> [central static-site QA strategy](../../operations/static-site-autotest-strategy.md),
> [scenario registry](../../testing/static-site-autotest-scenarios.v1.yml).

## 1. Принцип

Hero-talk получает отдельный test domain, но не отдельный несовместимый test
framework. Сценарии регистрируются в центральном static-site QA registry и
используют существующие build/browser/Android/iOS/direct-relay contracts.

Тестирование разделяется на четыре плоскости:

```text
content/facts
narrative graph and LLM generation
static/browser behavior
personalization/campaign/cross-device state
```

Красивый screenshot не заменяет facts, chain и lifecycle acceptance. Прошедший
LLM critic не заменяет deterministic validators. Browser smoke не доказывает
полезность текста.

## 2. Evidence contract

Каждый terminal run сохраняет sanitized:

```text
scenario_id
status PASS|WATCH|FAIL|BLOCKED
exact repo SHA
static build ID
catalog revision/hash
Hero-talk schema/compiler versions
program registry hash
phrase-pack hash
manifest hash
profile/campaign/thread fixture revisions
provider sends by model/stage
browser/platform
assertion summary
redaction result
```

Не сохраняются:

- auth token;
- email;
- свободный персональный текст;
- raw profile;
- full private generation prompt with secrets;
- bearer candidate URL в публичном artifact.

## 3. Сценарии

### `hero_talk.contract`

Provider-free PR gate.

Проверяет:

- docs/routes/schema consistency;
- placement/intent/origin enums;
- greetings присутствуют;
- onboarding strategy link разрешается;
- chain model содержит bridge/open-loop/resolution;
- release/testing companions существуют;
- нет legacy отдельного `Onboarding Talk`/`Campaign Talk` как продукта;
- current `HomeHeroTalk` не назван полноценным engine.

### `hero_talk.phrase_pack`

Provider-free после generation artifact.

Проверяет:

- exact JSON Schema;
- allowed scenario family/placement/persona pack;
- all links resolve;
- digits/dates/entities map to locked facts;
- lifecycle/safe-until valid;
- age rating canonical;
- max lines/links;
- short variants;
- clickbait budget/payoff;
- lexical/syntactic duplicate thresholds;
- forbidden claims/phrases;
- no raw URL/HTML;
- no public persona label.

### `hero_talk.chain_graph`

Provider-free.

Проверяет:

- unique node IDs;
- max nodes;
- all edges resolve;
- no cycle unless explicitly bounded/manual;
- no unreachable nodes;
- open loop resolves or has explicit expiry fallback;
- topic anchor consistency;
- one dominant CTA path;
- no contradictory first/return states;
- no unrelated greeting→topic jump;
- no campaign takeover;
- terminal node has no false pending cursor.

### `hero_talk.llm_generation`

Protected manual/scheduled cost-bearing run.

Проверяет:

- shared provider limiter;
- exact input fingerprints;
- bounded Writer sends;
- separate fact verifier;
- separate chain critic;
- separate global-style critic;
- failure/abstention receipts;
- invalid output not accepted;
- last-good retained;
- generated pack immutable;
- no secrets/PII in logs/artifacts.

### `hero_talk.compile_cold`

Provider-free from accepted packs.

Проверяет:

```text
catalog + programs + packs + fixtures
→ deterministic Hero-talk manifest
```

Assertions:

- static first node present;
- exact counts/routes;
- eligible chain selection;
- context packets;
- persona compatibility;
- campaign caps;
- output self-hash.

### `hero_talk.compile_warm`

Identical input replay.

Hard assertions:

```text
provider sends = 0
writes = 0
normalized manifest hash identical
served-plan fixtures identical
```

### `hero_talk.home_browser`

Chromium generated-tree gate.

Viewports:

```text
1440×900
1366×768
390×844
360×800
320×568
```

Assertions:

- first useful scene visible immediately;
- typed fragments complete;
- cursor semantics;
- chain max 3;
- CTA works first click;
- hover/focus/pointerdown complete and pause;
- no CLS;
- links never truncated;
- HomeQuickNav visible/reachable;
- feed begins within accepted viewport budget;
- terminal state stable;
- no console/page errors;
- age badge bound to event;
- missing media → text-only.

### `hero_talk.no_js_reduced_motion`

Provider-free browser gate.

Assertions:

- full readable static scene;
- no blinking cursor promise;
- no autoplay;
- links and navigation work;
- no empty hero;
- no hidden critical facts;
- no media placeholder/overflow.

### `hero_talk.page_end_matrix`

Page families:

```text
event
festival event
club event
collection
today
tomorrow
weekend
search results
search empty
For Me
clubs
```

Assertions:

- exact placement before focus NPS/footer;
- context entity matches page;
- existing Similar/More blocks not duplicated;
- one dominant CTA;
- chain text references only available facts;
- action result echo is truthful;
- no autoplay/fatigue violation after long content;
- no layout overlap with bottom navigation/safe area.

### `hero_talk.onboarding`

Uses frozen capability-state fixtures:

```text
unknown
eligible
exposed
attempted
succeeded
repeated
mastered
dismissed
needs_reintroduction
```

Assertions:

- message only when eligible;
- one capability at a time;
- result echo after actual success;
- target result link works;
- mastered suppresses basic hint;
- dismissal/cooldown respected;
- first artifact hint has exact accessible location;
- exposure/click does not alter taste profile.

### `hero_talk.personalization`

Golden fixtures:

```text
cold generic
high unknown mass
explicit lectures
family + price/distance constraints
mixed music/theatre
long-term interest
campaign/artifact hunter
exact tombstone
sensitive-topic action
```

Assertions:

- scene/program selection changes where expected;
- facts/rights remain unchanged;
- explicit preference language differs from inference;
- no persona labels;
- exact hide always excludes;
- sensitive action does not become facet;
- campaign exposure isolated;
- generic fallback on profile failure.

### `hero_talk.return_delta`

Assertions:

- valid meaningful watermark;
- Hero-talk count and destination share `served_delta_id`;
- destination `For Me / New since visit` contains same eligible cohort;
- exact hide/lifecycle applied before count;
- zero-delta and many-new variants correct;
- stale profile/catalog produces honest explanation;
- backgrounding/BFCache does not create false visit.

### `hero_talk.cross_device`

Uses pre-seeded authorized session unless the run is specifically Auth E2E.

Flow:

```text
device A meaningful action/visit
→ server materialization
→ device B restore
→ Hero-talk continuation
```

Assertions:

- thread/watermark/profile revision restored;
- previous/penultimate node state bounded;
- open loop continues only when valid;
- account switch clears incompatible thread;
- multi-tab coordinator prevents duplicate commits;
- direct disabled → relay path;
- relay disabled → direct path;
- no duplicate side effects.

### `hero_talk.editorial_campaign`

Assertions:

- `surface=hero_talk` activity;
- dynamic festival/program identity;
- newly imported eligible event appears after compile;
- past/cancelled excluded;
- one campaign max one scene/chain unless explicit takeover experiment;
- rotation/caps;
- qualified exposure only after visible/read opportunity;
- campaign click does not train organic profile;
- age rating/disclosure truth.

### `hero_talk.image_mosaic`

Assertions:

- exact source order/hash;
- admission role/crop/quality;
- atomic text/CTA/media transition;
- entry/hold/conditional exit;
- terminal persistence;
- pause cancels pending transition;
- exact asset missing → abstain/text fallback;
- mobile does not download forbidden media arm;
- decoded pixels and crop match expected ranges.

### `hero_talk.video_mosaic_lab`

Protected/manual noindex only.

Platforms:

```text
Chromium desktop
Android Chrome
Mobile Safari/iOS
reduced motion
slow network/data saver
```

Faults:

```text
source 404
decode error
autoplay rejection
tab hidden
orientation change
canvas context failure
```

Assertions:

- one muted playsinline source;
- no audio;
- poster fallback;
- text available before video;
- no broken first click;
- pause on hidden;
- frame/long-task/LCP/INP budgets;
- no video request in excluded arm;
- terminal state retains frame/poster;
- sanitized short recording and screenshot evidence.

## 4. Cadence

| Scenario group | PR | Scheduled | Manual/protected |
|---|---:|---:|---:|
| contracts/schemas/graph | yes | yes | optional |
| phrase-pack validators | affected | yes | optional |
| LLM generation | no | bounded | yes |
| cold/warm compiler | affected | daily | yes |
| Chromium home/page-end | affected | daily | yes |
| Android/iOS core | selected | scheduled | release |
| cross-device/direct-relay | selected | scheduled | release |
| image mosaic | affected | scheduled | review |
| video mosaic | no | no until fixture | yes |
| live canary | no | release window | yes |

## 5. FAIL / WATCH policy

### Hard FAIL

- unsupported fact/entity/date;
- broken CTA;
- stale lifecycle claim;
- exact hide leak;
- dangling chain/open loop without fallback;
- runtime provider call;
- warm provider send/write;
- first-tap failure;
- no-JS empty state;
- reduced-motion autoplay;
- profile/account leakage;
- page-end duplicates recommendations;
- video audio or no fallback;
- critical a11y/performance regression.

### WATCH

- low phrase-pack variety;
- high repetition near threshold;
- sparse optional persona coverage;
- campaign supply too low;
- high generic fallback rate;
- non-critical visual crop concern;
- novelty/fatigue signal pending sample.

WATCH is visible evidence and may become blocking before public promotion.

## 6. Release receipt

A Hero-talk release cannot be declared accepted without:

- exact immutable target;
- all required scenario terminal outcomes;
- accepted phrase/program/manifest hashes;
- current catalog/profile/campaign revisions;
- browser and selected native-platform evidence;
- redaction PASS;
- owner visual/editorial sign-off;
- rollback target and drill.
