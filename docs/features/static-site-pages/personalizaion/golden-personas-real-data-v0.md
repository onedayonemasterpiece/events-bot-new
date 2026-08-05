# Golden personas v0: проверка персонализации на реальном каталоге

> **Статус:** принятый test-panel contract. Это не описание реальных людей, не
> аудитория для рекламы и не production segmentation.
> **Жёсткое правило:** persona может быть синтетическим тестовым actor, но
> events, dates, prices, locations, lifecycle и source facts берутся только из
> frozen read-only production snapshot.

## 1. Граница реального и управляемого

| Объект | Правило |
|---|---|
| Event/catalog | только canonical read-only snapshot и произведённые manifests |
| Persona | вымышленный actor без имени, контактов и PII |
| Actions | deterministic/seeded действия в isolated E2E namespace |
| Ground truth | независимая разметка real event IDs; ranker score не является label |
| Future replay | только факты/состояния, реально наблюдавшиеся в snapshots |
| Production users | профили и действия не копируются |

Отсутствующий perfect match не создаётся. Результат
`NO_RELEVANT_CATALOG_SUPPLY` отделяется от ranking failure.

## 2. Панель v0

| ID | Контекст | Core job и constraints | Обязательная проверка |
|---|---|---|---|
| `P01_STUDENT_DISCOVERY` | mobile, ограниченный budget | концерты/stand-up/фестивали, вечер, public transport | быстрое value без превращения выдачи только в free |
| `P02_AFTERWORK_STAGE` | after-work | театр/jazz/выставки после 18:30 | релевантное входит в первые 20 inspected при supply |
| `P03_FAMILY_CAREGIVER` | взрослый с ребёнком | age/time/price hard constraints | похожее, но неподходящее не считается успехом |
| `P04_TIMEBOXED_CULTURE` | мало времени | Fri/Sat window, city, calendar conflict | constraints применены до semantic score |
| `P05_CLASSICAL_DEPTH` | узкий культурный поиск | Чайковский/симфония/камерная музыка | core не растворяется; adjacency контролируема |
| `P06_DAYTIME_ACCESS` | дневной сценарий | daylight, address/transport/accessibility | unknown accessibility не становится true |
| `P07_VISITOR_WEEKEND` | 2–3 дня в регионе | date window, duration, meeting point | template без occurrence не является supply |
| `P08_NEGATIVE_BOUNDARIES` | explicit negatives | исключает kids/nightlife/stand-up/mass festival | hidden не возвращается; undo работает |
| `P09_PREFERENCE_DRIFT` | longitudinal pivot | classical → theatre/contemporary art | профиль адаптируется без мгновенного стирания истории |
| `P10_RARE_SUPPLY` | niche interest | organ/early music/historical instruments | zero supply честен; generic substitute не получает pass |
| `P11_BROAD_NOVELTY` | broad explorer | theatre/jazz/exhibitions/lectures | diversity без потери core relevance |
| `P12_HARD_ACCESS_PRICE` | stress constraints | city, Fri/Sat, price cap, accessibility fact | hard mismatch не попадает в guarded top slots |

Возраст здесь — coverage axis интерфейса, а не причина автоматически назначать
интерес. Детский сценарий моделируется взрослым caregiver.

## 3. Persona manifest

Каждый versioned manifest содержит:

```text
persona_id / persona_version
contains_real_personal_data = false
positive / adjacent / negative / unknown facets
hard / soft constraints
unknown-fact policy
maturity policy and session schedule
seed
catalog snapshot hash
label revision
real holdout event IDs + source/content hashes
expected profile facets / exclusions / no-supply outcomes
```

## 4. Snapshot and timeline

Snapshot фиксирует:

- `snapshot_taken_at`, source DB fingerprint, query/version;
- normalized event hashes;
- public fields only;
- no Telegram session, auth/profile, private payload or secret.

Virtual `as_of` допустим только внутри доказанного timeline. Нельзя придумывать
новую дату, отмену, price, accessibility или `first_seen_at`. Для проверки
нового ingest нужен более поздний реальный snapshot.

## 5. Evaluation loop

```text
freeze snapshot + panel + labels + baseline
-> run action-driven sessions
-> locate first broken layer
-> one bounded change
-> paired replay on same evidence/seed
-> browser sentinel on actual Astro build
-> accept/reject log
-> validate on newer untouched snapshot
```

Порядок диагностики:

```text
supply -> eligibility -> candidate recall -> collection -> rollup/maturity
-> profile application -> ranking -> presentation -> outcome
```

Нельзя менять ranker weights, пока failure находится раньше в цепочке.

## 6. Core metrics and gates

```text
cards_to_first_relevant =
  distinct valid inspected cards through the first independently labelled
  relevant event that receives a meaningful action
```

Golden gate для mature eligible scenario: `<=20`; system-level target `<=30`
остаётся отдельным journey SLO. No-supply session не маскирует ranking failure и
не попадает в его denominator.

Дополнительно:

- precision/MRR at 20;
- hard-constraint violation = 0;
- hidden recurrence = 0;
- collection/dedupe correctness;
- profile rollup/application lag;
- diversity and concentration;
- static/degraded fallback;
- bounded DB growth and cleanup.

## 7. Data and storage safety

- trusted server-side `e2e_run_id/test_actor`, не browser flag;
- test actors исключены из ordinary product aggregates;
- Fly canonical event DB read-only;
- no raw scroll/click firehose;
- traces/screenshots live in ignored artifacts/Object Storage;
- cleanup proves exact remaining rows/bytes;
- repeated same-seed run does not duplicate accepted facts;
- 30/90/365-day process replay validates bounded growth, while Playwright
  remains a representative browser sentinel.

## Acceptance

- [ ] Все persona use only real catalog facts.
- [ ] Ground truth independent from ranker.
- [ ] No-supply/unknown facts classified honestly.
- [ ] Every accepted change improves target persona without hard regressions in
  the full panel.
- [ ] Result repeats on a newer untouched snapshot.
- [ ] Unified Statistics Runtime and retention gates pass.
- [ ] Evidence is exact-SHA, reproducible and free of PII/secrets.
