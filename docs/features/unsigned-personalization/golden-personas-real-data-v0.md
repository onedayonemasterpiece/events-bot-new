# Golden personas v0 и протокол тестирования на реальных событиях

> **Статус:** конкретная test-panel specification для следующего implementation slice; не описание реальных людей, не сегментация аудитории и не финальная release-калибровка.
>
> **Жёсткое правило:** все события, факты, даты, цены, города, источники, статусы и lifecycle transitions в evaluation берутся только из зафиксированных реальных production records. Выдуманные event fixtures и «синтетические миры каталога» запрещены.

## 1. Что здесь реально, а что контролируемо

| Объект | Политика |
|---|---|
| Event/catalog data | Только read-only snapshot канонического Fly SQLite `/data/db.sqlite` и произведённые из него static manifests. |
| Golden persona | Контролируемый вымышленный тестовый актор без имени, контактов, аккаунта реального человека и PII. |
| UI-действия | Детерминированные/seeded действия тестового актора по реальным карточкам; допустимы только в изолированном E2E tenant/namespace и маркируются `e2e_run_id`. |
| Ground truth | Независимая разметка реальных event IDs с source/content evidence; score проверяемого ranker не является label. |
| Future replay | Сдвигается evaluator `as_of`, но используются только уже наблюдённые реальные записи и доказуемые даты/статусы. Новые события и отмены не придумываются. |
| Production users | Их профили, действия и идентификаторы не копируются в fixtures и не используются для управления persona. |

Golden persona — это управляемая utility/behavior specification. Она может быть синтетической как актор, но не имеет права создавать синтетическую реальность каталога.

## 2. Панель v0

Возраст — axis покрытия интерфейса и жизненного контекста, а не источник автоматических интересов. Ranker не должен выводить интерес из возраста; интерес подтверждается только действиями. До отдельного privacy/legal решения панель начинается с 18+, а семейный сценарий моделирует взрослого caregiver, не профиль ребёнка.

| ID | Возрастной/жизненный контекст | Core interests | Hard/soft constraints | Наблюдаемое поведение | Продуктовый результат |
|---|---|---|---|---|---|
| `P01_STUDENT_DISCOVERY_18_24` | 18–24, студент, mobile-first | концерты, stand-up, городские фестивали, маркеты | free/low-price; вечер; общественный транспорт | короткие частые сессии, save/share, явный отказ от дорогого | быстро находит доступное событие, но выдача не превращается только в бесплатную |
| `P02_AFTERWORK_STAGE_25_34` | 25–34, работающий, after-work | современный театр, jazz, выставки | будни после 18:30; ограниченное окно; билет в среднем диапазоне | detail для состава/описания, затем ticket/save | релевантное вечернее событие входит в первые 20 inspected cards при наличии supply |
| `P03_FAMILY_CAREGIVER_30_44` | 30–44, взрослый с ребёнком 7–10 | мастер-классы, детский театр, interactive museum, outdoor family | возраст, дневное время, finish до 19:00, family budget | проверяет age/time/price; mismatch даёт hide/not-interested | семантически похожее, но возрастно/временно неподходящее событие не считается успехом |
| `P04_TIMEBOXED_CULTURE_35_49` | 35–49, мало свободного времени | театр, лекции, выставки | конкретные Fri/Sat окна; город; календарный конфликт — hard fail | filter → card → detail → calendar; мало случайных кликов | constraints применяются до semantic score, back сохраняет state |
| `P05_CLASSICAL_DEPTH_50_64` | 50–64, целевой культурный поиск | Чайковский, симфоническая/камерная музыка, филармония | вечер/выходные; ограничение цены | deliberate scan, long detail dwell, ticket/save | узкий core не растворяется; ballet/music lecture остаются безопасной adjacency |
| `P06_DAYTIME_ACCESS_65_PLUS` | 65+, дневное посещение | музеи, классика, экскурсии, краеведение | дневное время, доступность, понятный адрес/транспорт, избегать late-night | медленнее просматривает, чаще читает detail; unknown accessibility не принимается за true | полезная выдача без age-stereotype; accessibility uncertainty честно видна |
| `P07_VISITOR_WEEKEND_ADULT` | взрослый турист, 2–3 дня в регионе | архитектурные экскурсии, local history, museums, regional trip | жёсткий date window, meeting point, duration; город/область | search редко, detail/share для совместного плана | on-demand template без concrete occurrence не становится relevant supply |
| `P08_NEGATIVE_BOUNDARIES_ADULT` | age-neutral adult control | jazz, contemporary theatre, exhibitions | исключает kids, loud nightlife, stand-up, mass festival | минимум три explicit negatives на разных реальных событиях; один undo | отрицания устойчивы между сессиями, hidden event не возвращается |
| `P09_PREFERENCE_DRIFT_ADULT` | age-neutral longitudinal control | дни 1–7: classical/museum; после доказанного pivot: theatre/contemporary art | стабильные city/price/time | новые positive и repeated skip старого core; один противоречивый старый save | профиль адаптируется за несколько rollups, но не стирает историю мгновенно |
| `P10_RARE_SUPPLY_ADULT` | adult niche-interest control | орган, early music, historical instruments | регион, ближайшие реальные даты, budget | принимает choral/music-history adjacency, но не generic pop-in-cathedral | zero supply — отдельный честный outcome; ranker не штрафуется и не получает pass |
| `P11_BROAD_NOVELTY_ADULT` | age-neutral broad explorer | theatre, jazz, exhibitions, excursions, lectures | мягкие ограничения | category switching, мало negatives, выше curiosity | core relevance сохраняется при полезной новизне и venue/category diversity |
| `P12_HARD_ACCESS_PRICE_ADULT` | adult constraint stress | широкая культура | только Калининград; Fri/Sat window; максимум 800 ₽; обязательный accessibility fact | filter-heavy; incomplete price/accessibility вызывает detail check, не eligibility assumption | ни один hard mismatch не попадает в guarded top slots |

### Обязательное покрытие каждого actor manifest

Каждый versioned persona manifest содержит:

- `persona_id`, `persona_version`, `age_cohort`, `life_context` и явное `contains_real_personal_data: false`;
- positive, adjacent, negative и unknown facets из контролируемой taxonomy;
- hard/soft constraints и поведение при неизвестном значении;
- maturity policy, session schedule и deterministic seed;
- ссылку на конкретный real catalog snapshot и label revision;
- реальные holdout event IDs, их source/content hashes и основание, почему evaluator вправе раскрыть их в этот `as_of`;
- ожидаемые profile facets, exclusions, no-supply states и anti-bubble assertions.

Машиночитаемый контракт: [`schemas/product-e2e/golden-persona-v0.schema.json`](schemas/product-e2e/golden-persona-v0.schema.json).

## 3. Real catalog snapshot contract

### Захват

1. Прочитать production schema и сделать read-only export только нужных public event fields.
2. Зафиксировать `snapshot_taken_at`, source DB fingerprint, max event ID, query/version и SHA-256 каждого нормализованного event record.
3. Не переносить bot operations, Telegram sessions, user/profile data, private source payloads и secrets.
4. Сохранить bulky snapshot в ignored `artifacts/`/Object Storage; в git допускаются только schema, manifest и минимальный redacted example без выдуманных событий.
5. Label panel размечает реальные IDs независимо от ranker output.

### Сдвиг даты вперёд

Для virtual day `D` evaluator вычисляет eligibility по реальным полям snapshot при `as_of=D`:

```text
eligible(D) = real_record
              AND published/active according to captured evidence
              AND occurrence/window has not ended at D
              AND not cancelled in evidence available for D
              AND satisfies persona hard constraints
```

Допустимо сдвинуть `as_of` вперёд и увидеть, какие из уже зафиксированных будущих/длительных событий останутся. Недопустимо:

- создавать событие, дату, цену, адрес, отмену или revision, которых не было в source evidence;
- считать запись «новой на day 8» только ради сценария без доказуемого `first_seen_at`/import/build watermark;
- переносить дату реального события для удобства fixture;
- считать ended/cancelled event supply;
- заменять отсутствие нужного события synthetic perfect match.

Если для будущего дня нет доказуемого каталога, outcome — `INSUFFICIENT_REAL_SNAPSHOT` или `NO_RELEVANT_CATALOG_SUPPLY`. Для проверки действительно новых поступлений нужен новый snapshot, снятый позднее. Time-travel не предсказывает будущий ingest.

Машиночитаемый timeline contract: [`schemas/product-e2e/catalog-timeline-v0.schema.json`](schemas/product-e2e/catalog-timeline-v0.schema.json).

### Первый реальный supply baseline — 2026-07-14

Read-only aggregate probe production Fly SQLite (`mode=ro`, `PRAGMA query_only=ON`, `quick_check=ok`) при `as_of=2026-07-14` зафиксировал:

- 51 active canonical event на дату среза;
- 172 уникальных event IDs, пересекающих дни 0–13; 218 — дни 0–29;
- 14-day daily supply: 38–66, среднее 48.1;
- за 30 дней: Калининград 167/218, Светлогорск 22/218, 97 location values;
- event/topic strata включают concerts, exhibitions, theatre, workshops, lectures, excursions и family, но это только supply features, не relevance labels;
- 129/218 записей не имели достаточного price evidence; accessibility не имела надёжного отдельного canonical поля.

Следствие: уже есть реальная база для 14-day replay, но price/accessibility personas должны различать `eligible`, `ineligible` и `unknown_fact`. Нельзя дополнять пробелы догадками. Один текущий snapshot годится для `forward_schedule_projection`; честные new/update/cancel transitions требуют серии реальных daily snapshots. Exact queries/results лежат в ignored `artifacts/codex/personalization-e2e-real-data-audit/`.

## 4. Holdout и защита от переобучения

Holdout — это реальное событие, скрытое от определённой training/label-selection фазы evaluator, а не искусственно созданная запись.

- `sealed_at` и `released_at` относятся к evaluation protocol, не меняют canonical lifecycle.
- Разбиение time-aware: одно и то же событие/revision не попадает одновременно в обучение и blind outcome.
- После повторяющегося tuning holdout помечается `spent_for_tuning` и заменяется новым real-event pack.
- Perfect matches, hard negatives и adjacency выбираются по независимой разметке и source facts.
- Если реальных candidates мало, тест фиксирует supply gap; каталог нельзя «дополнить» invented fixture.

## 5. Итеративный E2E-driven цикл

```text
freeze real snapshot + persona panel + labels + baseline
-> run action-driven sessions
-> locate the first broken layer
-> make one bounded product/system change
-> paired replay on the same evidence and seeds
-> browser sentinel on the actual built Astro surface
-> accept/reject + decision log
-> validate on a newer untouched real snapshot
```

Порядок диагностики: `catalog supply -> eligibility -> candidate recall -> telemetry collection -> rollup/maturity -> profile application -> ranking -> presentation -> outcome`. Изменение ranker weights запрещено, пока failure находится раньше в цепочке.

Change принимается, только если:

1. целевая persona улучшила declared outcome на paired replay;
2. core panel не нарушил hard constraints, negatives, privacy, static fallback и anti-bubble guardrails;
3. улучшение повторилось хотя бы на одном более новом real snapshot, не использованном для tuning;
4. [database sustainability gate](database-sustainability-e2e.md) прошёл;
5. evidence packet содержит versions, hashes, served/candidate lists, profile revisions, DB cleanup proof и accept/reject rationale.

Так golden persona становится не одноразовым demo, а regression contract: каждый failure порождает bounded hypothesis, изменение и повторную проверку, пока actor не достигает продуктового результата без деградации остальных cohorts.

## 6. Что ещё требует внешней критики

Панель v0 конкретна и пригодна для реализации manifests, но второй eligible consultant должен проверить: достаточно ли age/life-context coverage; нет ли стереотипов; нужны ли отдельные mobility/sensory/access cohorts; какие maturity thresholds и behavior probabilities можно калибровать только по consented real logs; как ротировать sealed real-event holdouts; и должен ли rare-supply actor оцениваться через ranker, search/alert или catalog-acquisition outcome.

См. [пакет для внешнего агента](external-consultant-review-pack.md) и [полученную консультацию](reviews/product-e2e/supplementary-research-intake-2026-07-14-ru.md).
