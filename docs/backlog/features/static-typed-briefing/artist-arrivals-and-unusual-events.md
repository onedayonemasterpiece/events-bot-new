# Приезды артистов и необычные события: product contract

Статус: **design / backlog**, production pipeline не реализован.

Документ определяет две связанные функции для narrative engine:

1. автоматический дайджест артистов, которые физически приедут на будущие
   события в Калининградскую область;
2. безопасное выявление отличительных/необычных событий для hero и будущих
   тематических подборок.

Источник identity seed и locality guardrails:
[artist-visit-registry.md](../../../reference/artist-visit-registry.md). Semantic
contract необычности:
[unusual-event-detection.md](../../../llm/unusual-event-detection.md).

## A. Автоматический дайджест приездов артистов

### Пользовательская ценность

Дайджест отвечает на один вопрос: **кто из подтверждённых неместных артистов,
спикеров или публичных гостей выступит в регионе в ближайшем окне и где открыть
событие**. Он не ранжирует людей по «звёздности» и не заменяет общую афишу.

В первой версии это weekly rolling digest на 14 дней. Горизонт и день выпуска
конфигурируемы в `Europe/Kaliningrad`. Delta-выпуск возможен только для нового
подтверждённого приезда, не чаще одного раза в сутки на channel/surface.

### Входы

- canonical active/future events с revision/hash, датами, venue/municipality,
  links и source bundle;
- LLM-extracted participant + role + evidence spans;
- canonical artist registry identity candidates;
- row-level identity/activity/locality evidence с freshness;
- cancellation/status updates и duplicate/program grouping;
- renderer registry для web hero, digest page и будущих Telegram/VK outputs.

`Monitoring_Priority` исходного XLSX управляет стоимостью enrichment/review,
но не является публичной популярностью и не повышает событие в дайджесте.

### Eligibility: все условия обязательны

1. event активен, находится внутри будущего horizon и физически проходит в
   Калининградской области;
2. источник прямо подтверждает личное/очное участие entity в роли performer,
   speaker, artist, guest, host или другой allowlisted active role;
3. identity match разрешён: unique exact с контекстом или подтверждённый
   verifier/reviewer; duplicate/короткий alias не auto-eligible;
4. `locality.status` равен `non_local_ru_verified` или
   `non_local_international_verified`, evidence row-level и не истёк;
5. event source не является только репостом без даты/места/роли, а entity не
   упомянут как автор произведения, герой фильма, tribute subject, запись или
   дистанционный участник;
6. нет cancellation, unresolved duplicate, venue/date conflict или stale
   source revision.

`local_verified`, `mobile_or_mixed` и `unknown` не попадают в auto digest.
`mobile_or_mixed` можно пропустить через редакторскую очередь, но публичный
текст не должен называть «неместным».

### Processing graph

```text
event source bundle
  -> LLM participant/role extraction
  -> deterministic registry candidate recall
  -> LLM identity disambiguation when needed
  -> on-demand row-level identity/locality enrichment
  -> separate physical-visit verifier
  -> eligibility + freshness gates
  -> tour/appearance dedupe and grouping
  -> digest manifest
  -> surface-specific renderers
```

Это LLM-first pipeline: regex/aliases только находят кандидатов. Semantic role,
identity ambiguity, locality claim и physical visit решаются малыми отдельными
контрактами, а не одним большим writer prompt.

### Grouping, dedupe, ranking

- primary key item: `(registry_id, visit_cluster_id)`;
- `visit_cluster_id` группирует несколько выступлений одной программы/тура в
  ограниченном временном окне и регионе; разные по смыслу события не сливаются;
- одна entity показывается один раз, внутри — все подтверждённые даты/города;
- повторная публикация блокируется по `digest_window + registry_id +
  canonical_event_ids + source_revisions`;
- сортировка: ближайшая дата → выше evidence/freshness → стабильный
  `registry_id`; XLSX priority не используется как editorial rank;
- max items и category diversity задаются surface config; лишнее доступно на
  полной странице, а не исчезает из manifest.

### Manifest v1

```json
{
  "digest_id": "artist-arrivals:2026-W30:v1",
  "timezone": "Europe/Kaliningrad",
  "window_start": "2026-07-20",
  "window_end": "2026-08-02",
  "built_at": "...",
  "registry_schema_version": "kenigevents.artist_visit_registry.v1",
  "items": [
    {
      "registry_id": "RUART-...",
      "display_name": "...",
      "locality_status": "non_local_ru_verified",
      "locality_evidence_ids": ["..."],
      "visit_cluster_id": "...",
      "event_ids": ["..."],
      "dates": ["..."],
      "municipalities": ["..."],
      "participant_role": "performer",
      "participant_evidence_ids": ["..."],
      "source_revisions": ["..."],
      "expires_at": "..."
    }
  ],
  "excluded_counts_by_reason": {}
}
```

Writer не получает весь источник. Он получает approved fields и link tokens,
строит короткий intro/заголовок, после чего validator проверяет dates/names/
links и запрещённые claims.

### Copy contract

Разрешено:

- «В Светлогорске выступит {name}. Концерт — {date}.»;
- «На следующей неделе в регион приедут {count} артистов. Вот даты и события.»;
- указать подтверждённый home city/country, если это помогает понять контекст.

Запрещено без отдельного evidence:

- «впервые», «редкий приезд», «долгожданный», «звезда», «вся область ждёт»;
- nationality вместо профессиональной базы;
- «приезжает», если это screening, tribute, remote stream или произведение;
- «не местный» как публичная бирка: лучше нейтральное «приедет/выступит»;
- делать вывод из отсутствия в local list.

Hero-сценарий `visiting_artist_*` берёт один item manifest и действует с
cooldown `once/person/event/30d`. Weekly digest и hero exposure имеют отдельные
caps: просмотр дайджеста не должен навсегда скрывать полезный date reminder.

### Операции и observability

Хранить на каждый attempt: event/source revision, participant spans, top-k
identity candidates/scores, verifier decisions/versions, locality evidence и
freshness, exclusion reason, cluster members, writer/validator result,
published surface/message URL, corrections/retractions.

Основные метрики:

- participant-role precision;
- identity precision и ambiguous review rate;
- locality precision/unknown rate/evidence staleness;
- visit eligibility precision;
- duplicate/retraction/cancellation latency;
- digest item coverage среди human-confirmed приездов;
- open-to-event и downstream calendar/save/share, без оптимизации только CTR.

### Acceptance gate

- 100% published items имеют active event link, participant evidence и свежую
  non-local locality evidence;
- unsupported `приезжает/впервые/из города` = 0 на eval;
- identity и visit-role precision не ниже 0.98 на стратифицированной выборке;
- locality precision не ниже 0.95; `unknown` лучше false positive;
- cancellation/source change инвалидирует item до следующего surface refresh;
- повторный build с теми же входами идемпотентен;
- shadow digest сравнивается с ручным списком минимум четыре weekly cycles;
- auto publication включается canary/kill-switch, до этого — editorial approve.

## B. Необычные события

### Product use

Один и тот же decision artifact может питать:

- typed briefing сценарий `unusual_event`;
- блок «Необычное на неделе»;
- будущий digest, но только после отдельной проверки частоты/разнообразия.

Hero не показывает абстрактное «нашли необычное». Он показывает один
отличительный факт и ссылку. Narrative может работать на любопытство, но не
скрывает название события бесконечно: teaser-node обязан раскрыть факт или
event в следующем узле bounded chain.

### Selection policy

1. U0–U4 из [LLM contract](../../../llm/unusual-event-detection.md) формируют
   только grounded eligible decisions.
2. `unusual_public` выше `distinctive_fact_only`, но freshness, category/
   municipality diversity и previous exposures применяются до selection.
3. Не более одного unusual candidate в auto chain; cooldown once/event/30d.
4. `distinctive_fact_only` не получает слова «редкий/необычный» — публичным
   объектом интереса становится сам факт.
5. Promo campaign может boost уже eligible candidate, но не создаёт rarity
   decision и не обходит evidence/baseline gates.
6. При insufficient baseline или конфликте выбирается navigation/festival/
   date scenario, а не догадка.

### Не смешивать сигналы

- приезд артиста — отдельная family; известность не делает событие необычным;
- погода может связать approved outdoor event в chain, но не доказывает
  необычность и не предсказывает безопасность;
- comments/popularity выбирают «обсуждаемое», но не `unusual_event`;
- charity, rare format, festival window и natural window имеют собственные
  claims/cooldowns, даже если один event подходит сразу нескольким families.

### Product acceptance

- все public facts entail source spans;
- `unusual_public` имеет достаточный versioned baseline;
- `distinctive_fact_only` никогда не переписывается writer-ом в superlative;
- на golden set присутствуют positive types (short natural window, тематический
  день, unusual access/route, uncommon interaction) и negative controls;
- precision/grounding gates из LLM contract пройдены по category/season slices;
- пользователь может открыть event сразу и продолжить bounded chain публичной
  кнопкой, не используя LAB controls.

## Rollout sequence

1. Импорт canonical artist snapshot; никаких locality claims.
2. Shadow participant matching и on-demand row enrichment.
3. Human-reviewed artist-arrival digest на четыре cycles.
4. Canary automatic digest + retraction/cancellation checks.
5. Параллельно shadow U1–U3 unusualness; editorial review labels.
6. `distinctive_fact_only` canary в hero.
7. Только после baseline/precision gate — `unusual_public` и автоматическая
   подборка.
