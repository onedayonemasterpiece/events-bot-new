# Единые требования к связанным событиям

> Версия контракта: `related-events-surface-v1`.
> Этот checklist обязателен для каждой ветки, которая показывает другие даты,
> похожие события, широкое продолжение или персональную ленту.

## 1. Семантика и идентичность

- **REL-001 — Явный тип связи.** Каждый список кандидатов несёт один из
  `same_occurrence_group`, `semantic_related`, `broad_discovery`,
  `personalized_feed`. Неявный общий массив `related[]` без `relation_kind`
  запрещён.
- **REL-002 — Канонический id.** `event_id` — id Fly SQLite. Slug, Supabase row
  id, source post id и vector document id не заменяют его.
- **REL-003 — Другие даты.** `same_occurrence_group` означает ту же программу,
  а не просто ту же площадку/жанр. Target `linked_event_ids`/group projection
  обязан быть симметричным, без self и с детерминированным порядком дата →
  время → id; текущие legacy data этому ещё не гарантированно соответствуют.
  Exact same `(date,time)` — `duplicate_same_slot` review/merge defect, а не
  другая дата.
- **REL-004 — Не merge.** Relation edge не объединяет canonical rows и не
  участвует самостоятельно в Smart Update dedup/merge. Ошибка рекомендации не
  должна менять факты события.
- **REL-005 — Membership отдельно.** Фестиваль, площадка, организатор, источник,
  рубрика и search results не записываются как `semantic_related` без отдельной
  оценки сходства.
- **REL-006 — Stable group target.** Целевая canonical модель хранит
  `occurrence_group_id`, relation kind, provenance, decision-engine version,
  confidence/review state, timestamps и manual lock. `linked_event_ids` —
  derived compatibility projection, пока migration не завершена.
- **REL-007 — LLM-first ambiguity.** Composite/generic/festival/series identity
  решается LLM-first adjudication/review; deterministic matching используется
  для candidate retrieval и узких invariants, но не для широкой смысловой
  склейки.

## 2. Общая схема результата

Минимальный переносимый envelope:

```json
{
  "schema_version": "related-events-surface-v1",
  "surface": "event_detail_related",
  "relation_kind": "semantic_related",
  "anchor_event_id": 6408,
  "algorithm_id": "event_pgvector_related_chain_v2_two_doc",
  "generated_at": "2026-07-21T00:00:00Z",
  "build_id": "immutable-build-id",
  "items": [
    {
      "event_id": 123,
      "rank": 0,
      "slot_type": "strict_similar",
      "score": 0.87,
      "reason_codes": ["semantic:strong", "same_city"],
      "display": {"href": "/sobytiya/example-123/"}
    }
  ]
}
```

Обязательны `schema_version`, `surface`, `relation_kind`, `event_id`,
`algorithm_id`, `generated_at`, `build_id`. Для `semantic_related` обязателен
аудируемый score/verdict или reason evidence; для `personalized_feed` —
совместимая версия profile/taxonomy и честный fallback algorithm id.

## 3. Lifecycle, freshness и дедупликация

- **REL-010 — Hard exclusions.** Во всех recommendation/feed surfaces нельзя
  показывать текущий event, deleted, duplicate/merged, cancelled, ended/past и
  неактивные rows. Postponed показывается только по отдельному явно принятому
  продуктовым правилу, не как обычный active candidate. Sold-out исключается
  из strict similar; в broad/listing он допустим только как явно disabled/status
  card, а не как обычный доступный вариант.
- **REL-011 — Occurrence exclusion.** Все прямые и обратные siblings текущего
  `same_occurrence_group` исключаются из `semantic_related`, broad tail и
  персонального top-up; они остаются только в `Другие даты/Другое время`.
- **REL-012 — Cross-section dedupe.** Событие, уже показанное на странице или в
  предыдущем section, не повторяется в следующем section. Дедупликация идёт по
  canonical id и occurrence group, не по title string.
- **REL-013 — Time policy.** Event-detail related рекомендует будущие
  старты. Длинные события, уже начавшиеся, не должны тихо попадать туда только
  потому, что `end_date` ещё в будущем. Date-listing может иметь отдельный
  явно документированный policy для «идут сейчас».
- **REL-014 — Refresh.** Изменение ranking/lifecycle фактов в Smart Update
  ставит static export/build. Новый event или новый fingerprint требует
  пересмотра обратных связей активного графа: он может стать лучшим кандидатом
  для старых страниц.
- **REL-015 — Scheduled ageing.** Даже без traffic/telemetry nightly/full
  rebuild удаляет истёкшие candidates. Browser повторно проверяет lifecycle и
  fail-closed сохраняет страницу/CTA, если свежий manifest недоступен.
- **REL-016 — Atomic corpus.** Related cache привязан к ids/fingerprints/policy
  signature одного export corpus. Несовпадение hashes инвалидирует cache;
  partial stale graph не публикуется как healthy.
- **REL-017 — Reconcile triggers.** Occurrence graph пересчитывается и очищает
  stale edges при изменении title/location/date/time/end_date/lifecycle/silent,
  merge/delete и identity-relevant festival/program facts, плюс periodic full
  audit. Early-return не имеет права сохранять старую invalid group.
- **REL-018 — Source graph gate.** Export не должен тихо удалять self/dangling/
  asymmetric/non-transitive/same-slot defects и публиковать «нормальный» graph.
  Он пишет ledger и fail-closed/blocker по принятому threshold.

## 4. Retrieval и качество

- **REL-020 — Offline first.** Retrieval, embeddings и LLM audit выполняются в
  build/batch path. Page view не вызывает LLM, embedding provider или vector DB.
- **REL-021 — Honest algorithm.** TF-IDF/sparse baseline называется
  `lexical/sparse`, не semantic. Raw pgvector proximity без precision gate не
  подписывается пользователю как строго «похожее».
- **REL-022 — Две зоны.** Высокоуверенные candidates идут в
  `semantic_related/strict_similar`; adjacent/diverse tail — в
  `broad_discovery`. Слабый tail не смешивается под заголовком «Похожие».
- **REL-023 — Context dominates.** На event detail сходство с anchor сильнее
  long-term profile. Лайки пользователя могут слегка изменить порядок, но не
  превратить jazz page в theatre подборку.
- **REL-024 — Diversity.** Broad/personal tail ограничивает повтор площадки,
  категории и occurrence family. Diversity не имеет права вытолкнуть все
  действительно похожие cards из strict block.
- **REL-025 — Quality gates.** Golden anchors и hard negatives обязательны;
  проверяются self/sibling/status filters, топология graph, score provenance,
  семантическая связность top-k и отсутствие generic-token false positives.

## 5. Публичные названия и композиция

- **REL-030 — Словарь.** `Другие даты` — только siblings; `Похожие события` —
  strict semantic; `Ещё события` — broad discovery; `Для вас`/`По вашим
  интересам` — только после фактического profile rerank. `Смотрите дальше` и
  `Вам могут быть интересны` считаются legacy-совместимостью и не должны
  появляться в новых ветках как неразличимый umbrella label.
- **REL-031 — Event detail order.** Дата/CTA → компактный occurrence selector →
  описание/практическая информация → конечные `Похожие события` → при наличии
  отдельный finite broad/personal tail.
- **REL-032 — Mobile.** `Другие даты` раскрывается рядом с выбранной датой;
  cards `Похожие события` идут вертикально, target десять initial + следующие
  порции по десять через явное `Показать ещё`, без горизонтального hidden rail
  и бесконечного scroll. Повторный broad block не создаётся, если он дублирует
  тот же набор. Изменение размера порции требует отдельного измерения, но не
  может тихо наследоваться от desktop tail `≤6`.
- **REL-033 — Desktop.** Strict related — desktop-native grid/module 3–4 cards
  в ряд. Broad/personal tail — максимум шесть cards. Sticky hero/CTA обязан
  отпуститься до relation section.
- **REL-034 — Галерея.** Последний gallery slide может быть CTA на первый
  strict candidate. Он визуально и семантически помечен как CTA, не изображение.
- **REL-035 — Empty/error.** Пустой relation section не рендерится. Ошибка
  profile/backend не создаёт skeleton навсегда и не ломает основной контент.

## 6. Поведение occurrence selector

- **REL-040 — Selection ownership.** Выбранная дата/время имеет одно состояние.
  При выборе обновляются date/time, ticket/register/phone CTA, calendar URL,
  share/canonical navigation target и доступность; старые данные anchor не
  остаются рядом с новой датой.
- **REL-041 — Hierarchy.** Same-day times показываются как `Другое время`;
  другие days — как раскрываемые `Другие даты N`. Текущий вариант отмечен
  `выбрано`, прошлые/неактивные siblings скрыты или явно disabled по policy.
- **REL-042 — Direct pages.** Каждая occurrence сохраняет собственный canonical
  URL/row. Selector навигирует или атомарно переключает полный projection; он
  не склеивает несколько дат в один SEO event.
- **REL-043 — Telegraph parity.** Telegraph показывает до шести ближайших
  relevant siblings: active occurrence — crawlable link, cancelled/postponed —
  disabled item с `❌/⏸` без CTA; остаток `и ещё N`. Текущий producer фильтрует
  non-active до renderer, поэтому parity считается target gap, а не shipped.
- **REL-044 — Один projection service.** Telegraph, static detail, listings,
  mobile feed и Telegram/VK publishing получают occurrence projection из одной
  canonical service/export. Frontend exact-title inference и implicit feeding
  special cases либо мигрируются в relation model, либо явно остаются
  отдельными правилами с tests.

## 7. Card, media, accessibility и SEO/GEO

- **REL-050 — Один renderer/API.** Related, broad, search и personal cards
  используют canonical EventCard API/interaction controller. Surface-specific
  read-only/compact variant допустим, но второй handwritten dynamic renderer и
  расхождение identity/href/actions semantics запрещены.
- **REL-051 — Scan order.** После media: title → date/time/status → venue/city →
  admission → utility actions.
- **REL-052 — Actions.** `Не интересно`, like/unlike, share и undo — `<button>`
  с `aria-label` и `data-nosnippet`; title/detail/calendar/ticket сохраняют
  осмысленные links.
- **REL-053 — Media.** Compact cards используют один surface-specific crop
  decision: `visual_only` может быть focal-aware `cover`; OCR/unknown documents
  остаются `contain`. Alt, geometry и protected regions не теряются.
- **REL-054 — Orientation.** Feedback не перемещает acted card и cards выше неё
  в текущем viewport. `Не интересно` даёт объясняющую plate/toast с `Отменить`;
  plate не открывает detail.
- **REL-055 — Keyboard/touch.** Один logical focus owner; arrows переходят по
  card graph, Enter открывает, интерактивные controls не перехватываются.
  Runtime rerank/hydration сохраняет focus и canonical href.
- **REL-056 — Static crawlability.** Strict fallback присутствует в HTML без JS;
  service copy исключена из snippets, но event facts и внутренние ссылки
  индексируемы. Personal continuation не получает отдельный canonical URL и
  не попадает в sitemap.

## 8. Personalization и feedback

- **REL-060 — Consent/version gate.** Profile применяется только после consent
  и при совпадении `profile_version`, `feature_schema_version` и
  `taxonomy_version`; иначе остаётся static order.
- **REL-061 — Strong actions.** `like` — positive, `unlike` только снимает
  positive; negative signal создаёт только `not_interested`. Hide/not-interest
  hard-filters exact event и по принятому policy его occurrence siblings.
- **REL-062 — Fail-safe.** localStorage/Supabase/RPC недоступны — static event
  page/list остаётся полезным. Нельзя заявлять server persistence, если actions
  сохранены только в browser.
- **REL-063 — Listing rules.** SEO list всегда присутствует. `Все / Для меня`
  появляется только при реальной разнице и показывает `Скрыто N`; personal
  tail конечный, кэшированный, не повторяет основной список.
- **REL-064 — Mobile feed naming.** Вкладка `Для меня` допустима только при
  profile-driven ranking. Новый visitor видит честное описание baseline/город,
  а не ложную персонализацию.

## 9. Analytics и privacy

- **REL-070 — Exposure join.** Strong action содержит `surface`,
  `relation_kind`, `anchor_event_id` (если есть), `event_id`, `rank`,
  `algorithm_id`, `build_id`, `served_list_id/hash`.
- **REL-071 — Bounded telemetry.** Храним compact summaries/counts и strong
  ids, не raw scroll/hover firehose и не полный browser history. Hover не
  является strong interest.
- **REL-072 — Honest reason.** Personalized reason/badge показывается только
  при достаточной confidence и объяснимом reason code. Иначе badge отсутствует,
  а не стоит на каждой второй card.

## 10. Проверка перед переносом/релизом

| Gate | Проверка |
| --- | --- |
| Identity | симметрия/no-self `linked_event_ids`; canonical ids; relation kinds не смешаны |
| Lifecycle | current/past/ended/cancelled/duplicate/merged/siblings отсутствуют там, где запрещены |
| Freshness | changed/new event, смена статуса и ход времени инвалидируют/перестраивают graph |
| Quality | golden top-k + hard negatives + graph topology + cache/corpus hashes |
| Event mobile | selector меняет все CTA/calendar данные; 10 initial + порции 10; нет дубля tail |
| Event desktop | 3–4 card grid; finite tail ≤6; sticky boundary; shared card geometry |
| Listings/feed | static SEO fallback; honest `Для меня`; cross-section/group dedupe; cache failure safe |
| Feedback | like/unlike semantics; not-interest + undo; viewport/focus не прыгает |
| A11y/SEO | buttons/links/aria/data-nosnippet; keyboard/touch; JSON-LD/canonical/sitemap не искажены |
| Observability | manifest metadata; surface/relation/rank/algorithm/build exposure; bounded telemetry |

Ветка не готова к применению, пока каждый релевантный gate не отмечен
`PASS`, `N/A` с причиной или отдельным blocker. Скриншот без DOM/data/lifecycle
проверки не закрывает acceptance.
