# Опыт Region Talk и фактическое состояние YDB

Снимок: **2026-07-11 UTC**. Это read-only инженерный аудит текущих row-level метрик и операторских артефактов Region Talk; числа показывают масштаб и найденные проблемы, но не являются стабильным API нового проекта. Существенная реализация находится в экспериментальных Region Talk branches/worktrees, а не в `origin/main`; канонические working docs местами всё ещё описывают docs-first состояние и отключённую публичную публикацию. Поэтому ниже отдельно маркируется фактический live evidence, а не статус README.

## Что найдено в действующей YDB

Region Talk использует одну физическую KV-таблицу `region_talk_state_kv`:

```text
pk           Utf8?  PRIMARY KEY
kind         Utf8?
payload_json Json?
updated_at   Utf8?
```

TTL и secondary indexes в фактическом DDL отсутствуют. Поверх простой таблицы появилось много логических row kinds. Это **не** 15 нормализованных таблиц из раннего design draft.

Read-only live query в `2026-07-11 11:56 UTC` насчитал 70 185 логических rows и 164.48 MiB живого JSON payload. Системный `.sys/partition_stats` показывал приблизительно 151 тыс. rows и 648.04 MiB physical data + 3.71 MiB index; system view может отставать и учитывать физические версии, поэтому эти числа не надо смешивать с exact logical aggregate. Назначение `partition_stats` как instant/operational system view описано в [YDB documentation](https://ydb.tech/docs/en/dev/system-views).

Наблюдавшийся срез:

| Сигнал | Значение | Вывод для новой фичи |
|---|---:|---|
| Канонические source rows | 7 295 в операторском срезе; 7 300 в более позднем live query | Масштаб достаточный для discovery, но это не готовый allowlist локальных авторов. |
| Источники с реально просмотренными постами | 474 | Нельзя заявлять «проанализировали всех» только по размеру очереди. |
| Источники в `pending_scan` | 6 503 | Нужны coverage gate и явный local-source contract. |
| Сохранённые обработанные посты | 12 261 в операторском срезе; 12 309 в более позднем live query | Инкрементальный corpus/state уже практически возможен. |
| Candidate memory | 628 | Схема shortlist/memory доказала полезность, но не topic-of-day clustering. |
| E5 rows / BGE-M3 rows в лимитированном срезе | 2 679 / 3 321 | Обе vector lanes реально работали на корпусе. |
| Точные E5+BGE пары | 2 420 | Dual-model enrichment практически достижим. |
| Actionable dual coverage | 99% | Барьер полноты можно сделать жёстким для фиксированного дневного корпуса. |
| Actionable E5 без BGE к концу цикла | 32 | Параллельный producer/worker даёт rolling lag; дневной корпус надо замораживать до обоих embeddings. |
| Image rows с реальным score | 10 | Media scoring пока слишком мало проверен. |
| Сильные изображения `score >= 0.70` | 1 | Нельзя переносить текущие пороги как доказанные. |
| Publication candidate rows | 35 | Candidate/finalizer contour существует. |
| Gemini-confirmed в последнем цикле | 1 | Final verifier работал, но выборка мала. |
| Доставлено в operator chat накопительно | 8 | Это операторский delivery, не подтверждение автопубликации в новом канале. |
| Comment-link rows | 0 | Комментарии/обсуждение не доказаны как стабильный сигнал. |

В одном из предшествующих снимков также было видно `1 134` duplicate identity rows среди processed posts и repair-delta между source aggregates и row-level state. После свежего цикла публичный счётчик показывал 12 261 unique rows, но сам факт расхождения важен: новое решение не должно дублировать `latest_state`, counters и item rows без необходимости.

В финальном срезе также оставались 22 source rows без корректной sequence/order, 5 112 legacy rows без нового order и 64 legacy duplicate-order rows. Это дополнительный аргумент не переносить растущую систему overlays/очередей в bounded daily pipeline.

### Фактическая стоимость дублирования

| Row kinds | Rows | JSON payload | Наблюдение |
|---|---:|---:|---|
| `source_queue_item` + `source_status_item` + `source_candidate_item` + `online_source_item` | 27 872 | 31.20 MiB | около 3.8 представления на один canonical source |
| `processed_post_item` + `post_live_item` | 23 645 | 27.67 MiB | один compact post payload записывается в два kind |
| `state_snapshot` + `run_state_snapshot` | 2 | 29.03 MiB | два почти одинаковых 15.2 MB snapshots |
| `text_vector_enrichment_item` | 6 554 | 47.98 MiB | наиболее тяжёлый логический kind; часть workers хранит dense vector |

Count-based pruning выполняется лишь для части служебных kinds и зависит от успешного финального snapshot. Durable source/post/vector rows не имеют TTL. Несмотря на заявленные limits, live table содержала 4 734 `business_event` при keep=500 и 291 `queue_cursor` при keep=100. `MAX_*_ROWS` ограничивает сборку snapshot, но не удаляет row-level state.

## Доказанные наработки, которые переиспользуем

### 1. Vector recall лучше keyword-only

В диагностике Region Talk:

- keyword/regex дал 1 115 сырых KO-совпадений, но после узких фильтров осталось 54;
- vector lane нашёл 602 кандидата;
- 580 vector-кандидатов не проходили через узкий regex-filter.

Это не доказывает точность каждого vector-кандидата, но хорошо доказывает, что keywords должны оставаться только дешёвым recall/diagnostic сигналом. Для «Темы дня» E5+BGE ищут соседние посты, а LLM подтверждает, что они действительно об одном событии/явлении.

### 2. E5 и BGE надо считать на одном immutable corpus

Region Talk использовал отдельный BGE worker. Он успешно обрабатывал batches, но основной candidate producer продолжал добавлять новые E5 rows, поэтому к closure оставался хвост. Для суточного дайджеста это устраняется проще:

1. закрыть окно `D-1` и записать `corpus_manifest`;
2. посчитать E5 и BGE ровно для его `document_hashes`;
3. не переходить к clustering, пока coverage обоих моделей не равно 100% eligible documents либо явно не включён degraded/manual mode.

### 3. Нужны row-level факты, а не удобные stale aggregates

Операторская статистика Region Talk отдельно предупреждает, что `latest_state` может отставать. Для новой фичи:

- run counters вычисляются из immutable artifact manifest и итогового candidate pack;
- одна сущность имеет один долговечный row;
- snapshot не дублируется несколькими row kinds;
- changed-only upsert предпочтительнее безусловной перезаписи.

### 4. Status/heartbeat и business counters обязательны

Переиспользуем:

- `kaggle_status.py`;
- `kaggle/kaggle_status_client.py`;
- `video_announce/kaggle_client.py`;
- `kaggle/TelegramMonitor/telegram_monitor.py`;
- `source_parsing/telegram/service.py`;
- `kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py`;
- `kaggle/StaticSiteBuilder/static_site_builder.py`.

Нужны не только `RUNNING`, но и `sources_done/sources_total`, `posts_fetched`, `metric_coverage`, `e5_done`, `bge_done`, `clusters_found`, `llm_candidates_checked`, `media_scored`, `render_done`, `publish_target_done`.

### 5. Per-source baseline полезнее raw popularity

`docs/features/post-metrics/README.md` уже нормализует Telegram/VK popularity по медиане конкретного источника и возрасту поста. Новая фича переносит принцип, а не SQLite-таблицы: raw metrics остаются в дневном artifact, а normalized scores сохраняются только у top evidence.

При этом фактические compact Region Talk post allow-lists **не содержат** `views_count`, `likes_count`, `reposts_count`, `comments_count` или `forwards_count`. Эти поля есть лишь в draft-схеме. Требование новой фичи по engagement нельзя считать уже обеспеченным YDB: метрики надо собирать в собственном дневном observation artifact.

### 6. Media pipeline нужен каскадный

Region Talk сформулировал разумную механику: дешёвые CV gates → aesthetic/technical model → CLIP/SigLIP → VLM только для finalist media. Переиспользуем pipeline shape, pHash dedupe, rights/safety checks и model-report contract. Пороги и итоговая полезность считаются **неотработанными**, потому что реальный scored sample пока мал; текущая реализация не доказывает корректный best-of-album и местами оценивала только первый кадр/item.

### 7. Public publisher и anti-repeat не являются готовыми компонентами

Live evidence относится к operator-chat delivery. Telegram/VK public publishing в Region Talk docs обозначен как future/disabled, semantic history/anti-vector — как неполный. Эти контуры для «Темы дня» проектируются заново с проверенными общими publisher primitives events-bot, а не объявляются переиспользованными.

### 8. Текущая ветка не acceptance-ready

В targeted audit текущего Region Talk branch 238 тестов дали 2 failures и 8 errors; часть errors связана с отсутствующим `openpyxl`/runtime auto-install под PEP 668, failures — с LLM sync-wrapper status. Это не обесценивает live probes, но запрещает маркировать весь pipeline как проверенный production baseline. Зависимости будущих Kaggle images должны быть pinned, а тесты — hermetic.

## Что не переносим как есть

- четыре и более source rows на один источник (`queue/status/online/candidate`);
- отдельную строку на каждый embedding каждого дневного поста;
- бесконечный rolling producer во время парного E5+BGE анализа;
- regex как финальное смысловое решение;
- «сумму просмотров» без нормализации по источнику и возрасту;
- любое значение отсутствующей метрики как `0`;
- `latest_state` как источник истины;
- operator-chat delivery как доказательство production publication;
- существующие image thresholds без локального golden set.

## Готовность исходного source catalog

Единственный подходящий canonical registry grain в текущей YDB — `source_queue_item:<canonical_source_key>`. `source_status_item`/`online_source_item` являются overlays, а `source_candidate_item` — discovery frontier и может быть unresolved/rejected.

Текущая YDB содержит большой source graph, но для нового продукта ещё нужен компактный versioned каталог именно локальных источников:

```json
{
  "source_key": "telegram:example",
  "platform": "telegram",
  "canonical_url": "https://t.me/example",
  "title": "...",
  "ownership_group": "...",
  "scope": "kaliningrad_local",
  "active": true,
  "rights_policy": "link_only|media_reuse_allowed|unknown",
  "updated_at": "..."
}
```

Он должен быть либо read-only projection поверх Region Talk, либо компактным `jsonl.zst` export с версией и checksum. Новый проект не пишет в source registry и не копирует 7 295 source rows к себе.

Region Talk искал внешних авторов и специально выводил локальные источники из своего основного funnel. В live query было только 103 `rejected_local_region_source` (90 Telegram, 13 VK). Это полезный стартовый seed для «Темы дня», но не доказательство полноты региональной базы: требуется ручной allowlist/ownership/coverage review.

## Главный вывод опыта

Region Talk доказал инфраструктурную осуществимость source discovery, dual vectors, final verification, status/lease и operator delivery. Он **не доказал** саму продуктовую гипотезу «одна главная локальная тема за сутки». Поэтому новый проект начинает с shadow reports и размеченного набора, а не с автопубликации; необходимое число тестовых черновиков пока не выбрано владельцем продукта.
