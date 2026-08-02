# Recap Калининград: требования, аналитика и MVP

**Дата:** 2026-06-28
**Статус:** draft / product+technical requirements
**Рекомендуемое место в проекте:** `docs/features/recap/requirements.md`
**Контекст:** `events-bot-new`, Smart Update, Telegram/VK monitoring, Post Metrics, LLM Gateway, Kaggle CPU runtimes, будущие Astro static pages, Yandex Object Storage.

---

## 1. Краткое решение

Recap нужно делать как отдельный автоматический post-event pipeline и отдельную контентную поверхность, а не как ручное редакционное дополнение к афише.

Базовая продуктовая формула:

```text
Recap = факты + эмоция + значимость + доказательство повторяемости / похожести + CTA к будущим анонсам
```

Главный посыл каждого recap:

```text
Событие уже прошло, но такой формат может повториться.
Следите за анонсами, чтобы не пропустить следующий раз.
```

Ключевые решения:

1. **Делать отдельный recap-канал** с несколькими постами в день.
2. **Не делать ручную премодерацию.** Допустимы небольшие ошибки, если есть постфактум-диагностика, автоисправления, suppress/rebuild и накопление правил.
3. **Переиспользовать Smart Update fact-first pipeline**, а не изобретать новый механизм разбора текста в факты.
4. **Добавить recap-специфичный слой emotion extraction**: не только что произошло, но и как это воспринималось, какой тон, энергия, реакция, настроение.
5. **Добавить event significance scoring**: общий балл значимости + баллы по сегментам аудитории.
6. **Сильно учитывать метрики отчётных постов относительно медианы источника**: views/likes/reposts/comments выше обычного уровня источника — один из ключевых сигналов.
7. **Discovery для recap — это поиск источников и post-event evidence**, а не поиск мест, куда публиковать recap. Это отдельная задача от Subscriber Acquisition.
8. **Обработку делать отдельной очередью**, которую можно выполнять в Kaggle CPU notebook: deterministic stages, LLM API calls, сборка Astro pages, загрузка в Yandex Object Storage.
9. **Gemma 4 26B A4B использовать как ёмкость для структурного анализа recap**, потому что этот резерв ключей сейчас недоиспользован; Gemini Lite оставить на финальный writer, если он лучше/дешевле/стабильнее для текста.
10. **Пересмотреть хранение событий:** нельзя терять связь `event ↔ recap` после удаления/архивации события. Нужно хранить минимальный event snapshot или archive anchor.

---

## 2. Продуктовая гипотеза

### 2.1. Основная гипотеза

Recap-канал повысит удержание и подписки, если будет регулярно показывать, что городские события действительно происходят и имеют живой отклик.

Пользовательская логика:

```text
Я видел анонсы → событие прошло → вижу фото/итоги/реакции → понимаю, что это было стояще → хочу следить за следующими такими событиями.
```

Recap должен создавать не сожаление “я пропустил”, а управляемый FOMO:

```text
В городе есть интересные события. Я могу их пропустить, если не буду следить за афишей.
```

### 2.2. Почему recap отличается от обычной афиши

Обычный анонс продаёт будущее событие. Recap продаёт доверие к будущим событиям через доказательство прошлого.

Recap даёт:

- social proof для будущих анонсов;
- материал для промо организаторов;
- основу для повторяемых event series;
- архив культурной жизни города;
- источник данных для понимания, какие события действительно вызывают отклик;
- дополнительный daily habit: пользователь может читать не только “куда пойти”, но и “как прошло”.

---

## 3. Цели и нецели

### 3.1. Цели

1. Автоматически находить post-event материалы: отчёты, фото, видео, реакции, комментарии, репосты, UGC.
2. Связывать найденные материалы с уже существующими событиями.
3. Если исходное событие отсутствует, фиксировать missed-event signal и использовать его для discovery новых источников.
4. Извлекать факты по уже успешной Smart Update fact-first схеме.
5. Дополнительно извлекать эмоциональный тон, реакцию аудитории и “как это выглядело”.
6. Считать recap_score и event_significance_score.
7. Автоматически выбирать формат выхода: evidence only, micro recap, standard recap, media recap, long/static page, digest.
8. Публиковать несколько recap в день в отдельный recap-канал.
9. Публиковать лучшие recap на сайт или Telegraph в зависимости от качества и SEO-ценности.
10. Сохранять явную связь `event ↔ recap ↔ series ↔ sources`.
11. Использовать recap как будущий social proof: в новых анонсах, подборках, сериях, промо-кампаниях.

### 3.2. Нецели MVP

1. Не строить ручную редакционную очередь.
2. Не отвечать автоматически в чужих чатах и комментариях как часть recap discovery. Это задача Subscriber Acquisition, не MVP recap.
3. Не пытаться делать полноценную SEO-статью для каждого recap.
4. Не публиковать recap в основной афишный канал без жёсткого отбора.
5. Не хранить бесконечно весь сырой мусор из соцсетей без политики retention.
6. Не переносить локальный inference Gemma 26B на Kaggle CPU как основной режим: Kaggle CPU должен быть orchestrator/runtime, а Gemma 26B — API/LLM Gateway stage.

---

## 4. Связь с существующими компонентами проекта

### 4.1. Smart Update как основа fact extraction

В проекте уже есть успешный Smart Update fact-first pipeline. Его нужно переиспользовать для recap.

Текущий инвариант Smart Update:

```text
sources → facts → text
```

Публичный текст строится из извлечённых фактов, а не из сырого текста источников. Для recap нужно сохранить этот принцип.

Что переиспользовать:

- извлечение атомарных фактов из source text / OCR / постов;
- fact buckets: `facts_text_clean`, `facts_infoblock`, `facts_drop`;
- deterministic guardrails против логистики, CTA, ссылок, хэштегов, мусора;
- coverage-check: текст не должен добавлять утверждения, которых нет в фактах;
- structured JSON output для Gemma 4 stages;
- staged writer pipeline: extract → bucket → write → check → revise.

Что добавить:

- `facts_emotion` — факты/сигналы о настроении и восприятии;
- `facts_outcome` — итоги события: что получилось, сколько участников, чем завершилось, какие результаты;
- `facts_audience_reaction` — аплодисменты, очереди, заполненность, отзывы, благодарности, обсуждения;
- `facts_media` — что подтверждается фото/видео: зал заполнен, дети участвуют, мастер-класс идёт, сцена, экспозиция, толпа, атмосфера;
- `facts_repeatability` — признаки повторяемости: серия, фестиваль, регулярный формат, ежегодность, будущие даты;
- `facts_promo_context` — почему recap полезен для будущей промо-связки.

Важно: emotion extraction не должен превращаться в свободную фантазию модели. Эмоциональный тон должен быть source-grounded: модель обязана вернуть evidence phrases или media evidence, на основании которых она сделала вывод.

---

### 4.2. Telegram Monitoring

Сейчас Telegram Monitoring уже отличает отчёты о прошедших событиях от анонсов. Такие посты не должны создавать event card и режутся как `skipped_non_event:completed_event_report`.

Для recap это не “мусор”, а основной входной поток.

Новая маршрутизация:

```text
completed_event_report
→ recap_candidate_post
→ event matching
→ fact/emotion extraction
→ scoring
→ publication/page/archive
```

Требование: существующий event extraction не ломать. Recap должен идти отдельной веткой, чтобы посты-отчёты не создавали события задним числом.

---

### 4.3. VK Auto Queue

VK Auto Queue уже работает как авторазбор VK-постов через Smart Update. Для recap нужно добавить параллельную recap-очередь:

```text
vk_inbox post
→ event draft extraction, если это анонс
→ recap candidate extraction, если это отчёт/итог/фотоотчёт
```

Важно: recap extraction не должен требовать наличия даты будущего события. Напротив, сильный сигнал recap — прошедшее время: “прошло”, “состоялось”, “подвели итоги”, “делимся фото”, “благодарим участников”.

---

### 4.4. Post Metrics

Post Metrics нужно использовать как один из сильнейших сигналов scoring.

Особенно важны не абсолютные просмотры/лайки, а отношение к нормальному уровню источника:

```text
post_views_vs_source_median
post_likes_vs_source_median
post_reposts_vs_source_median
post_comments_vs_source_median
```

Причина: 2 000 просмотров в большом канале могут быть обычным шумом, а 400 просмотров в маленьком канале могут быть сильным сигналом.

---

### 4.5. LLM Gateway

Новые recap stages должны идти через LLM Gateway, а не через прямые SDK-вызовы.

Требования:

- scoped model/key routing;
- fail-fast при rate limits;
- structured JSON output для Gemma 4 stages;
- логирование `requested_model`, `provider_model`, `invoked_model`;
- separate consumer scope для recap, чтобы не съедать ключи критичных event-import потоков;
- возможность явно использовать Gemma 4 26B A4B как основной structured-analysis model для recap;
- возможность fallback на Gemini Lite / другую модель только для высокоприоритетных задач.

Рекомендуемый model split:

| Stage | Основная модель | Комментарий |
|---|---|---|
| post-event classification | Gemma 4 26B A4B | structured JSON |
| event matching adjudication | Gemma 4 26B A4B | только после deterministic shortlist |
| fact extraction | переиспользовать Smart Update / Gemma 4 | желательно общий контракт |
| emotion extraction | Gemma 4 26B A4B | source-grounded JSON |
| significance scoring | Gemma 4 26B A4B | общий + сегменты |
| final recap writing | Gemini Lite или Gemma writer lane | профессионально-журналистский стиль |
| page metadata | Gemini Lite / Gemma light | title, description, tags |

---

### 4.6. Kaggle Status Framework

Recap notebook должен быть обычным Kaggle runtime под status framework.

Обязательные события:

```text
kernel_started
preflight_ok
source_discovery_started
source_discovery_done
candidate_collection_started
candidate_collection_done
matching_started
matching_done
scoring_started
scoring_done
writing_started
writing_done
astro_build_started
astro_build_done
publish_target_started
publish_target_done
report_written
```

Для длинных runs notebook должен отправлять `alive` с прогрессом:

```json
{
  "phase": "scoring",
  "processed_posts": 318,
  "total_posts": 920,
  "accepted_recaps": 14,
  "static_pages_built": 6,
  "llm_calls": 148,
  "llm_failures": 3
}
```

---

### 4.7. Subscriber Acquisition

Subscriber Acquisition и recap discovery не должны смешиваться по цели.

**Subscriber Acquisition discovery** отвечает на вопрос:

```text
Где в соцпространствах уместно порекомендовать человеку будущее событие?
```

**Recap discovery** отвечает на вопрос:

```text
Где появились источники, отчёты, фото, видео и реакции о прошедших событиях?
```

Общие компоненты можно переиспользовать:

- social space registry;
- platform connectors;
- cooldown/dedup infrastructure;
- source trust scoring;
- monitoring schedules;
- anti-spam guardrails, если когда-нибудь появятся внешние ответы.

Но MVP recap discovery **не публикует наружу и не ищет места для публикации**. Он ищет только источники и evidence.

---

## 5. Целевые поверхности

### 5.1. Отдельный recap-канал

Главная поверхность MVP.

Рекомендуемая стартовая частота:

```text
2–5 recap-постов в день
```

Посты должны быть достаточно короткими, чтобы канал читали как живую ленту, а не как архив протоколов.

Типы постов:

| Тип | Когда использовать | Длина |
|---|---|---:|
| `micro_recap` | один хороший источник, небольшой отчёт, есть эмоция или фото | 400–700 знаков |
| `standard_recap` | сильный отчёт, понятная связь с event, нормальные факты | 700–1400 знаков |
| `media_recap` | ключевая ценность — фото/видео | короткий текст + media group |
| `strong_video_recap` | сильное видео, вероятный высокий отклик | видео + короткий lead |
| `multi_source_recap` | несколько независимых источников | пост + страница |
| `promo_small_recap` | событие небольшое, но полезно для промо/сегмента | аккуратный небольшой пост |
| `digest_recap` | несколько мелких событий одной темы/дня | подборка |

Обязательный CTA:

```text
Такие события могут повторяться — следите за новыми анонсами в афише.
```

Формулировка должна быть осторожной. Нельзя обещать повтор, если evidence нет.

Разрешённые варианты:

- “Формат может повториться — следите за новыми датами.”
- “Похожие события появляются в афише, поэтому за анонсами стоит следить.”
- “Если формат вам близок, лучше подписаться на будущие анонсы.”
- “Серия продолжается — следующую дату ищите в афише.” Только если есть явная серия/будущие даты.

---

### 5.2. Основной афишный канал

Основной канал не должен перегружаться recap.

Режим:

```text
0–1 recap в день
или 2–3 recap в неделю
или weekly recap digest
```

Критерии попадания:

- высокий recap_score;
- высокая повторяемость / future intent;
- городская значимость;
- сильное видео/медиа;
- связка с будущим событием;
- промо-ценность для кампании.

---

### 5.3. VK и социальные поверхности проекта

VK важен не только как публикационная поверхность, но и как место, где организаторы могут репостить recap.

Режим:

- стандартные recap можно публиковать в VK-группу проекта;
- media recap можно оформлять как карусель/альбом;
- сильные видео — отдельным видео-постом;
- лучшие recap можно подавать в Promo Campaigns как `vk_repost` / `tg_repost` activity.

Важно: поиск мест для репоста не является recap discovery MVP. Но после создания recap его можно передать в уже существующий промо-слой.

---

### 5.4. Сайт

Сайт должен быть полноценным носителем recap, а не только витриной единичных лучших материалов.

Рекомендуемая структура:

```text
/recap/                         — общий архив, index
/recap/2026/06/                 — месячный архив, index или noindex по качеству
/recap/<slug>/                  — editorial recap page, index
/recap/archive/<slug>/          — thin/noindex page
/recap/series/<series-slug>/    — страница повторяемого формата/серии, index
/event/<slug>/                  — будущая canonical event page
```

Типы страниц:

| Тип | Где | Индексация | Когда создавать |
|---|---|---|---|
| editorial recap | сайт | index | high score, факты, медиа, будущая ценность |
| thin archive recap | сайт | noindex | нужен носитель/ссылки, но мало редакционного текста |
| list-like recap | Telegraph или сайт noindex | noindex/compat | много ссылок, мало самостоятельного текста |
| series recap | сайт | index | событие повторяется или формирует серию |
| weekly digest | сайт | index | подборка нескольких событий недели |

Правило `noindex`:

```text
noindex нужен не потому, что есть ссылки, а потому что страница тонкая, дублирующая, временная или с недостаточной редакционной ценностью.
```

Для UGC/social links можно использовать `rel="ugc nofollow"`. Для официальных источников и организаторов можно применять trust-based link policy.

---

### 5.5. Telegraph

Telegraph сохраняется как compatibility/fallback layer.

Использовать для:

- list-like recap;
- тонких страниц;
- временного MVP;
- случаев, где не нужна индексация;
- fallback, если Astro/Yandex build не завершился.

Не использовать как единственный долгосрочный носитель всех качественных recap.

---

## 6. Discovery для recap

### 6.1. Исправленное определение

Discovery для recap — это **поиск источников post-event evidence**, а не поиск мест, куда опубликовать recap.

Он нужен, потому что источников, которые публикуют результаты событий, часто больше, чем организаторов и исходных анонсных источников.

Примеры таких источников:

- участники;
- площадки;
- партнёры;
- городские и районные паблики;
- образовательные учреждения;
- студенческие каналы;
- родительские сообщества;
- фото/видео сообщества;
- каналы администраций;
- тематические комьюнити;
- СМИ и микро-СМИ;
- комментарии под анонсами;
- репостные цепочки.

### 6.2. Отличие от Subscriber Acquisition

| Измерение | Subscriber Acquisition | Recap Discovery |
|---|---|---|
| Цель | найти момент для рекомендации события | найти источники отчётов и доказательств |
| Объект | человек/обсуждение с intent | post-event source / evidence |
| Действие | рекомендовать будущее событие | добавить источник, собрать пост, создать recap |
| Основной риск | спам | неверная атрибуция / плохие источники / чужие медиа |
| MVP-публикация наружу | возможно позже | нет, discovery ничего не публикует |

### 6.3. Подрежимы discovery

#### `recap_source_discovery`

Ищет новые источники, которые регулярно публикуют отчёты.

Входы:

- текущий список организаторов/площадок;
- источники существующих событий;
- ссылки и упоминания в post-event отчётах;
- репосты и авторы репостов;
- комментарии под event posts;
- VK/TG search по post-event паттернам;
- источники с высоким количеством `completed_event_report`;
- источники, где post-event посты часто выше медианы.

Выход:

```json
{
  "platform": "vk",
  "source_id": "club123456",
  "source_url": "https://vk.com/...",
  "source_type": "venue_or_partner",
  "post_event_frequency_score": 84,
  "media_quality_score": 72,
  "trust_score": 68,
  "event_match_success_rate": 0.76,
  "recommended_action": "auto_add_to_recap_monitoring",
  "evidence_posts": ["..."],
  "reason": "regular photo reports with successful event matches"
}
```

#### `recap_candidate_discovery`

Находит конкретные посты, которые можно превратить в recap.

Сигналы:

- “как прошло”;
- “состоялось”;
- “прошёл/прошла/прошло”;
- “фотоотчёт”;
- “итоги”;
- “делимся кадрами”;
- “благодарим участников”;
- “вчера/на выходных/на прошлой неделе состоялось”;
- альбомы, видео, сторис-репосты;
- посты с сильными метриками относительно медианы источника.

Выход:

```json
{
  "post_type": "post_event_report",
  "recap_candidate": true,
  "source_id": 991,
  "post_url": "...",
  "candidate_event_ids": [4617, 4598],
  "needs_event_match": true,
  "source_discovery_signal": false
}
```

#### `missed_event_discovery`

Находит отчёты о событиях, которых не было в базе.

Это важный сигнал качества мониторинга:

```text
Если recap discovery регулярно находит отчёты об отсутствующих событиях, значит event discovery пропускает источники, форматы или площадки.
```

Выход должен создавать не полноценную event card задним числом, а `missed_event_signal`:

```json
{
  "status": "missed_event_signal",
  "probable_event_title": "...",
  "probable_date": "2026-06-27",
  "source_url": "...",
  "source_type": "post_event_report",
  "recommended_action": "add_source_to_event_monitoring"
}
```

#### `series_source_discovery`

Ищет повторяемые форматы.

Примеры:

- еженедельные мастер-классы;
- ежегодные фестивали;
- циклы лекций;
- регулярные кинопоказы;
- клубные встречи;
- музейные программы;
- детские занятия.

Выход:

```json
{
  "series_candidate": true,
  "series_name": "...",
  "source_ids": [1, 2, 3],
  "matched_event_ids": [100, 188, 244],
  "recap_ids": [12, 19],
  "repeatability_score": 87,
  "next_event_signal": true
}
```

### 6.4. Source discovery scoring

Источник получает балл по отдельной шкале, не равной recap_score.

```text
source_discovery_score =
  post_event_frequency
+ successful_event_match_rate
+ media_quality
+ source_trust
+ engagement_above_median
+ uniqueness_of_coverage
+ future_event_discovery_value
- noise/toxicity/risk
```

Пример порогов:

| Балл | Действие |
|---:|---|
| 0–39 | игнорировать или оставить в raw log |
| 40–59 | occasional scan |
| 60–74 | add to low-frequency recap monitoring |
| 75–89 | add to normal recap monitoring |
| 90+ | high-priority source, scan more often |

Поскольку пользователь явно не хочет ручных процессов, добавление источника может быть автоматическим, но с ограничениями:

- новые источники сначала идут в low-frequency scan;
- публикация из нового источника требует более высокого recap_score;
- trust_score растёт по мере успешных матчей и отсутствия ошибок;
- источники с жалобами/ошибками автоматически понижаются или отключаются.

---

## 7. Общий pipeline recap

```text
1. Source discovery
   ↓
2. Post ingestion from known + discovered sources
   ↓
3. Post-event classification
   ↓
4. Candidate event shortlist
   ↓
5. Event match adjudication
   ↓
6. Smart Update fact extraction reuse
   ↓
7. Emotion / audience reaction extraction
   ↓
8. Media assessment
   ↓
9. Metrics enrichment vs source median
   ↓
10. Event significance + segment scoring
   ↓
11. Recap scoring + route decision
   ↓
12. Final writer
   ↓
13. Publication to recap channel / VK / site / Telegraph
   ↓
14. Attach recap to event / series / future announcements
   ↓
15. Post-fact monitoring and auto-improvement
```

---

## 8. Event matching и связь `event ↔ recap`

### 8.1. Требование

Каждый опубликованный recap должен иметь явную связь:

```text
recap.id → event.id или event_archive_snapshot.id
recap.id → source posts
recap.id → series.id, если применимо
```

Нельзя публиковать recap с уверенной формулировкой о событии, если событие не сматчилось.

Если event отсутствует:

- можно создать `missed_event_signal`;
- можно создать `unmatched_recap_digest`, но без утверждения о конкретном событии;
- нельзя делать сильный CTA к “следующему такому событию”, если серия/организатор не определены.

### 8.2. Candidate shortlist

До LLM нужно сделать deterministic shortlist:

- дата события ± 1–7 дней от даты отчёта;
- совпадение площадки/адреса/алиаса;
- совпадение источника/организатора;
- совпадение названия события;
- совпадение фестиваля/серии;
- наличие ссылок на тот же event_source;
- media/poster hash, если доступно;
- близость тематики и named entities.

LLM должен выбирать только из shortlist и иметь право вернуть `no_match`.

### 8.3. Event retention policy

Проблема: если события удаляются, recap теряет якорь.

Решение: ввести минимальную архивную сущность, которая живёт дольше активной event card.

Варианты:

#### Вариант A — soft delete events

`event` не удаляется физически, а получает статус:

```text
active
expired
archived
hidden
deleted_public
```

Плюс поля:

```text
is_public=false
retention_reason='recap_anchor'
archived_at
```

Плюс удаление тяжёлых/сырых полей по retention policy.

#### Вариант B — отдельный `event_archive_snapshot`

При удалении/архивации события создаётся минимальный snapshot:

```sql
create table event_archive_snapshot (
  id bigserial primary key,
  original_event_id bigint,
  stable_event_uid text not null,
  title text not null,
  date date,
  end_date date,
  time time,
  city text,
  location_name text,
  location_address text,
  organizer text,
  festival text,
  series_id bigint,
  canonical_source_url text,
  public_url text,
  facts_digest text,
  created_at timestamptz default now(),
  archived_at timestamptz default now()
);
```

Рекомендуемый подход: **B как обязательная страховка + A, если архитектура позволяет soft delete.**

### 8.4. Retention уровни

| Уровень | Что хранить | Срок |
|---|---|---|
| active event | всё нужное для афиши | до завершения + рабочее окно |
| expired event | event + sources + публикации | 30–180 дней |
| recap anchored | минимальный snapshot + source links + recap facts | бессрочно или долго |
| raw source text | только если нужно для аудита/пересборки | ограниченно |
| media refs | ссылки/хеши/превью | по storage policy |
| failed/noise posts | компактный лог, без тяжёлых данных | короткий срок |

---

## 9. Fact extraction и emotion extraction

### 9.1. Fact extraction

Не создавать новый extraction pipeline. Использовать Smart Update fact-first contract.

Для recap добавить новый тип source/candidate:

```text
source_type='recap_post_event_report'
```

Факты должны быть атомарными и source-grounded.

Пример:

```json
{
  "fact_type": "outcome",
  "text": "На встрече участники обсудили историю района и посмотрели архивные фотографии.",
  "source_url": "...",
  "confidence": 0.86
}
```

### 9.2. Emotion extraction

Emotion extraction — обязательный компонент, потому что recap должен передавать не только фактаж, но и восприятие события.

Выход:

```json
{
  "emotional_tone": {
    "dominant": "warm_positive",
    "secondary": ["family_friendly", "curious", "community"],
    "intensity": 0.72,
    "confidence": 0.81,
    "evidence": [
      {
        "type": "text",
        "excerpt": "участники благодарили организаторов",
        "source_url": "..."
      },
      {
        "type": "media",
        "description": "на фото видно вовлечённых детей за столами мастер-класса",
        "media_id": "..."
      }
    ],
    "risks": []
  }
}
```

Разрешённые tone labels:

```text
warm_positive
festive
intellectual
family_friendly
youthful
calm
crowded
intimate
community
creative
educational
sporty
touristic
official
mixed
neutral
negative
unclear
```

Важно:

- `positive` нельзя ставить только потому, что пост от организатора.
- “Все были в восторге” нельзя писать, если это не подтверждено текстом/реакциями.
- Негативный/смешанный тон не обязательно блокирует recap, но меняет стиль и может отправить в `archive_only`.

### 9.3. Recap facts schema

Рекомендуемый JSON-контракт:

```json
{
  "post_type": "post_event_report",
  "matched_event": {
    "event_id": 4617,
    "confidence": 0.93,
    "evidence": ["date proximity", "venue match", "title mention"]
  },
  "facts": {
    "outcome": [],
    "program": [],
    "participants": [],
    "organizers": [],
    "audience_reaction": [],
    "media_observations": [],
    "repeatability": []
  },
  "emotional_tone": {},
  "audience_segment_scores": {},
  "event_significance": {},
  "risks": [],
  "recommended_route": "standard_recap"
}
```

---

## 10. Event significance и audience segment scoring

### 10.1. Зачем нужен отдельный significance score

Recap_score отвечает: “стоит ли делать recap из этих материалов?”

Event_significance отвечает: “насколько само событие важно/интересно для аудитории?”

Эти оценки связаны, но не равны.

Пример:

- маленький мастер-класс может иметь невысокую городскую значимость, но высокий family score и хороший promo_small_recap potential;
- официальный форум может иметь высокую городскую значимость, но слабую эмоциональность и низкую пригодность для recap-канала;
- камерный концерт может иметь мало источников, но сильное видео и высокий culture_lovers score.

### 10.2. Сегменты

Базовые сегменты:

```text
youth
families
children
seniors
tourists
culture_lovers
creative_community
education_audience
local_history_audience
nightlife_audience
sports_outdoor_audience
civic_audience
general_city_audience
```

Выход LLM:

```json
{
  "event_significance": {
    "overall": 74,
    "city_importance": 68,
    "repeatability_value": 82,
    "future_announcement_value": 79,
    "confidence": 0.77,
    "reason": "regular cultural format with strong media and positive post-event evidence"
  },
  "audience_segment_scores": {
    "youth": 52,
    "families": 84,
    "children": 76,
    "seniors": 49,
    "tourists": 58,
    "culture_lovers": 81,
    "general_city_audience": 70
  }
}
```

### 10.3. Требование к объяснимости

Каждый высокий сегментный балл должен иметь evidence.

Плохо:

```json
"families": 90
```

Хорошо:

```json
{
  "segment": "families",
  "score": 90,
  "evidence": ["детский мастер-класс", "на фото дети с родителями", "организатор пишет о семейной программе"]
}
```

---

## 11. Recap scoring

### 11.1. Главные сигналы

| Сигнал | Вес | Комментарий |
|---|---:|---|
| event_match_confidence | высокий | неправильная связь хуже пропуска |
| source_trust | высокий | официальный/партнёрский источник ценнее случайного |
| metrics_vs_median | высокий | особенно для отчётных постов |
| media_quality | высокий | фото/видео — ключ к recap |
| emotional_tone_strength | высокий | recap должен передавать атмосферу |
| event_significance | средний/высокий | общий интерес аудитории |
| segment_fit | средний/высокий | для целевых каналов/постов |
| repeatability_score | высокий | recap нужен для будущих анонсов |
| future_event_link | очень высокий | если есть следующее событие серии |
| source_discovery_value | средний | помогает расширять мониторинг |
| promo_fit | средний/высокий | особенно для small recap |
| risk_score | отрицательный | ошибки, негатив, токсичность, сомнительные медиа |

### 11.2. Метрики относительно медианы

Нужно считать не абсолютные значения, а отношение к baseline источника.

Пример:

```text
views_ratio = post_views / max(source_median_views, 1)
likes_ratio = post_likes / max(source_median_likes, 1)
comments_ratio = post_comments / max(source_median_comments, 1)
reposts_ratio = post_reposts / max(source_median_reposts, 1)
```

Чтобы всплески не ломали шкалу:

```text
views_score = clamp(0, 100, 50 + 20 * log2(views_ratio))
likes_score = clamp(0, 100, 50 + 20 * log2(likes_ratio))
```

Итоговый metrics score:

```text
metrics_vs_median_score =
  0.45 * views_score
+ 0.30 * likes_score
+ 0.15 * reposts_score
+ 0.10 * comments_score
```

Если источник маленький, нужно добавить minimum sample guard:

```text
if source_baseline_sample_size < N:
  lower confidence of metrics score
```

### 11.3. Базовая формула recap_score

```text
recap_score =
  0.18 * event_match_score
+ 0.13 * source_trust_score
+ 0.16 * metrics_vs_median_score
+ 0.13 * media_quality_score
+ 0.12 * emotional_tone_score
+ 0.10 * event_significance_score
+ 0.08 * repeatability_score
+ 0.05 * future_event_link_score
+ 0.05 * promo_fit_score
- 0.20 * risk_score
```

Это стартовая формула. Весами нужно управлять через config, а не hardcode.

### 11.4. Route thresholds

| Score | Route | Действие |
|---:|---|---|
| < 35 | `ignore_or_raw_log` | не сохранять как recap, только технический лог |
| 35–49 | `evidence_only` | сохранить как evidence к event/source |
| 50–64 | `micro_recap` | публиковать при дневной квоте и низком risk |
| 65–79 | `standard_recap` | публиковать в recap-канал |
| 80–89 | `standard_recap_plus_page` | recap-канал + сайт/Telegraph |
| 90+ | `top_recap` | сайт index + VK + кандидат в основной канал |

Отдельное правило:

```text
Если event_match_confidence < 0.75, запрещён standard/top recap независимо от score.
```

Для `promo_small_recap` можно разрешить score 50–65, если:

- низкий risk;
- хороший segment_fit;
- есть промо-цель;
- есть повторяемость/потенциал источника;
- соблюдена квота small promo recap.

---

## 12. Promo small recap

Иногда нужно делать recap для отдельных незначимых событий в целях промо.

Это не ошибка, если явно отделить два типа ценности:

```text
editorial significance — событие важно само по себе
promo significance — recap полезен для роста сети источников, отношений, сегмента или будущих событий
```

### 12.1. Когда делать promo small recap

- новый источник нужно “подхватить” и дать ему повод репостнуть проект;
- маленькое событие хорошо попадает в сегмент;
- у события сильная эмоция/медиа, хоть оно не городского масштаба;
- есть регулярная серия;
- источник потенциально будет давать будущие анонсы;
- мероприятие нишевое, но аудитория канала может быть лояльной.

### 12.2. Ограничения

- отдельная дневная квота: например 1–2 promo_small_recap в день;
- нельзя выдавать маленькое событие за “главное событие города”;
- стиль более камерный;
- CTA мягкий;
- если источник новый и trust низкий, нужен более строгий event_match.

---

## 13. Writer: стиль и ограничения

### 13.1. Стиль

Финальное написание должно быть:

```text
профессионально-журналистское + немного блогерское
```

То есть:

- уверенный лид;
- конкретные факты;
- живой язык;
- без канцелярита;
- без рекламной гиперболы;
- без “все были в восторге”, если это не подтверждено;
- без пересказа источника копипастой;
- без сухого протокола;
- без токсичного FOMO.

### 13.2. Структура стандартного recap-поста

```text
1. Лид: что прошло и почему это заметно.
2. 1–2 фактических детали.
3. 1 эмоционально-атмосферный вывод, grounded in evidence.
4. Мягкий CTA к будущим анонсам.
5. Ссылка на страницу/источник, если применимо.
```

Пример шаблона:

```text
Вчера в <место/формат> прошёл <нейтральное название события>. По отчётам организаторов, главным акцентом стали <факт 1> и <факт 2>.

По фото и отзывам видно, что формат получился <тон>: <короткое grounded explanation>. Такие события могут повторяться — следите за новыми анонсами в афише.
```

### 13.3. Запреты

- не обещать повтор, если нет evidence;
- не использовать “лучшее”, “главное”, “уникальное”, если это не подтверждено;
- не вставлять неподтверждённые числа;
- не делать выводы по фото, которые нельзя уверенно сделать;
- не использовать чужие отзывы как массовое мнение;
- не создавать event card из отчёта;
- не публиковать sensitive/личные данные участников;
- не нарушать авторство медиа.

---

## 14. Data model

### 14.1. `recap_candidate_post`

```sql
create table recap_candidate_post (
  id bigserial primary key,
  platform text not null,
  source_id text,
  source_url text,
  post_url text unique,
  post_text text,
  posted_at timestamptz,
  fetched_at timestamptz default now(),
  media_refs jsonb default '[]'::jsonb,
  metrics_snapshot jsonb default '{}'::jsonb,
  classifier_result jsonb default '{}'::jsonb,
  status text not null default 'new',
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);
```

Statuses:

```text
new
classified_non_recap
candidate
matched
unmatched_missed_event
scored
routed
published
archived_evidence
suppressed
failed
```

### 14.2. `recap`

```sql
create table recap (
  id bigserial primary key,
  stable_recap_uid text unique not null,
  event_id bigint,
  event_archive_snapshot_id bigint,
  series_id bigint,
  route text not null,
  status text not null default 'draft',
  title text,
  lead text,
  body_md text,
  cta_text text,
  emotional_tone jsonb default '{}'::jsonb,
  audience_segment_scores jsonb default '{}'::jsonb,
  event_significance jsonb default '{}'::jsonb,
  recap_score numeric,
  risk_score numeric,
  source_post_ids bigint[] default '{}',
  page_url text,
  telegraph_url text,
  tg_post_url text,
  vk_post_url text,
  noindex boolean default false,
  created_at timestamptz default now(),
  published_at timestamptz
);
```

### 14.3. `recap_event_link`

```sql
create table recap_event_link (
  recap_id bigint not null,
  event_id bigint,
  event_archive_snapshot_id bigint,
  match_confidence numeric not null,
  match_evidence jsonb default '[]'::jsonb,
  created_at timestamptz default now(),
  primary key (recap_id)
);
```

### 14.4. `recap_source_candidate`

```sql
create table recap_source_candidate (
  id bigserial primary key,
  platform text not null,
  source_key text not null,
  source_url text,
  source_type text,
  discovery_reason text,
  source_discovery_score numeric,
  post_event_frequency_score numeric,
  media_quality_score numeric,
  trust_score numeric,
  successful_event_match_rate numeric,
  status text not null default 'candidate',
  evidence jsonb default '[]'::jsonb,
  last_seen_at timestamptz,
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  unique(platform, source_key)
);
```

Statuses:

```text
candidate
low_frequency_monitoring
active_monitoring
high_priority_monitoring
disabled
blocked
```

### 14.5. `missed_event_signal`

```sql
create table missed_event_signal (
  id bigserial primary key,
  source_post_id bigint,
  probable_title text,
  probable_date date,
  probable_location text,
  source_url text,
  confidence numeric,
  reason text,
  recommended_source_action text,
  status text default 'new',
  created_at timestamptz default now()
);
```

---

## 15. Kaggle CPU notebook architecture

### 15.1. Роль Kaggle CPU

Kaggle CPU notebook должен быть orchestration/runtime для recap batch.

Он может делать:

- source discovery;
- сбор постов из разрешённых источников;
- deterministic filtering;
- metrics enrichment;
- event shortlist;
- LLM API calls через LLM Gateway/Google keys;
- structured JSON validation;
- route decisions;
- финальную сборку output artifacts;
- Astro static pages build;
- upload в Yandex Object Storage;
- публикационные side effects, если runtime secrets и права настроены.

Он не должен делать основную работу как локальный CPU inference Gemma 26B. Это будет медленно и нестабильно. Gemma 26B нужно использовать как remote provider model через LLM Gateway / Google AI keys.

### 15.2. Зачем отдельный service user в Yandex Cloud

Для сборки и публикации static pages из Kaggle нужен отдельный сервисный пользователь/ключ с минимальными правами:

```text
service account: recap-static-builder
scope: конкретный bucket/prefix
permissions: upload/update objects, maybe list prefix
no broad admin if можно ограничить
```

Runtime secrets:

```text
YC_RECAP_STORAGE_ACCESS_KEY_ID
YC_RECAP_STORAGE_SECRET_ACCESS_KEY
YC_RECAP_STORAGE_BUCKET
YC_RECAP_STORAGE_PREFIX=recap/
YC_RECAP_PUBLIC_BASE_URL=https://...
```

### 15.3. Astro build в notebook

Рекомендуемая схема:

```text
1. Kaggle получает recap records и page payloads.
2. Генерирует markdown/content collection entries.
3. Запускает npm install / npm ci, если зависимости не закэшированы.
4. Выполняет npm run build.
5. Загружает dist/ в Object Storage.
6. Пишет manifest с URL созданных страниц.
7. Сервер обновляет recap.page_url.
```

Выходной manifest:

```json
{
  "run_id": "recap:2026-06-28",
  "pages": [
    {
      "recap_id": 552,
      "route": "/recap/2026/06/.../",
      "url": "https://.../recap/2026/06/.../",
      "noindex": false,
      "status": "uploaded"
    }
  ],
  "errors": []
}
```

### 15.4. Failover

Если Astro build не прошёл:

- recap-канал может всё равно опубликовать пост без page_url;
- для high-score recap можно создать Telegraph fallback;
- job должен быть retriable;
- page build не должен блокировать весь recap pipeline.

---

## 16. Publication routing

### 16.1. Decision object

Каждый recap после scoring получает route decision:

```json
{
  "route": "standard_recap_plus_page",
  "publish_to_recap_channel": true,
  "publish_to_main_channel": false,
  "publish_to_vk": true,
  "create_static_page": true,
  "static_page_noindex": false,
  "create_telegraph": false,
  "attach_to_future_events": true,
  "reason": "high match confidence, strong media, metrics above median, repeatable format"
}
```

### 16.2. Quotas

Стартовые квоты:

```text
recap_channel_total: 2–5/day
promo_small_recap: 1–2/day
main_channel_recap: 0–1/day
vk_recap: 1–5/day
static_index_pages: no hard cap, but require quality threshold
thin_noindex_pages: cap by storage/noise policy
```

### 16.3. Dedup

Нельзя публиковать несколько recap об одном и том же событии без причины.

Правило:

```text
one event → one primary recap
```

Исключения:

- фестиваль/многодневное событие;
- strong video позже отдельным постом;
- weekly digest с ссылкой на уже опубликованный recap;
- обновление страницы новыми источниками без повторной публикации в канал.

---

## 17. MVP

### Phase 0 — schema + retention foundation

Цель: создать основу, чтобы recap не терял связь с событиями.

Сделать:

- `recap_candidate_post`;
- `recap`;
- `recap_event_link`;
- `recap_source_candidate`;
- `event_archive_snapshot` или soft-delete policy;
- stable uid для event/recap;
- source links retention policy.

Acceptance criteria:

- post-event report можно сохранить без создания event card;
- recap может ссылаться на active event или archived snapshot;
- удаление/архивация event не ломает recap;
- source_url и evidence сохраняются.

### Phase 1 — candidate detection + event matching

Цель: начать собирать recap candidates автоматически.

Сделать:

- перехват `completed_event_report` из Telegram Monitoring;
- аналогичный классификатор для VK;
- deterministic event shortlist;
- LLM match adjudication через Gemma 4 26B A4B;
- `missed_event_signal` для unmatched reports;
- dry-run report без публикации.

Acceptance criteria:

- минимум 70–80% очевидных отчётов попадают в candidates;
- high-confidence event matches сохраняются;
- unmatched отчёты не создают задним числом event cards;
- ошибки матчинга диагностируются по `match_evidence`.

### Phase 2 — facts + emotions + scoring

Цель: получить recap_score и готовый structured brief.

Сделать:

- переиспользование Smart Update fact extraction;
- новый emotion extraction stage;
- media assessment;
- metrics vs median enrichment;
- significance scoring по сегментам;
- route decision.

Acceptance criteria:

- каждый candidate имеет structured JSON;
- emotion tone имеет evidence;
- scoring объясним;
- `promo_small_recap` отделён от editorial recap;
- risk_score может блокировать публикацию.

### Phase 3 — auto publication to recap channel

Цель: начать регулярную автоматическую публикацию без ручной премодерации.

Сделать:

- final writer;
- Telegram recap channel publisher;
- daily quota;
- idempotency;
- suppress/retract mechanism;
- post-publication monitor.

Acceptance criteria:

- 2–5 постов в день публикуются автоматически;
- каждый пост имеет event/series/source link;
- каждый пост содержит future-oriented CTA;
- нет дублей по одному событию;
- publication errors уходят в retry/fail state.

### Phase 4 — Astro static pages + Telegraph fallback

Цель: создать носитель recap вне канала.

Сделать:

- page payload schema;
- Astro content generation;
- Kaggle build;
- upload to Yandex Object Storage;
- `index/noindex` routing;
- Telegraph fallback.

Acceptance criteria:

- high-score recap получает static page;
- thin pages получают noindex;
- build manifest возвращает URL;
- failure page build не блокирует channel publication.

### Phase 5 — source discovery MVP

Цель: находить новые источники отчётов.

Сделать:

- `recap_source_discovery`;
- source discovery score;
- low-frequency scan для новых источников;
- missed-event feedback loop;
- trust escalation/de-escalation.

Acceptance criteria:

- система находит новые источники post-event reports;
- новые источники автоматически попадают в low-frequency monitoring;
- хорошие источники повышаются до active monitoring;
- плохие/шумные источники отключаются автоматически.

### Phase 6 — future event reuse

Цель: использовать recap как social proof в будущих анонсах.

Сделать:

- `series_id` / repeatability detection;
- attachment recap → future event;
- блок “как это проходило раньше” в будущих pages/posts;
- promo campaign integration.

Acceptance criteria:

- будущий event может получить link на прошлый recap;
- повторяемые форматы имеют series page;
- CTR из будущего анонса на прошлый recap измеряется.

---

## 18. Error handling без ручной премодерации

Поскольку ручных процессов быть не должно, нужны автоматические safety loops.

### 18.1. Pre-publication automated guards

- event_match_confidence threshold;
- risk_score threshold;
- source trust threshold;
- duplicate check;
- banned/sensitive content check;
- media ownership/source policy;
- hallucination check: writer output vs facts;
- CTA check: не обещать повтор без repeatability evidence.

### 18.2. Post-publication monitoring

- отслеживание удалённых/исправленных исходных постов;
- реакции/жалобы/комментарии;
- аномально плохой engagement;
- ошибки ссылок;
- автоматический suppress при критическом флаге;
- rebuild страницы при появлении новых sources.

### 18.3. Feedback commands

Это не ручная премодерация, а постфактум-исправление.

Команды:

```text
/recap_suppress <id> <reason>
/recap_rebuild <id>
/recap_source_block <source>
/recap_source_trust <source> <delta>
/recap_link_fix <recap_id> <event_id>
```

Любая команда должна создавать training/evaluation signal:

```text
operator correction → error class → regression test / prompt rule / scoring adjustment
```

---

## 19. Метрики успеха

### 19.1. Product metrics

| Метрика | Что показывает |
|---|---|
| recap channel views/post | базовый интерес к формату |
| recap channel subscriber growth | работает ли новый habit |
| reactions/reposts per recap | эмоциональная ценность |
| CTR from recap to announcements | ведёт ли recap к будущим событиям |
| CTR from future event to past recap | работает ли social proof |
| conversion to subscription | acquisition effect |
| future event engagement with attached recap | влияние на анонсы |

### 19.2. Quality metrics

| Метрика | Цель |
|---|---|
| event match precision | высокий при auto publication |
| duplicate recap rate | низкий |
| suppress rate | низкий, но не ноль |
| hallucination/error reports | снижать |
| source trust drift | хорошие источники растут, плохие падают |
| missed_event_signal count | индикатор дыр в event discovery |

### 19.3. Operational metrics

| Метрика | Цель |
|---|---|
| LLM calls per published recap | снижать |
| Google key usage by stage/model | не съедать event pipeline |
| Kaggle run success rate | стабильный batch |
| Astro build success rate | высокий |
| average processing delay | acceptable for daily recap |
| cost per accepted recap | контролируемый |

---

## 20. Конфигурация

Пример ENV/config:

```text
RECAP_PIPELINE_ENABLED=1
RECAP_DRY_RUN=0
RECAP_CHANNEL_ID=@...
RECAP_DAILY_MIN_POSTS=2
RECAP_DAILY_MAX_POSTS=5
RECAP_PROMO_SMALL_DAILY_MAX=2
RECAP_MAIN_CHANNEL_DAILY_MAX=1

RECAP_MATCH_MIN_CONFIDENCE=0.75
RECAP_AUTOPUBLISH_MIN_SCORE=65
RECAP_MICRO_MIN_SCORE=50
RECAP_TOP_SCORE=90
RECAP_RISK_MAX=35

RECAP_STRUCTURED_MODEL=models/gemma-4-26b-a4b-it
RECAP_WRITER_MODEL=gemini-lite-or-configured
RECAP_LLM_CONSUMER_SCOPE=recap

RECAP_SOURCE_DISCOVERY_ENABLED=1
RECAP_SOURCE_AUTO_LOW_FREQ_MIN_SCORE=60
RECAP_SOURCE_AUTO_ACTIVE_MIN_SCORE=75

RECAP_ASTRO_BUILD_ENABLED=1
RECAP_STATIC_SITE_ENABLED=1
RECAP_STATIC_NO_INDEX_THRESHOLD=80
RECAP_TELEGRAPH_FALLBACK=1
```

---

## 21. Риски

### 21.1. Неверная связь отчёта с событием

Самый опасный риск. Митигировать:

- строгий shortlist;
- high confidence threshold;
- `no_match` разрешён;
- при сомнении `evidence_only`, не публикация;
- post-fact correction command.

### 21.2. Recap-канал превращается в шум

Митигировать:

- daily quota;
- разнообразие типов;
- сегментная ротация;
- не публиковать слабые протокольные отчёты;
- weekly quality report.

### 21.3. LLM начинает придумывать эмоции

Митигировать:

- emotion evidence required;
- writer строит эмоциональные выводы только из `facts_emotion`;
- coverage/extra check;
- запрет массовых утверждений без evidence.

### 21.4. Сайт разрастается thin/noisy страницами

Митигировать:

- index/noindex routing;
- separate thin archive;
- quality thresholds;
- canonical links;
- series pages для повторяемых событий.

### 21.5. Source discovery захватывает мусор

Митигировать:

- trust ramp-up;
- low-frequency first;
- disable/block on errors;
- match success rate as trust signal;
- source score decay.

---

## 22. Открытые вопросы

1. Где физически будет recap-канал: новый Telegram channel, VK-only, оба?
2. Нужно ли делать отдельный recap brand или он остаётся под брендом основной афиши?
3. Какой минимальный набор исходных источников дать для source discovery seed?
4. Какие платформы MVP source discovery поддерживает первыми: TG, VK или обе?
5. Какую политику использовать для чужих фото/видео: только embedded links, repost, storage copy, thumbnail, no-copy?
6. Как долго хранить raw source text для опубликованных recap?
7. Какой final writer выбрать по факту теста: Gemini Lite или Gemma writer lane?
8. Нужен ли отдельный `/recap` command/admin report в боте?
9. Какой публичный домен/URL будет у static recap pages?
10. Как объединять recap нескольких источников, если один источник публикует фото, другой видео, третий текст?

---

## 23. Рекомендуемый ближайший план реализации

Самый рациональный порядок:

```text
1. Добавить schema и retention anchors.
2. Перехватить completed_event_report в recap_candidate_post.
3. Сделать event matching и missed_event_signal.
4. Подключить Smart Update fact extraction + emotion extraction.
5. Посчитать metrics/significance/recap_score.
6. Запустить автопубликацию в recap-канал с квотами.
7. Добавить Astro static pages для score 80+.
8. Добавить source discovery и автоматическое расширение мониторинга.
9. Добавить series/future-event reuse.
```

Главная архитектурная установка:

```text
Не строить второй Smart Update.
Строить Recap как post-event слой поверх Smart Update facts, event graph, metrics и source discovery.
```

---

## 24. Reference links внутри проекта

- `docs/features/smart-event-update/README.md`
- `docs/features/smart-event-update/fact-first.md`
- `docs/features/telegram-monitoring/README.md`
- `docs/features/vk-auto-queue/README.md`
- `docs/features/post-metrics/README.md`
- `docs/features/llm-gateway/README.md`
- `docs/features/kaggle-status-framework/README.md`
- `docs/features/subscriber-acquisition/requirements.md`
- `docs/features/promo-campaigns/README.md`

---

## 25. Итоговый verdict

Recap стоит запускать. Лучшее решение — не “отчёты ради отчётов”, а автоматический post-event retention loop:

```text
event → post-event evidence → facts + emotion → recap → channel/site → future event social proof → retention/acquisition
```

Discovery нужен, но его надо определить правильно: **recap discovery ищет источники и evidence**, потому что отчёты часто публикуют не те же источники, которые публиковали анонсы. Это отдельная задача от Subscriber Acquisition.

Smart Update нужно использовать как базовый fact-first механизм. Для recap добавляется слой эмоций, значимости, сегментного scoring и повторяемости. Это позволит не размножать extraction-процессы, а использовать уже успешный pipeline проекта.
