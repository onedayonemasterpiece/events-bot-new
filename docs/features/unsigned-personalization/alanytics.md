> **Source research draft.** This file preserves the initial analytical proposal.
> Canonical implementation decisions are in `README.md`, `neural-flow.md`,
> `taxonomy.md`, `database.md`, and `model-selection.md`. In particular, legacy
> examples that used one broad `negative_tags` concept are superseded by
> `event.audience_exclusion_tags` and `user.negative_interest_tags`.

Как строится выявление и долгосрочный контроль интересов пользователя просматривающего анонсы
Вводная: некая система собирает информация о социальном поведении пользователя при просмотре афиш. Пользователь приходит смотреть афиши допустим регулярно, как из просмотренного им сформировать выдачу для него более релеватную?

Я предполагаю что нужна нейросеть, подойдёт ли нейросеть типа Gemma4-31B например или более младшие?

Как это может работать? Вот есть информация о последней пачке просмотров, мы её закинули в нейросеть, с чем, в каком формате? Что будет результатом? Как это хранить, что это? Что далее подавать чтобы решить показывать конкретную выборку афиш или нет, точнее что из этой выборки показывать или нет?

Как решается задача персонализированного выбора при использовании нейросетей.

Разбор на практике под будущее применение. Цель чтобы работало для большого количества пользоваетелей на доступных моделях

Разберу это как практическую архитектуру рекомендательной системы, а не просто передачу просмотров в LLM: данные, признаки, профиль интересов, ранжирование, хранение и контроль. Отдельно проверю актуальность Gemma и где уместнее LLM, эмбеддинги или классические ранжировщики.

Думал на протяжении 4m 9s

Да, задача решается нейросетями, но не так, что вы просто каждую пачку просмотров отправляете в Gemma и просите “выбери афиши”. Для большого числа пользователей обычно строят рекомендательную систему из нескольких слоёв:

события пользователя → признаки и профиль интересов → генерация кандидатов → ранжирование → переранжирование → обратная связь → обновление профиля.

LLM вроде Gemma может быть полезна, но чаще как вспомогательный компонент: понять текст афиши, извлечь жанры/темы/сущности, сделать embeddings, объяснить рекомендацию, обработать холодный старт. Основное ранжирование лучше делать более дешёвыми моделями: эмбеддинги, two-tower retrieval, CatBoost/LightGBM/XGBoost, маленькая нейросеть, learning-to-rank.

1. Что именно нужно хранить

У вас есть две главные сущности: афиша и поведение пользователя.

Афиша

Для каждой афиши нужно хранить не только текст, но и нормализованные признаки.

Пример:

{
  "event_id": "ev_123",
  "title": "Вечер камерного джаза",
  "description": "Концерт современного джаза в малом зале...",
  "category": "music",
  "subcategories": ["jazz", "live_music", "instrumental"],
  "artists": ["..."],
  "venue_id": "venue_45",
  "city": "Москва",
  "date_start": "2026-07-12T19:00:00",
  "price_min": 1500,
  "price_max": 4500,
  "age_rating": "12+",
  "tags": ["джаз", "камерный зал", "вечер", "инструментальная музыка"],
  "embedding": [0.012, -0.045, 0.221, "..."]
}

embedding — это числовой вектор смысла афиши. Его можно получить не большой LLM, а специальной embedding-моделью. Например, у Google есть EmbeddingGemma — 308M-параметрическая multilingual embedding-модель, предназначенная для retrieval, semantic similarity, classification и clustering. Это гораздо ближе к вашей задаче, чем большая генеративная модель на каждый запрос.

Поведение пользователя

Нужно хранить не только факт просмотра, а тип действия, контекст и силу сигнала.

Пример:

{
  "user_id": "u_777",
  "event_id": "ev_123",
  "action": "view_detail",
  "timestamp": "2026-06-20T12:45:00",
  "dwell_seconds": 38,
  "source": "main_feed",
  "position": 4,
  "session_id": "s_abc",
  "device": "mobile",
  "city_context": "Москва"
}

Типы действий желательно различать:

impression         — пользователь просто увидел карточку
quick_skip         — быстро пролистал
view_detail        — открыл афишу
long_dwell         — долго читал
save/favorite      — сохранил
share              — поделился
ticket_click       — перешёл к покупке
purchase           — купил / зарегистрировался
hide/not_interested — явно неинтересно

Разные действия имеют разный вес. Например:

impression:          0.1
quick_skip:         -0.5
view_detail:         1.0
long_dwell:          2.0
save:                4.0
ticket_click:        6.0
purchase:           10.0
hide:               -8.0

Это не финальные веса, а стартовая эвристика.

2. Как из просмотров получить профиль интересов

Профиль пользователя лучше хранить не в виде одного текста “пользователь любит джаз и театр”, а в нескольких формах одновременно.

2.1. Краткосрочный вектор интересов

Это то, что пользователь смотрит сейчас или в последние дни.

Например, если он за последнюю сессию посмотрел 5 джазовых концертов, краткосрочный профиль быстро смещается в сторону джаза.

Формула упрощённо:

user_vector_short =
  weighted_average(
    item_embedding_i * action_weight_i * time_decay_i
  )

Где:

item_embedding_i — вектор афиши
action_weight_i  — вес действия: просмотр, сохранение, покупка и т.д.
time_decay_i     — затухание по времени

Например:

time_decay = exp(-age_hours / tau)

Для краткосрочного профиля tau может быть 24–72 часа.

2.2. Среднесрочный профиль

Это интересы за последние недели: например, пользователь стабильно смотрит стендап, выставки современного искусства и концерты по пятницам.

user_vector_mid =
  weighted_average(events from last 14-30 days)
2.3. Долгосрочный профиль

Это устойчивые интересы за месяцы: любимые жанры, площадки, районы, ценовой диапазон, типы мероприятий, дни недели.

user_vector_long =
  weighted_average(events from last 6-12 months)

Для долгосрочного профиля затухание медленнее. Старые действия не исчезают сразу, но постепенно теряют вес.

2.4. Явные распределения по признакам

Кроме вектора полезно хранить понятные статистики:

{
  "user_id": "u_777",
  "category_affinity": {
    "music": 0.74,
    "theatre": 0.31,
    "sport": -0.12,
    "kids": -0.30
  },
  "tag_affinity": {
    "jazz": 0.81,
    "standup": 0.43,
    "classical_music": 0.36,
    "football": -0.22
  },
  "venue_affinity": {
    "venue_45": 0.62,
    "venue_18": 0.41
  },
  "price_preference": {
    "min_usual": 800,
    "max_usual": 4000,
    "median_clicked": 1800
  },
  "time_preference": {
    "weekday_evening": 0.55,
    "friday_evening": 0.77,
    "sunday_morning": -0.15
  }
}

Зачем это нужно? Вектор хорошо ловит смысловую близость, но плохо объясняет бизнес-логику. А распределения по жанрам, цене, району, времени и площадкам дают контроль.

3. Что подавать в модель

Зависит от того, какую именно модель вы используете.

Вариант A. Простая эвристическая модель для MVP

На вход:

{
  "user_profile": {
    "short_vector": [...],
    "long_vector": [...],
    "tag_affinity": {...},
    "price_preference": {...},
    "city": "Москва"
  },
  "candidate_event": {
    "event_id": "ev_123",
    "embedding": [...],
    "category": "music",
    "tags": ["jazz", "live_music"],
    "price_min": 1500,
    "date_start": "2026-07-12T19:00:00",
    "popularity_score": 0.72,
    "freshness_score": 0.91
  }
}

Считается итоговый скор:

score =
  0.35 * cosine(user_short_vector, event_embedding)
+ 0.25 * cosine(user_long_vector, event_embedding)
+ 0.15 * category_affinity
+ 0.10 * tag_affinity
+ 0.05 * price_match
+ 0.05 * date_match
+ 0.05 * popularity_or_freshness

Это можно запустить быстро, без обучения большой модели.

Вариант B. Learning-to-rank модель

Когда накопятся данные, вы обучаете модель предсказывать вероятность целевого действия:

P(click)
P(save)
P(ticket_click)
P(purchase)
P(long_dwell)

На вход модели подаются признаки пары user-event:

{
  "user_id": "u_777",
  "event_id": "ev_123",

  "features": {
    "dot_short_event": 0.81,
    "dot_long_event": 0.63,
    "category_affinity_music": 0.74,
    "tag_affinity_jazz": 0.81,
    "price_distance_from_user_median": 0.12,
    "days_until_event": 22,
    "same_city": true,
    "seen_before": false,
    "venue_affinity": 0.62,
    "event_popularity_7d": 0.68,
    "friend_saved_count": 3,
    "friend_attended_count": 1
  },

  "label": {
    "clicked": 1,
    "saved": 0,
    "purchased": 0
  }
}

Для старта часто достаточно CatBoost/LightGBM/XGBoost. Они дешевле, понятнее и часто сильнее “голой LLM” для табличного ранжирования.

Вариант C. Two-tower модель

Это уже более нейросетевой вариант.

Одна “башня” кодирует пользователя:

user features → user embedding

Вторая “башня” кодирует афишу:

event features → event embedding

Потом считается близость:

score = dot(user_embedding, event_embedding)

Такая архитектура удобна для масштабной генерации кандидатов: можно заранее посчитать embeddings всех афиш, а в момент запроса быстро найти ближайшие к пользователю. Google Cloud описывает two-tower retrieval как подход для personalization, где модель учит semantic similarity между двумя сущностями, например пользователем/запросом и кандидатами.

4. Правильная архитектура выдачи

Типовая рекомендательная система не ранжирует весь каталог сразу. Она работает в несколько стадий.

Google в своих материалах по рекомендательным системам описывает трёхстадийную схему: candidate generation, scoring, re-ranking. Candidate generation сужает большой пул до небольшого набора, scoring оценивает релевантность, re-ranking учитывает дополнительные ограничения вроде разнообразия, свежести и предпочтений.

Для афиш это может выглядеть так.

Стадия 1. Фильтры допустимости

Сначала убираем то, что вообще нельзя показывать:

другой город
мероприятие уже прошло
нет билетов
неподходящий возрастной рейтинг
пользователь уже скрыл событие
пользователь уже купил билет
слишком далеко по географии
Стадия 2. Генерация кандидатов

Из всего каталога получаем, например, 500–5000 кандидатов.

Источники кандидатов:

1. Похожие по embedding к краткосрочному профилю пользователя.
2. Похожие по embedding к долгосрочному профилю.
3. Афиши из любимых категорий и тегов.
4. Афиши с любимыми артистами, площадками, районами.
5. Популярные среди похожих пользователей.
6. Популярные среди друзей или социальной группы.
7. Новые и свежие события.
8. Редакционные/промо-кандидаты.
9. Exploration-кандидаты — немного нового вне привычного пузыря.

На этом этапе можно использовать vector search / ANN-индекс: FAISS, ScaNN, Milvus, Qdrant, Elasticsearch/OpenSearch vector search и т.д.

Стадия 3. Ранжирование

Берёте кандидатов и для каждого считаете вероятность полезного действия:

score_click
score_save
score_ticket_click
score_purchase
score_negative_feedback

Финальный скор может быть не просто CTR:

final_score =
  0.25 * P(click)
+ 0.20 * P(save)
+ 0.35 * P(ticket_click_or_purchase)
+ 0.10 * expected_dwell
- 0.20 * P(hide_or_skip)
+ freshness_bonus
+ diversity_bonus

Для афиш важно не оптимизировать только клики. Иначе система быстро начнёт показывать “кликабельные”, но не обязательно полезные события.

Стадия 4. Переранжирование

После ранжирования нужно поправить выдачу:

не показывать 10 джазовых концертов подряд
разбавить категориями
не повторять одну и ту же площадку слишком часто
учесть близость даты
дать шанс новым событиям
оставить 10–20% exploration
соблюсти рекламные/редакционные ограничения

Например:

из топ-100 по score выбрать топ-20,
но не больше 3 событий одного поджанра,
не больше 2 событий одной площадки,
минимум 2 новых события,
минимум 1 exploration-кандидат.
5. Где здесь Gemma / LLM

Gemma 4 31B действительно существует: в официальной документации Google указаны размеры Gemma 4 E2B, E4B, 12B, 31B и 26B A4B; там же отмечено, что большие модели обычно способнее, но дороже по вычислениям, памяти и энергии.

Но для вашей задачи Gemma 4 31B, скорее всего, избыточна.

Где LLM полезна

LLM хорошо использовать для обработки новых афиш:

сырой текст афиши → нормализованные теги, жанры, настроение, аудитория, сущности

Например, вход:

{
  "title": "Вечер камерного джаза",
  "description": "Концерт современного инструментального джаза в малом зале...",
  "venue": "Дом музыки",
  "city": "Москва"
}

Выход:

{
  "category": "music",
  "subcategories": ["jazz", "instrumental", "live_music"],
  "mood": ["calm", "evening", "intellectual"],
  "audience": ["adults", "music_lovers"],
  "entities": {
    "genres": ["jazz"],
    "venue_type": "concert_hall"
  },
  "confidence": 0.91
}

Это можно делать один раз при добавлении афиши, а не на каждый пользовательский запрос.

LLM также полезна для:

обогащения плохих описаний
нормализации жанров
извлечения артистов и площадок
классификации “для детей / не для детей”
создания человекочитаемых объяснений
обработки поисковых запросов пользователя
создания cold-start анкеты
Где LLM не лучший выбор

Плохая идея:

на каждый заход пользователя отправлять последние 100 просмотров + 500 афиш в Gemma 31B
и просить выбрать лучшие.

Почему:

дорого
медленно
сложно контролировать
плохо масштабируется
неудобно обучать на миллионах интеракций
риск нестабильного JSON/галлюцинаций
трудно проводить A/B-тесты

Для массовой персонализации лучше:

LLM/embedding-модель → офлайн-обогащение и векторы
маленькие модели → онлайн-рекомендации
6. Как может выглядеть “пачка просмотров” для LLM

Если всё-таки использовать LLM для обновления интерпретируемого профиля, то лучше подавать не сырые логи, а компактный JSON.

Вход:

{
  "previous_profile": {
    "stable_interests": ["джаз", "камерные концерты", "стендап"],
    "negative_interests": ["детские мероприятия", "футбол"],
    "preferred_price_range": [1000, 4000],
    "preferred_time": ["friday_evening", "saturday_evening"],
    "confidence": 0.72
  },
  "recent_events": [
    {
      "action": "view_detail",
      "dwell_seconds": 42,
      "timestamp": "2026-06-20T12:45:00",
      "event": {
        "title": "Вечер камерного джаза",
        "category": "music",
        "tags": ["jazz", "live_music", "instrumental"],
        "price_min": 1500,
        "venue_type": "concert_hall"
      }
    },
    {
      "action": "hide",
      "timestamp": "2026-06-20T12:47:00",
      "event": {
        "title": "Детский спектакль",
        "category": "kids",
        "tags": ["children", "theatre"]
      }
    }
  ],
  "task": "Update user interest profile. Return strict JSON only."
}

Выход:

{
  "profile_updates": {
    "positive_tags": [
      {"tag": "jazz", "delta": 0.08, "confidence": 0.86},
      {"tag": "instrumental_music", "delta": 0.04, "confidence": 0.74}
    ],
    "negative_interest_tags": [
      {"tag": "children_events", "delta": -0.12, "confidence": 0.91}
    ],
    "price_range_update": {
      "min": 1000,
      "max": 4500,
      "confidence": 0.63
    },
    "short_term_intent": "Пользователь сейчас проявляет интерес к живым музыкальным событиям, особенно джазу.",
    "ttl_days": 14
  }
}

Но это лучше использовать как дополнительный слой объяснимости, а не как главный механизм. Главный профиль лучше обновлять математически: через embeddings, веса действий и decay.

7. Как хранить профиль пользователя

Можно хранить несколько таблиц/документов.

Таблица событий
user_events(
  user_id,
  event_id,
  action,
  timestamp,
  dwell_seconds,
  session_id,
  source,
  position,
  context_json
)
Таблица афиш
events(
  event_id,
  title,
  description,
  category,
  tags,
  city,
  venue_id,
  date_start,
  price_min,
  price_max,
  status,
  metadata_json
)
Таблица embeddings афиш
event_embeddings(
  event_id,
  embedding_vector,
  model_version,
  created_at
)
Таблица профилей пользователей
user_profiles(
  user_id,
  short_vector,
  mid_vector,
  long_vector,
  category_affinity_json,
  tag_affinity_json,
  venue_affinity_json,
  price_preference_json,
  time_preference_json,
  updated_at,
  profile_version
)
Таблица социальных признаков

Если есть социальное поведение:

social_features(
  user_id,
  event_id,
  friend_saved_count,
  friend_purchased_count,
  cohort_popularity_score,
  similar_users_score,
  updated_at
)

Лучше не показывать “ваш друг X идёт”, если нет явного согласия. Для ранжирования можно использовать агрегаты: “3 человека из похожей аудитории сохранили”.

8. Как обновлять долгосрочные интересы

Допустим, пользователь посмотрел афишу event_id = ev_123.

Получаем:

item_vector = embedding(ev_123)
action_weight = weight("view_detail") = 1.0

Обновляем краткосрочный профиль:

user_short_vector =
  normalize(0.85 * old_user_short_vector + 0.15 * action_weight * item_vector)

Обновляем долгосрочный профиль осторожнее:

user_long_vector =
  normalize(0.995 * old_user_long_vector + 0.005 * action_weight * item_vector)

Если пользователь купил билет, вес выше:

purchase_weight = 10.0

Если пользователь нажал “не интересно”, вес отрицательный:

hide_weight = -8.0

Важно: отрицательные сигналы лучше хранить отдельно, а не просто вычитать из общего вектора. Иначе профиль может стать шумным.

Например:

{
  "positive_vector_long": [...],
  "negative_vector_long": [...],
  "positive_tags": {"jazz": 0.81},
  "negative_interest_tags": {"kids": 0.72}
}

При ранжировании:

score =
  similarity(positive_user_vector, event_vector)
- 0.7 * similarity(negative_user_vector, event_vector)
9. Что показывать из конкретной выборки афиш

Допустим, у вас есть конкретная выборка из 1000 доступных афиш. Для каждой афиши считаете набор признаков:

sim_short = cosine(user_short_vector, event_vector)
sim_long = cosine(user_long_vector, event_vector)
category_score = affinity(user, event.category)
tag_score = average_affinity(user, event.tags)
venue_score = affinity(user, event.venue)
price_score = match(user.price_preference, event.price)
time_score = match(user.time_preference, event.date_start)
social_score = friend_saved_count / normalized
freshness_score = new_event_bonus
negative_score = similarity(user_negative_vector, event_vector)

Затем:

raw_score =
  model.predict(user_event_features)

Или на MVP:

raw_score =
  0.30 * sim_short
+ 0.25 * sim_long
+ 0.15 * tag_score
+ 0.10 * category_score
+ 0.05 * venue_score
+ 0.05 * price_score
+ 0.05 * time_score
+ 0.05 * social_score
- 0.30 * negative_score

Потом переранжирование:

top = sort_by(raw_score)
top = apply_diversity(top)
top = apply_freshness(top)
top = apply_business_rules(top)
top = insert_exploration(top)
return top_20
10. Как обучать модель

Сначала у вас есть логи:

user_id, event_id, timestamp, action, context

Из них делаете обучающие примеры:

Положительные:
- пользователь открыл карточку
- долго читал
- сохранил
- перешёл к покупке
- купил

Отрицательные:
- увидел, но не кликнул
- быстро пролистал
- скрыл
- нажал “не интересно”

В TensorFlow Recommenders в retrieval-туториале прямо используется implicit feedback: просмотренные фильмы считаются положительными примерами, а непосмотренные — implicit negative. Это близко к вашей ситуации с афишами: пользователь редко ставит оценку явно, поэтому почти всё обучение строится на поведении.

Обучающие строки:

{
  "user_features": {
    "user_id": "u_777",
    "short_vector": [...],
    "long_vector": [...],
    "city": "Москва",
    "top_tags": ["jazz", "standup"]
  },
  "event_features": {
    "event_id": "ev_123",
    "event_vector": [...],
    "category": "music",
    "tags": ["jazz"],
    "price_min": 1500,
    "days_until_event": 22
  },
  "context_features": {
    "hour": 19,
    "weekday": "friday",
    "device": "mobile",
    "source": "main_feed"
  },
  "label": 1
}

Для label можно сделать градацию:

0.0  — показал, пользователь проигнорировал
0.2  — открыл
0.4  — долго читал
0.6  — сохранил
0.8  — перешёл к покупке
1.0  — купил
-1.0 — скрыл / не интересно

Но для многих моделей лучше обучать отдельные цели:

clicked
saved
ticket_clicked
purchased
hidden

И потом собирать бизнесовый final_score.

11. Какой стек я бы выбрал

Для практического запуска на доступных моделях:

MVP
PostgreSQL / ClickHouse для событий
PostgreSQL / Elasticsearch / OpenSearch для каталога
Qdrant / FAISS / Milvus для векторного поиска
EmbeddingGemma или другой embedding model для афиш
простая формула ранжирования
ручные веса действий
A/B-тесты
Следующий уровень
CatBoost / LightGBM ranker
feature store для пользовательских и event-признаков
ANN retrieval
несколько горизонтов интересов: session / 7d / 30d / 180d
переранжирование по diversity/freshness
Более зрелая система
two-tower retrieval
нейросетевой ranker
мультитасковое обучение: click/save/purchase/hide
контекстная персонализация
exploration/exploitation
causal/counterfactual correction для bias показов

TensorFlow Recommenders подходит как библиотека для такого направления: официальная документация описывает её как библиотеку для workflow рекомендательных систем — подготовка данных, формулировка модели, обучение, оценка и deployment; она поддерживает retrieval-модели, пользовательские/item/context-признаки и multi-task цели.

12. Какую Gemma брать

Мой практический ответ:

Для ранжирования афиш Gemma 4 31B не нужна как основная модель.

Лучше так:

EmbeddingGemma / embedding-модель:
  для векторов афиш и семантического поиска

Gemma E2B/E4B/12B:
  для извлечения тегов, жанров, сущностей, нормализации описаний

Gemma 31B:
  только если нужно высококачественное сложное понимание текста,
  генерация объяснений, сложная модерация, обработка неоднозначных описаний,
  и это делается офлайн или батчами

Если у вас “большое количество пользователей”, то 31B на онлайн-запросах будет дорогой архитектурной ошибкой. Лучше потратить ресурсы на хороший event pipeline, embeddings, ranker и оценку качества.

13. Контроль долгосрочных интересов

Проблема в том, что интересы меняются. Пользователь мог неделю смотреть детские мероприятия, потому что искал подарок племяннику, но это не значит, что он теперь “детская аудитория” навсегда.

Поэтому нужны разные горизонты:

session profile    — что интересно прямо сейчас
short-term profile — последние дни
mid-term profile   — последние недели
long-term profile  — устойчивые паттерны

Пример:

{
  "session": {
    "dominant_tags": ["выставка", "импрессионизм"],
    "ttl": "2 hours"
  },
  "short_term": {
    "dominant_tags": ["джаз", "концерт"],
    "ttl": "14 days"
  },
  "long_term": {
    "dominant_tags": ["театр", "стендап", "живая музыка"],
    "ttl": "365 days"
  }
}

В ранжировании можно использовать смесь:

user_interest =
  0.45 * session_vector
+ 0.25 * short_vector
+ 0.30 * long_vector

Но если сессия слабая, например всего один просмотр, её вес уменьшается.

14. Важная проблема: просмотр ≠ интерес

Пользователь мог открыть афишу потому что:

она была первой
картинка яркая
название странное
он ошибся
он искал не себе
он сравнивал цены

Поэтому нельзя считать каждый просмотр сильным интересом.

Лучше разделять:

слабый сигнал: показ, короткий просмотр
средний сигнал: открытие, долгий dwell
сильный сигнал: сохранение, подписка, покупка
явный отрицательный сигнал: скрытие, “не интересно”

И обязательно учитывать позицию в выдаче. Если афиша была на первом месте, шанс клика выше просто из-за позиции, а не из-за реального интереса.

15. Метрики качества

Офлайн:

Recall@K
Precision@K
NDCG@K
MAP@K
AUC
logloss
coverage
diversity
novelty
calibration

Онлайн:

CTR
save rate
ticket click rate
purchase/conversion rate
hide rate
return rate
session depth
долгосрочное удержание
разнообразие потребления

Для афиш особенно важно смотреть:

переходы к покупке
сохранения
возвраты пользователя
не только клики

Иначе система будет учиться показывать “самое кликабельное”, а не “самое ценное”.

16. Практическая схема для вашего кейса

Я бы строил так:

1. Собираем события пользователя.
2. Обогащаем каждую афишу:
   - категория
   - теги
   - сущности
   - embedding
   - цена/город/дата/площадка

3. Обновляем профиль пользователя:
   - short_vector
   - long_vector
   - tag_affinity
   - negative_interest_tags
   - venue_affinity
   - price/time preferences

4. Для запроса выдачи:
   - фильтруем недоступные афиши
   - генерируем кандидатов через vector search, теги, популярность, социальные сигналы
   - ранжируем моделью
   - переранжируем для diversity/freshness/exploration
   - показываем top-N

5. Логируем:
   - что показали
   - на какой позиции
   - что пользователь сделал
   - обновляем модель и профиль

Минимальный рабочий вариант можно сделать без большой LLM:

embedding-модель для афиш
взвешенный профиль пользователя
vector search
простая скоринговая формула
ручное переранжирование

После накопления данных:

обучить ranker
добавить two-tower retrieval
добавить LLM для обогащения афиш и объяснений

Главное: профиль пользователя — это не текстовая “память” LLM, а набор числовых и структурных признаков, которые обновляются после каждого действия и используются для ранжирования. LLM может помогать понимать контент, но не должна быть единственным мозгом рекомендательной системы.
