# Подборки: пакет для внешнего критического анализа

Статус: **provisional review seed; не gold, не разрешение на публикацию**, 2026-08-01.

Этот документ нужен внешнему аналитику для критики продуктовых границ, качества
production-данных и выбранной схемы `Smart Update -> один StaticSiteBuilder ->
shared BGE -> ID-only manifests -> Astro`. Исходные требования владельца не
переписываются и остаются в [`podborki.md`](./podborki.md); полный проект — в
[`podborki-to-be.md`](./podborki-to-be.md).

Ветка review-пакета: `agent/static-collections-review/curation` от
`origin/main@db894ac521d7d74928415ba94fdcea59f6a504d4`.

## 1. Что именно требуется от аналитика

Нужен не общий пересказ и не визуальный review будущих страниц, а независимый
ответ на вопросы:

1. полезны ли пользователю предложенные границы каждой подборки;
2. какие provisional positives надо удалить, перенести или разделить;
3. достаточно ли hard negatives, особенно самых похожих на positives;
4. не смешивает ли «Наука» естественные, технические и гуманитарные исследования
   с научпопом, искусством и edutainment;
5. не превращаются ли «Сильные впечатления» в любую мастерскую или рекламно
   «яркое» пассивное шоу;
6. нужно ли публиковать medieval-подборку при малом числе независимых occurrence
   families;
7. правильно ли разделены direct kids и joint family audience и должна ли одна
   публичная `/detyam/` объединять обе принятые полки;
8. достаточна ли схема extraction приезжающих людей и provenance их origin;
9. верна ли модель occurrence-family для спектаклей и какие integrity gates
   обязательны до Astro;
10. в каком порядке выпускать страницы без снижения качества выдачи.

Ожидаемый результат review: для каждого provisional positive — `keep | drop |
move:<label> | needs_more_source`, для спорных hard negatives — `keep_negative |
move_positive | ambiguous`, затем уточнённые определения и NO-GO conditions.

## 2. Данные и воспроизводимость

Review основан на реальном read-only production Fly SQLite, а не на committed
preview data и не на сканировании сайтов площадок.

| Контроль | Значение |
|---|---|
| Production DB | Fly `/data/db.sqlite`, только read-only |
| Static batch | `/data/static_site_builder/collection-batch-v1.json` |
| Batch generated | `2026-08-01T20:32:41.791471Z` |
| Frozen catalog | 409 событий |
| BGE | `BAAI/bge-m3@5617a9f…`, 1024d, dense FP32, L2 |
| Document | `collection_semantics_v1 / collection-semantics-doc-v1` |
| Provider calls | 0 |
| Supabase core reads | 0 |
| Seed fixture | [`tests/fixtures/static_collections_gold_v1.json`](../../../tests/fixtures/static_collections_gold_v1.json) |

BGE-документ содержит только title, description, primary event type, venue,
city, organizers и participants. `topics`, regex tags и related digest намеренно
не передаются. Fixture сохраняет компактный excerpt того же смыслового
материала, но диагностически показывает, выбрал ли событие текущий BGE head.

Fixture намеренно имеет:

```text
status = provisional_agent_seed_not_owner_approved
publication_eligible = false
```

Это recall-oriented старт владельцу: спорные строки не скрыты, а помечены
`borderline`. Численный минимум сам по себе не превращает seed в gold.

## 3. Текущий BGE результат уже доказывает отсутствие калибровки

| Head | BGE candidates | Доля каталога | Вывод до ручной проверки |
|---|---:|---:|---|
| science | 139 | 34.0% | слишком широко для узкого определения через метод/данные |
| strong impressions | 169 | 41.3% | вероятна утечка обычных шоу и мастер-классов |
| medieval | 308 | 75.3% | head практически не различает тему и фоновые исторические слова |
| audience kids | 71 | 17.4% | одновременно есть существенные false negatives |
| audience family | 391 | 95.6% | непригодно как publication truth |
| unusual | 240 | 58.7% | старый head нельзя переносить на новый document contract без recalibration |

Текущие generic thresholds: positive similarity `0.42`, margin `0.02`;
audience recall-heads: `0.38` и margin `-0.01`. Manifest сохраняет item IDs, но
не threshold curve. После редакционной правки seed необходимо пересчитать exact
scores/PR curve над hash-bound NPZ, а не выбирать новый threshold по одному
удобному примеру.

Особенно важные видимые ошибки recall:

- `strong_impressions` не выбрал `6191 «Большой Кауп»`, `7333 «Чудеса науки»`
  и `7374 «Путешествие на драккаре»`;
- `audience_kids` не выбрал многие прямые детские события: `6689`, `6822`,
  `6823`, `7054`, `7112`, `7134`, `7172`, `7176`, `7211`, `7376`;
- `audience_family` выбрал почти весь каталог, поэтому его recall сейчас ничего
  не говорит о precision;
- medieval выбрал 308 событий при примерно десяти содержательно правдоподобных
  families.

Если уже сейчас грубо сравнить бинарный output с **provisional**, ещё не
принятым seed, картина такая:

| Head | Provisional recall | Hard-negative FPR | Требование | Предварительный вывод |
|---|---:|---:|---:|---|
| science | 16/16 = 1.00 | 14/25 = 0.56 | FPR <= 0.05 | head слишком широкий |
| strong impressions | 17/20 = 0.85 | 6/25 = 0.24 | recall >= 0.80, FPR <= 0.05 | recall приемлем, precision нет |
| medieval | 11/11 = 1.00 | 22/25 = 0.88 | FPR <= 0.05 | output почти не классифицирует |
| audience kids | 7/20 = 0.35 | 8/25 = 0.32 | recall >= 0.95 | одновременно низкие recall и precision |
| audience family | 20/20 = 1.00 | 24/25 = 0.96 | recall >= 0.95 | почти constant-positive head |

Эти числа нельзя использовать для финальной калибровки до owner review, но они
уже достаточны для NO-GO: текущие thresholds нельзя просто разрешить в public
manifest.

## 4. Предварительная редакционная выборка

Ниже перечислены все provisional positives. `high` означает мою уверенность в
границе, а не независимое подтверждение владельца. `borderline` намеренно
оставлен немного избыточно, чтобы владелец мог удалять из уже видимой выборки.
Полные 25 hard negatives на каждый head с compact evidence находятся в JSON
fixture.

### Наука

Определение: Научный метод, первичные данные/источники, полевая или исследовательская практика; не просто эффектное научное шоу.

| ID | Событие | Уверенность | Основание | BGE сейчас |
|---:|---|---|---|---|
| 698 | Древние воины Янтарного края | borderline | `primary_archaeological_evidence` | да |
| 4327 | Отдыха не знали, Из руин подняли | high | `primary_archive_evidence` | да |
| 4648 | Они были первыми | borderline | `interactive_history_of_discovery` | да |
| 6696 | 📷 Выставка пинхол-снимков Бориса Андреева | borderline | `technology_demonstration` | да |
| 6766 | Пинхол. Гусев | borderline | `technology_demonstration` | да |
| 6767 | Летний Экодвор | borderline | `ecology_program` | да |
| 6818 | Воображая Калининград | borderline | `humanities_research_program` | да |
| 6878 | Всё об экологичном травничестве на Экодворе | borderline | `ecology_practice_requires_claim_review` | да |
| 6937 | Лекция «Жизни и путешествиям Миклухо-Маклая» | high | `researcher_biography_and_fieldwork` | да |
| 7055 | Выставка «Новая жизнь: до и после» | borderline | `restoration_engineering_case_studies` | да |
| 7088 | Медицина и музыка | high | `research_results_and_experiments` | да |
| 7244 | Кинопоказ «Право женщин на море» | borderline | `documentary_about_working_scientists` | да |
| 7247 | Кинопоказ «Россия — пути времени» | high | `research_process_documentary` | да |
| 7310 | Человек на фоне истории города: Кёнигсберг — Калининград в литературе | high | `academic_humanities_analysis` | да |
| 7331 | 💡 Лекция «История истории философии: краткий экскурс в гуманитарное знание» | high | `methodology_of_humanities` | да |
| 7361 | Лекция «Знание — власть? Бэкон, Кант и современная наука» | borderline | `philosophy_of_scientific_method` | да |

Границы hard-negative:
`ordinary_concert` (1), `therapy_not_science` (1), `metaphorical_research_in_art` (1), `art_exhibition_not_science` (1), `concert_with_medical_biography` (1), `fictional_experiment` (1), `military_history_concert` (1), `art_practice_uses_research_language` (1), `dance_show_about_body` (1), `artistic_research_metaphor` (1), `relationship_comedy` (1), `stage_show_uses_explores` (1), `standup` (1), `theatre_uses_research_language` (1), `immersive_history_not_science` (1), `ordinary_spectacle` (1), `art_exhibition_about_ruins` (1), `physical_theatre_discussion` (1), `coffee_art_lecture` (1), `ordinary_concert_with_statistics` (1), `botanical_drawing_not_botany` (1), `generic_science_edutainment` (1), `generic_science_edutainment_duplicate` (1), `psychology_workshop_not_research` (1), `art_history_lecture` (1)

### Сильные впечатления

Определение: Посетитель сам проходит интенсивную, редкую, иммерсивную или эмоционально значимую практику; рекламная яркость и пассивное шоу недостаточны.

| ID | Событие | Уверенность | Основание | BGE сейчас |
|---:|---|---|---|---|
| 3999 | Женская арт-терапевтическая группа | borderline | `emotionally_intensive_group_practice` | да |
| 5781 | Экскурсия «Закулисье театра» | high | `behind_scenes_access` | да |
| 6191 | Фестиваль «Большой Кауп» | high | `living_history_participation` | нет |
| 6248 | 💃 Мини-курс «exoSummer sunset» | high | `active_skill_challenge` | да |
| 6652 | Руслан и Людмила. На стыке времён | high | `interactive_site_specific_theatre` | да |
| 6865 | Триатлон поколений | high | `active_team_challenge` | да |
| 6871 | Иммерсивный проект «Окна времени» | high | `immersive_multisensory_history` | да |
| 6949 | Магия персидских инструментов: мастер-класс Хамида Резы Даду | high | `rare_instrument_practice` | да |
| 7047 | Занятия по актерскому мастерству | high | `active_theatre_practice` | да |
| 7080 | Физический театр: как тело говорит без слов | high | `talk_film_body_practice_hybrid` | да |
| 7102 | Мастер-класс по созданию гига-мозаики | high | `collective_giant_mosaic` | да |
| 7103 | Утренники на Балтике | high | `cold_water_physical_activation` | да |
| 7238 | Экскурсия «Закулисье театра» | high | `restricted_backstage_route` | да |
| 7283 | День физкультурника | high | `active_multi_sport_participation` | да |
| 7290 | Столярный мастер-класс «Человек – пиктограмма (семейный)» | borderline | `hands_on_family_woodwork` | да |
| 7330 | 🎨 Тихая программа «Про историю и будущее» | borderline | `community_place_making` | да |
| 7333 | 🧪 Фестиваль «Чудеса науки!» | high | `hands_on_experiment_program` | нет |
| 7351 | 🤍 Мастер-класс «Линия жизни семьи» | borderline | `guided_emotional_reflection` | да |
| 7372 | 🧘‍♀️ «Семейный коврик» | borderline | `shared_body_practice` | да |
| 7374 | ⛵ Путешествие на драккаре викингов «Рагна» | high | `three_hour_viking_boat_participation` | нет |

Границы hard-negative:
`ordinary_concert` (5), `passive_dance_show` (3), `passive_multimedia_show` (2), `ordinary_spectacle` (2), `ordinary_concert_with_evocative_copy` (1), `passive_show_with_pyrotechnics` (1), `ordinary_candle_concert` (1), `passive_stage_show` (1), `standup` (1), `ordinary_organ_concert` (1), `passive_gala` (1), `passive_multimedia_concert` (1), `passive_concert_show` (1), `passive_magic_show_without_proven_participation` (1), `ordinary_concert_show` (1), `ordinary_party` (1), `spectator_sport` (1)

### Замки, рыцари и средневековье

Определение: Средневековье, рыцарская культура, замки или эпоха викингов являются содержанием события; само старое место или фэнтези недостаточны.

| ID | Событие | Уверенность | Основание | BGE сейчас |
|---:|---|---|---|---|
| 698 | Древние воины Янтарного края | high | `early_medieval_archaeology` | да |
| 3742 | Звучащие сады | borderline | `medieval_music_program` | да |
| 5703 | Альбрехт Дюрер. Секретный код | borderline | `late_medieval_northern_renaissance` | да |
| 6191 | Фестиваль «Большой Кауп» | high | `viking_living_history` | да |
| 6652 | Руслан и Людмила. На стыке времён | borderline | `castle_fantasy_with_knights` | да |
| 6796 | 🎵 Концерт дуэта «Древо» | borderline | `historical_music_at_viking_settlement` | да |
| 7055 | Выставка «Новая жизнь: до и после» | high | `medieval_fortress_restoration` | да |
| 7131 | Средневековый пир и Рыцарский Турнир | high | `medieval_feast_and_tournament` | да |
| 7353 | 🎵 Концерт группы Spiritual Seasons | borderline | `concert_inside_living_history_program` | да |
| 7373 | ⛵ Путешествие на драккаре викингов «Рагна» | high | `reconstructed_viking_boat_occurrence` | да |
| 7374 | ⛵ Путешествие на драккаре викингов «Рагна» | high | `participatory_viking_boat_occurrence` | да |

Границы hard-negative:
`first_world_war` (2), `ordinary_concert_in_historic_venue` (2), `postwar_history` (1), `sports_biography` (1), `broad_history_of_music_education` (1), `postwar_archive_history` (1), `generic_history_of_inventions` (1), `band_calls_itself_order` (1), `postwar_city_history` (1), `generic_commemoration` (1), `modern_military_history` (1), `medieval_plot_in_passive_ballet` (1), `unrelated_children_show_at_castle` (1), `generic_ruins_art_exhibition` (1), `fantasy_only` (1), `fantasy_only_occurrence` (1), `single_medieval_mention_in_art_lecture` (1), `modern_city_literature` (1), `first_world_war_commemoration` (1), `ordinary_ballet` (1), `ordinary_organ_concert` (1), `regional_anniversary` (1), `twentieth_century_history` (1)

### Детская аудитория

Определение: Ребёнок/школьник является прямым адресатом и участником программы; детская тема, детские авторы или допустимый возраст сами по себе недостаточны.

| ID | Событие | Уверенность | Основание | BGE сейчас |
|---:|---|---|---|---|
| 5556 | 🎤 Концерт ANJELINA, МАЛИ и ДЖЕДЖЕ | borderline | `explicit_children_and_youth_audience` | нет |
| 6689 | 🎭 Спектакль «Незнайка и его друзья» | high | `children_theatre` | нет |
| 6822 | Спектакль «Новые приключения Буратино» | high | `children_and_parents_invited` | нет |
| 6823 | Спектакль «Виват, Мюнхгаузен!» | high | `children_repertoire` | нет |
| 6898 | День крабовой палочки с VICI | high | `explicit_children_zone` | да |
| 7054 | Щелкунчик | high | `children_puppet_show` | нет |
| 7112 | Спектакль «Красная шапочка» | high | `small_viewers_participate` | нет |
| 7117 | Бременские музыканты | borderline | `family_musical` | нет |
| 7134 | 🪄 Детское шоу фокусов Александра Прахова | high | `children_family_magic_show` | нет |
| 7172 | 🎭 Мюзикл «Алиса в Стране Чудес» | high | `children_family_musical` | нет |
| 7176 | 🎭 «Три кота: День Варенья» | high | `children_franchise_show` | нет |
| 7190 | Спектакль «Репка» | high | `children_puppet_show` | да |
| 7211 | Улитка и Кит | high | `children_puppet_show` | нет |
| 7228 | Шоу «Крутые фокусы» блогера-иллюзиониста Влада Алмазова | borderline | `family_magic_show` | нет |
| 7256 | Психолого-педагогическая программа «Мои таланты» | high | `child_development_program` | да |
| 7290 | Столярный мастер-класс «Человек – пиктограмма (семейный)» | borderline | `child_parent_workshop` | да |
| 7326 | Мастер-класс «Городской скетчинг» | high | `family_workshop_with_children` | да |
| 7333 | 🧪 Фестиваль «Чудеса науки!» | high | `young_scientists_and_parents` | да |
| 7335 | Мастер-класс по точечной графике для детей | high | `children_art_workshop` | да |
| 7376 | 📚 Детский книжный клуб: «Гарри Поттер и Философский камень» | high | `children_book_club` | нет |

Границы hard-negative:
`generic_family_topic_concert` (5), `children_are_authors` (2), `age_rating_not_child_targeting` (2), `adult_standup` (2), `adult_women_only` (1), `children_authored_material_not_enough` (1), `close_people_not_children` (1), `adult_pole_dance` (1), `animal_art_not_child_audience` (1), `children_are_authors_not_visitors` (1), `family_topic_without_child_program` (1), `family_atmosphere_only` (1), `parents_and_benefits_not_child_program` (1), `sixteen_plus_workshop` (1), `parents_and_couples_only` (1), `women_only` (1), `youth_artist_not_explicit_child_program` (1), `adult_food_festival` (1)

### Семейная аудитория

Определение: Взрослый и ребёнок прямо приглашены участвовать вместе; слово «семейный», родители без детей или детская программа без совместного формата недостаточны.

| ID | Событие | Уверенность | Основание | BGE сейчас |
|---:|---|---|---|---|
| 4648 | Они были первыми | borderline | `explicit_for_families` | да |
| 6822 | Спектакль «Новые приключения Буратино» | high | `children_and_parents_invited` | да |
| 6824 | 🎭 «Бременские музыканты» | high | `explicit_family_musical` | да |
| 6865 | Триатлон поколений | high | `intergenerational_family_team` | да |
| 6898 | День крабовой палочки с VICI | high | `family_day_with_children_zone` | да |
| 6924 | Улитка и кит | high | `explicit_whole_family_invitation` | да |
| 7102 | Мастер-класс по созданию гига-мозаики | high | `parent_child_collective_mosaic` | да |
| 7117 | Бременские музыканты | high | `explicit_family_musical` | да |
| 7134 | 🪄 Детское шоу фокусов Александра Прахова | high | `explicit_family_show` | да |
| 7176 | 🎭 «Три кота: День Варенья» | borderline | `children_show_for_family_visit` | да |
| 7228 | Шоу «Крутые фокусы» блогера-иллюзиониста Влада Алмазова | high | `explicit_whole_family_show` | да |
| 7258 | Бесплатная игротека в МЕГЕ | borderline | `family_game_library` | да |
| 7283 | День физкультурника | borderline | `broad_family_sport_day` | да |
| 7290 | Столярный мастер-класс «Человек – пиктограмма (семейный)» | high | `parent_child_joint_woodwork` | да |
| 7293 | Кино под звёздами: Ведьмина служба доставки | borderline | `family_open_air_movie` | да |
| 7307 | Калининград | high | `family_laser_tag_tournament` | да |
| 7326 | Мастер-класс «Городской скетчинг» | high | `explicit_whole_family_workshop` | да |
| 7333 | 🧪 Фестиваль «Чудеса науки!» | high | `young_scientists_and_parents` | да |
| 7354 | 🥘 Пловный пикник в Агропарке | high | `explicit_whole_family_day` | да |
| 7372 | 🧘‍♀️ «Семейный коврик» | high | `parent_child_joint_yoga` | да |

Границы hard-negative:
`generic_family_topic_concert` (5), `child_show_without_joint_format` (3), `generic_family_topic_exhibition` (2), `child_program_without_joint_adult_activity` (2), `children_are_authors` (2), `adult_women_only` (1), `with_close_people_not_family_format` (1), `children_and_youth_not_joint_family` (1), `adult_practice` (1), `children_are_authors_not_visitors` (1), `child_only_development_program` (1), `family_atmosphere_only` (1), `parents_only_information_meeting` (1), `children_only_workshop` (1), `parents_and_couples_without_children` (1), `children_only_book_club` (1)

## 5. Критические замечания к самому seed

### 5.1. «Наука» пока не доказала самостоятельный продуктовый supply

Из 16 positives только 6 помечены `high`; остальные сознательно пограничны:
архивная гуманитаристика, технология фотографии, restoration engineering,
документальные фильмы о науке и экологическая практика. Аналитик должен решить,
является ли страница:

- только участием в научном методе/работой с данными;
- также evidence-led гуманитарным знанием;
- или более широкой «Наукой и исследованиями».

Нельзя добирать минимум обычными научпоп-лекциями или шоу с жидким азотом. Если
после review остаётся меньше 15 независимых positives, корректное решение —
оставить страницу в shadow до накопления supply, а не ослаблять значение.

### 5.2. «Сильные впечатления» имеет риск стать вторым «Необычным»

Предложенная граница требует собственного действия посетителя либо доказанного
иммерсивного/эмоционально интенсивного опыта. Обычный концерт со свечами,
пиротехникой, мультимедиа или рекламной фразой остаётся hard negative. Спорная
часть seed — стандартные craft/wellbeing practices: дерево, семейная йога,
психологическая рефлексия. Их следует оставить только если продукт действительно
обещает личное преобразующее участие, а не просто «что-то поделать».

### 5.3. Medieval имеет 11 occurrence rows, но меньше независимых families

`7373` и `7374` — две даты одного путешествия на драккаре. Минимум 15 positives
не достигнут даже recall-oriented seed. Старый замок сам по себе не включает
детский спектакль; средневековый сюжет пассивного балета и фэнтези «Робин Гуд»
тоже заданы hard negatives. Аналитику нужно решить: выпускать тонкую качественную
полку внутри hub, ждать supply или расширять продуктовую формулировку до
«История в замках». Последнее является сменой продукта, а не настройкой модели.

### 5.4. Kids и family — два extraction heads, но один пользовательский intent

Direct kids означает, что ребёнок адресат. Family означает совместную программу
ребёнка и взрослого. Одна публичная `/detyam/` может объединить их без
дублирования occurrence family, но evaluation должен сохранять две границы.
Возраст `6+`, детская тема, работы детей, «семейная атмосфера», встреча только
для родителей и обычный концерт с FAMILY topic не являются доказательством.

## 6. Гости: предыдущие production counts были названы неправильно

`foreign=1` и `russia=0` — это число уже материализованных
`people_appearances`, а не фактический supply. Exporter требовал `confirmed`
appearance и exact `origin_scope`, но candidate routing запускал people pass
почти только для topic `PERSONALITIES` или уже существующего decision. Большая
часть концертов и гастрольных спектаклей не проверялась вообще.

### Уже подтверждаемый иностранный supply

| ID | Событие | Evidence в production DB |
|---:|---|---|
| 7150 | Хироко Иноуэ | «орган, Япония» |
| 7220 | Романтические итальянские теноры | страна/идентичность прямо в названии source row |
| 7227 | Огни Анатолии | «Турецкое танцевальное шоу» |
| 7319 | Бенджамин Ригетти | «представляет Швейцарию» |
| 7365 | Мелоди Мишель | «орган, Франция» |
| 7369 | Дженс Корндорфер | «США/Германия» |

`PUPO`, event `6272`, действительно зарубежный гость, но дата была 29 июля и к
batch 1 августа событие уже не относилось к будущей выдаче.

### Российский supply

Нулевым он быть не может. Только Янтарь-холл содержит кандидатов Александр
Маршал, Ирина Дубцова, Вячеслав Бутусов, Гарик Сукачёв, Наташа Королёва,
Лолита, Стас Пьеха, Алексей Чумаков, «Комиссар», «Нэнси», «Бутырка», «КняZz»,
«Пикник», «Любэ» и другие. Театр эстрады добавляет «Цветы», «Белый орёл»,
«Самоцветы», «Земляне», Александра Устюгова, Дмитрия Дюжева и гастрольные
составы.

Это пока candidates, не новый точный count: appearance часто подтверждается
названием, но source text не всегда сообщает nonlocal origin. Простое решение:

1. high-recall candidate discovery по concert/performance/show, named
   performers/groups и уже имеющимся BGE/person signals;
2. маленький cached people adjudicator подтверждает appearance по DB source;
3. origin сначала берётся из DB source; если отсутствует — из отдельного
   provenance-aware performer entity resolver/registry, а не угадывается по
   имени и не требует повторного сканирования сайта площадки;
4. unknown не публикуется, но остаётся в review queue.

Аналитик должен оценить, допустим ли verified entity registry как origin truth
и какие источники/TTL/manual override обязательны.

## 7. Спектакли и связанные даты

Число `71` в batch — occurrence rows, а не уникальные постановки. Read-only
проверка mutual `linked_event_ids` дала:

| Метрика | Значение |
|---|---:|
| performance occurrence IDs | 71 |
| mutual explicit families внутри batch | 61 |
| multi-occurrence families | 9 |
| обнаруженная asymmetric связь | `7054 -> 7237` |

Unified UI resolver объединяет только взаимные explicit links. Совпадение
заголовка/места не создаёт identity. Это защищает от ложного merge, но входные
данные ещё не полностью корректны:

- `7054 «Щелкунчик»` односторонне ссылается на `7237`; UI обязан игнорировать;
- `7113 «Девчата»` взаимно связан с `7114`, но `7114` ошибочно имеет
  `event_type=кинопоказ` и не попал в 71;
- очевидные постановки/балеты с primary type `концерт` тоже пропущены exact
  фильтром;
- ссылки на прошлые occurrences допустимы, если public presentation их
  фильтрует, а canonical event pages сохраняются.

До Astro нужны gates: reciprocal link integrity, no dangling public links, no
duplicate slot, включение eligible sibling в collection projection и отдельный
`occurrence_count/family_count`. На `/spektakli/` применяется `per-family`, на
календарном контексте — `per-date`, на detail — selector других дат.

## 8. Архитектурные ограничения для критики

Сохраняются как исходные ограничения, а не как темы для расширения scope:

- один existing StaticSiteBuilder после 15 минут тишины Smart Update;
- один Fly SQLite snapshot на всю сборку;
- shared BGE кодирует только изменившиеся documents; несколько heads используют
  одну матрицу;
- factual admission/audience/people truth принадлежит Smart Update/DB, BGE —
  recall/disagreement detector;
- Astro потребляет принятые IDs и не переопределяет membership из prose;
- никакого LLM/BGE при открытии страницы;
- никакого дополнительного Supabase core egress;
- киноисточники и кинотеатры не добавляются и не меняются;
- фестивальные pages/extraction не входят в этот проект.

## 9. Предлагаемые release gates после внешнего review

1. Owner/editor применяет `keep/drop/move/ambiguous` к provisional seed.
2. Occurrence siblings дедуплицируются до family-weighted evaluation.
3. На exact raw scores строятся recall, hard-negative FPR, precision@page-size и
   threshold curve; prototype и threshold hashes фиксируются вместе.
4. `science`/`strong_impressions`: минимум 15 принятых positive families, 20
   hard negatives, recall >= 0.80, hard-negative FPR <= 0.05.
5. `medieval`: не публиковать при недостаточном family supply даже при хорошей
   модели.
6. Audience BGE обязан иметь recall >= 0.95, но publication truth остаётся
   grounded `audience_decision`; BGE-only candidate идёт в adjudication.
7. Guest pages остаются blocked до расширенного current-catalog people backfill.
8. `/spektakli/` blocked до type-coverage и occurrence-integrity отчёта.
9. Каждый label включается отдельно; blocked label отсутствует в navigation и
   sitemap, last-good public tree не заменяется пустым.

## 10. Файлы, которые аналитику нужно прочитать

- [`podborki.md`](./podborki.md) — неизменённые требования владельца;
- [`podborki-to-be.md`](./podborki-to-be.md) — полный продуктовый/технический проект;
- [`static_collections_gold_v1.json`](../../../tests/fixtures/static_collections_gold_v1.json) — все provisional positives и hard negatives с evidence;
- [`static_collection_prototypes.v1.json`](../../../site/scripts/static_collection_prototypes.v1.json) — текущие prototype texts;
- [`static_collection_policy.v1.json`](../../../site/scripts/static_collection_policy.v1.json) — thresholds/gates;
- [`static_collection_export.py`](../../../site/scripts/static_collection_export.py) — exact и semantic projection;
- [`backfill_static_collection_facts.py`](../../../scripts/backfill_static_collection_facts.py) — текущий candidate routing;
- [`eventOccurrences.ts`](../../../site/src/lib/eventOccurrences.ts) — family presentation contract;
- [`release-plan.md`](./release-plan.md) — release governance и handoff в будущую Astro-ветку.
