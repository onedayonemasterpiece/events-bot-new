# Phrase bank v1 — event-comment-feedback

Status: fixed public phrase library for MVP-0 design. LLM verifiers must approve/reject/downgrade these ids only; they must not generate new `public_sentence` values.

Phrase bank version: `event-comment-feedback-phrase-bank-v1`.

## Policy defaults

- Public wording always starts from “В комментариях …”; never “Все”, “зрители”, “лучшее”, “точно стоит”.
- `vector_only_allowed=true` means publication is possible only after strict vector thresholds, evidence thresholds and conflict checks pass.
- `requires_llm_verification=true` means a candidate group must pass the group-level verifier or manual review before new publication.
- Hard negatives are part of semantic matching, not keyword rules. They are embedded as negative/downgrade prototypes.

## Publishable phrases

### 1. `anticipation_high`
- **Category:** Ожидание и желание прийти
- **signal_type:** `anticipation`
- **tone/icon/risk:** `positive` / `smile_green` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях отмечают, что это очень ожидаемое мероприятие”
- **min evidence:** `min_evidence_count=4`, `min_unique_authors=3`
- **Positive prototypes:** “очень ждали это событие”; “наконец-то будет”; “давно ждали этот концерт”; “ура, наконец привозят”; “не могу дождаться”
- **Hard negatives:** “не понимаю, чего все ждут”; “не сказал бы, что это ожидаемое событие”

### 2. `anticipation_long_wait`
- **Category:** Ожидание и желание прийти
- **signal_type:** `long_wait`
- **tone/icon/risk:** `positive` / `smile_green` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях отмечают, что событие давно ждали”
- **min evidence:** `min_evidence_count=3`, `min_unique_authors=2`
- **Positive prototypes:** “ждали весь год”; “давно хотели попасть”; “столько времени ждали”; “наконец снова проходит”; “давно просили такое событие”
- **Hard negatives:** “раньше ждали, а теперь уже неинтересно”; “не ждал и не пойду”

### 3. `excitement_celebratory`
- **Category:** Ожидание и желание прийти
- **signal_type:** `celebratory_reaction`
- **tone/icon/risk:** `positive` / `smile_green` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях заметна радостная реакция на анонс события”
- **min evidence:** `min_evidence_count=5`, `min_unique_authors=4`
- **Positive prototypes:** “ура”; “круто”; “вот это новость”; “супер, наконец-то”; “очень рада”; “отличная новость”
- **Hard negatives:** “ура, опять перенос”; “круто, конечно, но нет”

### 4. `intent_to_attend`
- **Category:** Ожидание и желание прийти
- **signal_type:** `attendance_intent`
- **tone/icon/risk:** `positive` / `smile_green` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях отмечают готовность прийти на событие”
- **min evidence:** `min_evidence_count=3`, `min_unique_authors=2`
- **Positive prototypes:** “обязательно пойду”; “идём”; “планируем прийти”; “надо сходить”; “точно будем”; “хочу попасть”
- **Hard negatives:** “не пойду”; “хотел пойти, но не получится”

### 5. `already_planning_visit`
- **Category:** Ожидание и желание прийти
- **signal_type:** `planning_visit`
- **tone/icon/risk:** `positive` / `smile_green` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях пишут, что уже планируют посетить событие”
- **min evidence:** `min_evidence_count=3`, `min_unique_authors=2`
- **Positive prototypes:** “уже запланировали”; “занесли в календарь”; “уже договорились идти”; “собираемся всей компанией”; “билеты взяли, идём”
- **Hard negatives:** “пока не планируем”; “думали пойти, но передумали”

### 6. `group_visit`
- **Category:** Ожидание и желание прийти
- **signal_type:** `group_visit`
- **tone/icon/risk:** `positive` / `smile_green` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях отмечают интерес к посещению компанией”
- **min evidence:** `min_evidence_count=2`, `min_unique_authors=2`
- **Positive prototypes:** “идём компанией”; “позову друзей”; “надо собрать компанию”; “с кем пойдём?”; “отметил друзей, чтобы пойти вместе”
- **Hard negatives:** “компания не собирается”; “не с кем идти”

### 7. `family_visit`
- **Category:** Ожидание и желание прийти
- **signal_type:** `family_visit`
- **tone/icon/risk:** `positive` / `smile_green` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях обсуждают посещение события с семьёй”
- **min evidence:** `min_evidence_count=2`, `min_unique_authors=2`
- **Positive prototypes:** “пойдём всей семьёй”; “можно с детьми?”; “хочу привести ребёнка”; “подойдёт для семейного выходного”; “с семьёй было бы интересно”
- **Hard negatives:** “не для детей”; “с семьёй сюда точно не стоит”

### 8. `repeat_visit_intent`
- **Category:** Ожидание и желание прийти
- **signal_type:** `repeat_visit`
- **tone/icon/risk:** `positive` / `smile_green` / `medium`
- **Policy:** `verifier required` (`vector_only_allowed=false`, `requires_llm_verification=true`)
- **public_sentence:** “В комментариях отмечают желание прийти на событие снова”
- **min evidence:** `min_evidence_count=2`, `min_unique_authors=2`
- **Positive prototypes:** “были в прошлый раз, хотим снова”; “обязательно придём ещё раз”; “в прошлый раз понравилось”; “после прошлого события хочется повторить”; “ходили раньше, снова пойдём”
- **Hard negatives:** “в прошлый раз не понравилось”; “больше не пойдём”

### 9. `artist_loved`
- **Category:** Артисты, участники, программа, тема
- **signal_type:** `artist_loved`
- **tone/icon/risk:** `positive` / `smile_green` / `medium`
- **Policy:** `verifier required` (`vector_only_allowed=false`, `requires_llm_verification=true`)
- **public_sentence:** “В комментариях отмечают, что артист особенно любим аудиторией”
- **min evidence:** `min_evidence_count=3`, `min_unique_authors=3`
- **Positive prototypes:** “обожаю этого артиста”; “ради него и пойду”; “любимый исполнитель”; “давно хотела услышать его вживую”; “у него прекрасные выступления”
- **Hard negatives:** “не люблю этого артиста”; “артист разочаровал”; “ради него точно не пойду”

### 10. `actor_loved`
- **Category:** Артисты, участники, программа, тема
- **signal_type:** `actor_loved`
- **tone/icon/risk:** `positive` / `smile_green` / `medium`
- **Policy:** `verifier required` (`vector_only_allowed=false`, `requires_llm_verification=true`)
- **public_sentence:** “В комментариях отмечают, что актёр особенно любим аудиторией”
- **min evidence:** `min_evidence_count=3`, `min_unique_authors=3`
- **Positive prototypes:** “любимый актёр”; “ради этого актёра стоит идти”; “он прекрасно играет”; “давно хотели увидеть его на сцене”; “актёрский состав отличный”
- **Hard negatives:** “не нравится этот актёр”; “актёрская игра слабая”

### 11. `speaker_interest`
- **Category:** Артисты, участники, программа, тема
- **signal_type:** `speaker_interest`
- **tone/icon/risk:** `positive` / `smile_green` / `medium`
- **Policy:** `verifier required` (`vector_only_allowed=false`, `requires_llm_verification=true`)
- **public_sentence:** “В комментариях отмечают интерес к выступающему”
- **min evidence:** `min_evidence_count=2`, `min_unique_authors=2`
- **Positive prototypes:** “интересный лектор”; “хочу послушать этого спикера”; “давно слежу за его лекциями”; “ради выступающего стоит прийти”; “тема и спикер очень подходят”
- **Hard negatives:** “спикер слабый”; “не доверяю этому выступающему”

### 12. `lineup_interest`
- **Category:** Артисты, участники, программа, тема
- **signal_type:** `program_interest`
- **tone/icon/risk:** `positive` / `smile_green` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях отмечают интерес к программе события”
- **min evidence:** `min_evidence_count=3`, `min_unique_authors=2`
- **Positive prototypes:** “программа интересная”; “классный состав участников”; “интересно, что будет в программе”; “много всего заявлено”; “хорошая программа”
- **Hard negatives:** “программа слабая”; “непонятно, что вообще будет”

### 13. `topic_relevant`
- **Category:** Артисты, участники, программа, тема
- **signal_type:** `topic_relevant`
- **tone/icon/risk:** `positive` / `smile_green` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях отмечают актуальность темы события”
- **min evidence:** `min_evidence_count=3`, `min_unique_authors=2`
- **Positive prototypes:** “очень актуальная тема”; “важно об этом говорить”; “тема сейчас нужная”; “давно не хватало такого разговора”; “полезная тема”
- **Hard negatives:** “тема неактуальна”; “зачем это обсуждать”

### 14. `format_interest`
- **Category:** Артисты, участники, программа, тема
- **signal_type:** `format_interest`
- **tone/icon/risk:** `positive` / `smile_green` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях отмечают интерес к формату события”
- **min evidence:** `min_evidence_count=3`, `min_unique_authors=2`
- **Positive prototypes:** “интересный формат”; “такой формат редко бывает”; “люблю такие встречи”; “здорово, что это будет в таком формате”; “необычный формат”
- **Hard negatives:** “формат неудачный”; “не люблю такой формат”

### 15. `rare_format_interest`
- **Category:** Артисты, участники, программа, тема
- **signal_type:** `rare_format`
- **tone/icon/risk:** `positive` / `smile_green` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях отмечают, что формат события воспринимают как необычный”
- **min evidence:** `min_evidence_count=3`, `min_unique_authors=2`
- **Positive prototypes:** “редкий формат”; “такого давно не было”; “необычно”; “интересно попробовать”; “редко у нас такое проводят”
- **Hard negatives:** “ничего необычного”; “обычное мероприятие”

### 16. `past_event_positive`
- **Category:** Прошлый опыт, организаторы, атмосфера, площадка
- **signal_type:** `past_experience_positive`
- **tone/icon/risk:** `positive` / `smile_green` / `medium`
- **Policy:** `verifier required` (`vector_only_allowed=false`, `requires_llm_verification=true`)
- **public_sentence:** “В комментариях отмечают, что прошлое событие оставило хорошее впечатление”
- **min evidence:** `min_evidence_count=2`, `min_unique_authors=2`
- **Positive prototypes:** “в прошлый раз было отлично”; “были в прошлом году, понравилось”; “прошлое мероприятие было очень тёплым”; “после прошлого раза хочется снова”; “в прошлый раз всё было на уровне”
- **Hard negatives:** “в прошлый раз было плохо”; “после прошлого раза больше не хочется”

### 17. `organizer_trust`
- **Category:** Прошлый опыт, организаторы, атмосфера, площадка
- **signal_type:** `organizer_trust`
- **tone/icon/risk:** `positive` / `smile_green` / `medium`
- **Policy:** `verifier required` (`vector_only_allowed=false`, `requires_llm_verification=true`)
- **public_sentence:** “В комментариях отмечают доверие к организаторам”
- **min evidence:** `min_evidence_count=3`, `min_unique_authors=2`
- **Positive prototypes:** “организаторам можно доверять”; “они всегда делают хорошо”; “у этих организаторов всё на уровне”; “организаторы надёжные”; “они умеют делать события”
- **Hard negatives:** “организаторам не доверяю”; “в прошлый раз организация подвела”

### 18. `organizer_quality`
- **Category:** Прошлый опыт, организаторы, атмосфера, площадка
- **signal_type:** `organizer_quality`
- **tone/icon/risk:** `positive` / `smile_green` / `medium`
- **Policy:** `verifier required` (`vector_only_allowed=false`, `requires_llm_verification=true`)
- **public_sentence:** “В комментариях отмечают высокий уровень работы организаторов”
- **min evidence:** `min_evidence_count=3`, `min_unique_authors=2`
- **Positive prototypes:** “организация была отличная”; “всё было хорошо организовано”; “организаторы молодцы”; “уровень организации высокий”; “у них всегда всё продумано”
- **Hard negatives:** “организация была ужасная”; “очереди и хаос”; “организаторы не справились”

### 19. `atmosphere_positive`
- **Category:** Прошлый опыт, организаторы, атмосфера, площадка
- **signal_type:** `atmosphere`
- **tone/icon/risk:** `positive` / `smile_green` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях отмечают приятную атмосферу события”
- **min evidence:** `min_evidence_count=3`, `min_unique_authors=2`
- **Positive prototypes:** “там всегда хорошая атмосфера”; “было очень душевно”; “уютная атмосфера”; “приятная публика”; “атмосферное событие”
- **Hard negatives:** “атмосфера была неприятная”; “было неуютно”

### 20. `venue_positive`
- **Category:** Прошлый опыт, организаторы, атмосфера, площадка
- **signal_type:** `venue_positive`
- **tone/icon/risk:** `positive` / `smile_green` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях отмечают, что площадка хорошо подходит для события”
- **min evidence:** `min_evidence_count=3`, `min_unique_authors=2`
- **Positive prototypes:** “отличная площадка”; “там удобно”; “место подходит идеально”; “люблю эту площадку”; “хорошее пространство для такого события”
- **Hard negatives:** “площадка неудобная”; “место не подходит”

### 21. `venue_iconic`
- **Category:** Прошлый опыт, организаторы, атмосфера, площадка
- **signal_type:** `venue_interest`
- **tone/icon/risk:** `positive` / `smile_green` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях отмечают интерес к самой площадке”
- **min evidence:** `min_evidence_count=3`, `min_unique_authors=2`
- **Positive prototypes:** “ради площадки тоже хочется сходить”; “любимое место”; “там всегда красиво”; “давно хотела попасть в это пространство”; “площадка сама по себе интересная”
- **Hard negatives:** “не люблю эту площадку”; “место неинтересное”

### 22. `ticket_interest_high`
- **Category:** Билеты, спрос, sold-out, регистрация
- **signal_type:** `ticket_interest`
- **tone/icon/risk:** `positive` / `smile_green` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях отмечают высокий интерес к билетам и свободным местам”
- **min evidence:** `min_evidence_count=4`, `min_unique_authors=3`
- **Positive prototypes:** “где купить билеты”; “билеты ещё есть?”; “как попасть”; “места ещё остались?”; “хочу взять билеты”; “успеем купить?”
- **Hard negatives:** “продам билеты”; “ищу кому продать билет”; “билеты дорогие, не пойду”

### 23. `ticket_availability_question`
- **Category:** Билеты, спрос, sold-out, регистрация
- **signal_type:** `ticket_availability_question`
- **tone/icon/risk:** `neutral` / `neutral_gray` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях спрашивают о наличии билетов”
- **min evidence:** `min_evidence_count=2`, `min_unique_authors=2`
- **Positive prototypes:** “билеты ещё есть?”; “есть свободные места?”; “как узнать, остались ли билеты?”; “ещё можно купить?”; “а места остались?”
- **Hard negatives:** “продам билет”; “купил билет”

### 24. `registration_interest`
- **Category:** Билеты, спрос, sold-out, регистрация
- **signal_type:** `registration_interest`
- **tone/icon/risk:** `neutral` / `neutral_gray` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях уточняют условия регистрации”
- **min evidence:** `min_evidence_count=2`, `min_unique_authors=2`
- **Positive prototypes:** “как зарегистрироваться?”; “нужна регистрация?”; “где форма регистрации?”; “регистрация ещё открыта?”; “как записаться?”
- **Hard negatives:** “зарегистрировался”; “регистрация не нужна”

### 25. `extra_places_question`
- **Category:** Билеты, спрос, sold-out, регистрация
- **signal_type:** `extra_places_question`
- **tone/icon/risk:** `neutral` / `neutral_gray` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях спрашивают о дополнительных местах”
- **min evidence:** `min_evidence_count=2`, `min_unique_authors=2`
- **Positive prototypes:** “будут ещё места?”; “добавят места?”; “можно ли попасть в лист ожидания?”; “освободятся места?”; “а если кто-то откажется?”
- **Hard negatives:** “мест полно”; “места не нужны”

### 26. `extra_date_request`
- **Category:** Билеты, спрос, sold-out, регистрация
- **signal_type:** `extra_date_request`
- **tone/icon/risk:** `positive` / `smile_green` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях просят добавить ещё одну дату события”
- **min evidence:** `min_evidence_count=2`, `min_unique_authors=2`
- **Positive prototypes:** “добавьте ещё дату”; “будет повтор?”; “сделайте ещё один показ”; “хотим дополнительный день”; “а будет ещё раз?”
- **Hard negatives:** “не надо повторять”; “вторая дата лишняя”

### 27. `sold_out_discussion`
- **Category:** Билеты, спрос, sold-out, регистрация
- **signal_type:** `sold_out_discussion`
- **tone/icon/risk:** `neutral` / `neutral_gray` / `medium`
- **Policy:** `verifier required` (`vector_only_allowed=false`, `requires_llm_verification=true`)
- **public_sentence:** “В комментариях обсуждают доступность билетов”
- **min evidence:** `min_evidence_count=3`, `min_unique_authors=2`
- **Positive prototypes:** “билеты закончились?”; “пишут, что билетов нет”; “не понятно, есть ли места”; “кто знает, остались билеты?”; “всё разобрали или ещё можно купить?”
- **Hard negatives:** “продам два билета”; “билеты куплены, всё хорошо”

### 28. `sold_out_disappointment`
- **Category:** Билеты, спрос, sold-out, регистрация
- **signal_type:** `sold_out_disappointment`
- **tone/icon/risk:** `concern` / `sad_red` / `high`
- **Policy:** `verifier required` (`vector_only_allowed=false`, `requires_llm_verification=true`)
- **public_sentence:** “В комментариях расстраиваются, что билеты быстро закончились”
- **min evidence:** `min_evidence_count=3`, `min_unique_authors=3`
- **Positive prototypes:** “нет билетов, очень жаль”; “не успели купить”; “всё разобрали”; “опять билетов нет”; “расстроилась, что билеты закончились”; “хотели пойти, но мест уже нет”
- **Hard negatives:** “билеты ещё есть?”; “продам два билета”; “билеты купил”; “нет, билеты не закончились”

### 29. `high_demand_from_ticket_friction`
- **Category:** Билеты, спрос, sold-out, регистрация
- **signal_type:** `ticket_demand_friction`
- **tone/icon/risk:** `positive` / `smile_green` / `medium`
- **Policy:** `verifier required` (`vector_only_allowed=false`, `requires_llm_verification=true`)
- **public_sentence:** “В комментариях видно, что событие вызвало высокий интерес к билетам”
- **min evidence:** `min_evidence_count=4`, `min_unique_authors=3`
- **Positive prototypes:** “много кто спрашивает про билеты”; “места быстро разбирают”; “не успели купить”; “очередь за билетами”; “билеты расходятся быстро”
- **Hard negatives:** “билеты никому не нужны”; “много свободных мест”; “продам билет”

### 30. `waitlist_interest`
- **Category:** Билеты, спрос, sold-out, регистрация
- **signal_type:** `waitlist_interest`
- **tone/icon/risk:** `neutral` / `neutral_gray` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях интересуются листом ожидания”
- **min evidence:** `min_evidence_count=2`, `min_unique_authors=2`
- **Positive prototypes:** “есть лист ожидания?”; “можно в резерв?”; “как попасть, если мест нет?”; “запишите в ожидание”; “если освободится место, сообщите”
- **Hard negatives:** “не хочу ждать”; “лист ожидания не нужен”

### 31. `time_questions`
- **Category:** Практические вопросы
- **signal_type:** `time_questions`
- **tone/icon/risk:** `neutral` / `neutral_gray` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях уточняют время начала и входа”
- **min evidence:** `min_evidence_count=2`, `min_unique_authors=2`
- **Positive prototypes:** “во сколько начало?”; “когда вход?”; “за сколько приходить?”; “во сколько запуск?”; “сколько длится?”
- **Hard negatives:** “время известно”; “неважно во сколько”

### 32. `duration_questions`
- **Category:** Практические вопросы
- **signal_type:** `duration_questions`
- **tone/icon/risk:** `neutral` / `neutral_gray` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях спрашивают о длительности события”
- **min evidence:** `min_evidence_count=2`, `min_unique_authors=2`
- **Positive prototypes:** “сколько длится?”; “до скольки будет?”; “какая продолжительность?”; “долго идёт?”; “успеем после работы?”
- **Hard negatives:** “длительность указана”; “всё равно сколько длится”

### 33. `age_limit_questions`
- **Category:** Практические вопросы
- **signal_type:** `age_limit_questions`
- **tone/icon/risk:** `neutral` / `neutral_gray` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях спрашивают о возрастных ограничениях”
- **min evidence:** `min_evidence_count=2`, `min_unique_authors=2`
- **Positive prototypes:** “какое возрастное ограничение?”; “18+?”; “можно детям?”; “с какого возраста?”; “подойдёт подростку?”
- **Hard negatives:** “возраст не важен”; “не про возраст”

### 34. `children_questions`
- **Category:** Практические вопросы
- **signal_type:** `children_questions`
- **tone/icon/risk:** `neutral` / `neutral_gray` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях уточняют, можно ли прийти с детьми”
- **min evidence:** `min_evidence_count=2`, `min_unique_authors=2`
- **Positive prototypes:** “можно с ребёнком?”; “детям подойдёт?”; “с детьми пускают?”; “можно всей семьёй?”; “ребёнку будет интересно?”
- **Hard negatives:** “без детей”; “детей не берите”

### 35. `location_questions`
- **Category:** Практические вопросы
- **signal_type:** `location_questions`
- **tone/icon/risk:** `neutral` / `neutral_gray` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях уточняют, как добраться до площадки”
- **min evidence:** `min_evidence_count=2`, `min_unique_authors=2`
- **Positive prototypes:** “как добраться?”; “где это находится?”; “какой адрес?”; “откуда вход?”; “как пройти?”
- **Hard negatives:** “адрес понятен”; “не про место”

### 36. `parking_questions`
- **Category:** Практические вопросы
- **signal_type:** `parking_questions`
- **tone/icon/risk:** `neutral` / `neutral_gray` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях спрашивают о парковке и проезде”
- **min evidence:** `min_evidence_count=2`, `min_unique_authors=2`
- **Positive prototypes:** “где парковаться?”; “есть парковка?”; “как с проездом?”; “можно подъехать на машине?”; “где оставить машину?”
- **Hard negatives:** “без машины”; “парковка не нужна”

### 37. `accessibility_questions`
- **Category:** Практические вопросы
- **signal_type:** `accessibility_questions`
- **tone/icon/risk:** `neutral` / `neutral_gray` / `medium`
- **Policy:** `verifier required` (`vector_only_allowed=false`, `requires_llm_verification=true`)
- **public_sentence:** “В комментариях уточняют условия доступности площадки”
- **min evidence:** `min_evidence_count=1`, `min_unique_authors=1`
- **Positive prototypes:** “есть ли доступ для коляски?”; “можно ли попасть на инвалидной коляске?”; “есть ли лифт?”; “подходит ли для маломобильных?”; “есть пандус?”
- **Hard negatives:** “коляска детская”; “это не про доступность”

### 38. `payment_questions`
- **Category:** Практические вопросы
- **signal_type:** `payment_questions`
- **tone/icon/risk:** `neutral` / `neutral_gray` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях задают вопросы об оплате и льготах”
- **min evidence:** `min_evidence_count=2`, `min_unique_authors=2`
- **Positive prototypes:** “как оплатить?”; “есть скидки?”; “есть льготные билеты?”; “можно оплатить на месте?”; “какие способы оплаты?”
- **Hard negatives:** “оплатил”; “не нужна оплата”

### 39. `pushkin_card_questions`
- **Category:** Практические вопросы
- **signal_type:** `pushkin_card_questions`
- **tone/icon/risk:** `neutral` / `neutral_gray` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях спрашивают о возможности оплаты Пушкинской картой”
- **min evidence:** `min_evidence_count=2`, `min_unique_authors=2`
- **Positive prototypes:** “по Пушкинской карте можно?”; “Пушкинская карта действует?”; “можно оплатить Пушкинской?”; “есть по пушке?”; “пройдёт ли Пушкинская карта?”
- **Hard negatives:** “без Пушкинской карты”; “Пушкинская не нужна”

### 40. `online_recording_questions`
- **Category:** Практические вопросы
- **signal_type:** `online_recording_questions`
- **tone/icon/risk:** `neutral` / `neutral_gray` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях интересуются записью или онлайн-форматом события”
- **min evidence:** `min_evidence_count=2`, `min_unique_authors=2`
- **Positive prototypes:** “будет запись?”; “можно посмотреть онлайн?”; “будет трансляция?”; “запись выложат?”; “можно подключиться дистанционно?”
- **Hard negatives:** “только офлайн”; “запись не нужна”

### 41. `price_concern`
- **Category:** Сомнения, барьеры, фрустрация
- **signal_type:** `price_concern`
- **tone/icon/risk:** `concern` / `sad_red` / `medium`
- **Policy:** `verifier required` (`vector_only_allowed=false`, `requires_llm_verification=true`)
- **public_sentence:** “В комментариях отмечают, что стоимость билетов вызывает вопросы”
- **min evidence:** `min_evidence_count=3`, `min_unique_authors=2`
- **Positive prototypes:** “дорого”; “цена высокая”; “почему такие дорогие билеты?”; “дороговато”; “хотелось бы дешевле”
- **Hard negatives:** “цена нормальная”; “недорого”; “бесплатно”

### 42. `schedule_concern`
- **Category:** Сомнения, барьеры, фрустрация
- **signal_type:** `schedule_concern`
- **tone/icon/risk:** `concern` / `sad_red` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях отмечают, что время проведения подходит не всем”
- **min evidence:** `min_evidence_count=3`, `min_unique_authors=2`
- **Positive prototypes:** “жаль, что в это время”; “не успеваю после работы”; “слишком рано”; “слишком поздно”; “неудобное время”
- **Hard negatives:** “время удобное”; “как раз удобно”

### 43. `venue_concern`
- **Category:** Сомнения, барьеры, фрустрация
- **signal_type:** `venue_concern`
- **tone/icon/risk:** `concern` / `sad_red` / `medium`
- **Policy:** `verifier required` (`vector_only_allowed=false`, `requires_llm_verification=true`)
- **public_sentence:** “В комментариях обсуждают возможные неудобства площадки”
- **min evidence:** `min_evidence_count=3`, `min_unique_authors=2`
- **Positive prototypes:** “там неудобно”; “площадка тесная”; “далеко добираться”; “там плохо с парковкой”; “место не очень подходит”
- **Hard negatives:** “площадка удобная”; “там всё хорошо”

### 44. `queue_concern`
- **Category:** Сомнения, барьеры, фрустрация
- **signal_type:** `queue_concern`
- **tone/icon/risk:** `concern` / `sad_red` / `medium`
- **Policy:** `verifier required` (`vector_only_allowed=false`, `requires_llm_verification=true`)
- **public_sentence:** “В комментариях опасаются очередей или нехватки мест”
- **min evidence:** `min_evidence_count=3`, `min_unique_authors=2`
- **Positive prototypes:** “опять будут очереди”; “мест всем не хватит”; “будет толпа”; “придётся стоять в очереди”; “боюсь, что не попадём”
- **Hard negatives:** “очереди не было”; “мест много”

### 45. `weather_concern`
- **Category:** Сомнения, барьеры, фрустрация
- **signal_type:** `weather_concern`
- **tone/icon/risk:** `concern` / `sad_red` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях обсуждают погодные риски для события”
- **min evidence:** `min_evidence_count=2`, `min_unique_authors=2`
- **Positive prototypes:** “если будет дождь?”; “что будет при плохой погоде?”; “на улице может быть холодно”; “а если шторм?”; “погода может помешать”
- **Hard negatives:** “погода отличная”; “дождь не помешает”

### 46. `organization_concern`
- **Category:** Сомнения, барьеры, фрустрация
- **signal_type:** `organization_concern`
- **tone/icon/risk:** `concern` / `sad_red` / `high`
- **Policy:** `verifier required` (`vector_only_allowed=false`, `requires_llm_verification=true`)
- **public_sentence:** “В комментариях отмечают отдельные вопросы к организации события”
- **min evidence:** `min_evidence_count=3`, `min_unique_authors=2`
- **Positive prototypes:** “организация в прошлый раз подвела”; “непонятно, кто отвечает”; “в прошлый раз была путаница”; “плохо организовано”; “есть вопросы к организаторам”
- **Hard negatives:** “организация была отличная”; “организаторы молодцы”

### 47. `format_unclear`
- **Category:** Сомнения, барьеры, фрустрация
- **signal_type:** `format_unclear`
- **tone/icon/risk:** `neutral` / `neutral_gray` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях уточняют формат проведения события”
- **min evidence:** `min_evidence_count=2`, `min_unique_authors=2`
- **Positive prototypes:** “что за формат?”; “как это будет проходить?”; “это лекция или встреча?”; “будет интерактив?”; “непонятно, что именно будет”
- **Hard negatives:** “формат понятен”; “всё ясно”

### 48. `information_missing`
- **Category:** Сомнения, барьеры, фрустрация
- **signal_type:** `information_missing`
- **tone/icon/risk:** `neutral` / `neutral_gray` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях отмечают, что по событию не хватает практических деталей”
- **min evidence:** `min_evidence_count=3`, `min_unique_authors=2`
- **Positive prototypes:** “мало информации”; “где подробности?”; “не хватает описания”; “а программа где?”; “ничего не понятно по условиям”
- **Hard negatives:** “всё подробно написано”; “информации достаточно”

### 49. `accessibility_concern`
- **Category:** Сомнения, барьеры, фрустрация
- **signal_type:** `accessibility_concern`
- **tone/icon/risk:** `concern` / `sad_red` / `high`
- **Policy:** `verifier required` (`vector_only_allowed=false`, `requires_llm_verification=true`)
- **public_sentence:** “В комментариях обращают внимание на возможные сложности доступности площадки”
- **min evidence:** `min_evidence_count=1`, `min_unique_authors=1`
- **Positive prototypes:** “туда сложно попасть на коляске”; “нет лифта”; “непонятно, есть ли пандус”; “маломобильным будет сложно”; “доступность не описана”
- **Hard negatives:** “есть пандус”; “доступность хорошая”

### 50. `refund_exchange_questions`
- **Category:** Сомнения, барьеры, фрустрация
- **signal_type:** `refund_exchange_questions`
- **tone/icon/risk:** `neutral` / `neutral_gray` / `medium`
- **Policy:** `verifier required` (`vector_only_allowed=false`, `requires_llm_verification=true`)
- **public_sentence:** “В комментариях спрашивают об обмене или возврате билетов”
- **min evidence:** `min_evidence_count=2`, `min_unique_authors=2`
- **Positive prototypes:** “можно вернуть билет?”; “как обменять билет?”; “что делать, если не получается прийти?”; “возврат возможен?”; “можно переоформить?”
- **Hard negatives:** “ничего возвращать не надо”; “билет оставлю себе”

### 51. `local_community_interest`
- **Category:** Социальная значимость и локальный интерес
- **signal_type:** `local_community_interest`
- **tone/icon/risk:** `positive` / `smile_green` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях отмечают интерес события для местного сообщества”
- **min evidence:** `min_evidence_count=3`, `min_unique_authors=2`
- **Positive prototypes:** “для города это важно”; “такого у нас не хватает”; “хорошо, что это делают в Калининграде”; “важно для местных”; “нужное событие для сообщества”
- **Hard negatives:** “городу это не нужно”; “никому здесь не интересно”

### 52. `nostalgia_interest`
- **Category:** Социальная значимость и локальный интерес
- **signal_type:** `nostalgia_interest`
- **tone/icon/risk:** `positive` / `smile_green` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях отмечают ностальгический интерес к программе”
- **min evidence:** `min_evidence_count=3`, `min_unique_authors=2`
- **Positive prototypes:** “ностальгия”; “песни детства”; “вспомнить молодость”; “любимые старые песни”; “вернуться в то время”
- **Hard negatives:** “никакой ностальгии”; “устарело и неинтересно”

### 53. `kids_value_positive`
- **Category:** Социальная значимость и локальный интерес
- **signal_type:** `kids_value`
- **tone/icon/risk:** `positive` / `smile_green` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях отмечают, что событие может быть интересно детям”
- **min evidence:** `min_evidence_count=3`, `min_unique_authors=2`
- **Positive prototypes:** “детям будет интересно”; “отлично для ребёнка”; “хорошая детская программа”; “подойдёт школьникам”; “ребёнок хочет пойти”
- **Hard negatives:** “детям не подойдёт”; “не для детей”

### 54. `free_admission_interest`
- **Category:** Социальная значимость и локальный интерес
- **signal_type:** `free_admission_interest`
- **tone/icon/risk:** `positive` / `smile_green` / `low`
- **Policy:** `vector-only allowed` (`vector_only_allowed=true`, `requires_llm_verification=false`)
- **public_sentence:** “В комментариях отмечают интерес к бесплатному входу или льготам”
- **min evidence:** `min_evidence_count=3`, `min_unique_authors=2`
- **Positive prototypes:** “бесплатно?”; “здорово, что вход свободный”; “есть льготы?”; “бесплатные места есть?”; “приятно, что без оплаты”
- **Hard negatives:** “не бесплатно”; “льгот нет”

### 55. `social_value_positive`
- **Category:** Социальная значимость и локальный интерес
- **signal_type:** `social_value`
- **tone/icon/risk:** `positive` / `smile_green` / `medium`
- **Policy:** `verifier required` (`vector_only_allowed=false`, `requires_llm_verification=true`)
- **public_sentence:** “В комментариях отмечают социальную значимость события”
- **min evidence:** `min_evidence_count=3`, `min_unique_authors=2`
- **Positive prototypes:** “важная инициатива”; “нужное дело”; “хорошо, что поднимают эту тему”; “социально значимое событие”; “полезно для общества”
- **Hard negatives:** “не вижу социальной пользы”; “тема надуманная”

## Internal-only classes — do not publish directly

### 56. `spam_ticket_resale`
- **public_sentence:** `null`
- **publishable:** `false`
- **Purpose:** detect and exclude ticket resale/spam comments.
- **Positive prototypes:** “продам два билета”; “пишите в личку, есть билеты”; “куплю/продам билет”; “перепродам место”; “отдам билет дороже”
- **Action:** Exclude from evidence; may increment internal spam/resale diagnostics.

### 57. `off_topic_discussion`
- **public_sentence:** `null`
- **publishable:** `false`
- **Purpose:** detect comments unrelated to event.
- **Positive prototypes:** “админ удалите”; “это не по теме”; “обсуждение политики”; “спор не про событие”; “реклама”
- **Action:** Exclude from evidence.

### 58. `sarcasm_or_negation`
- **public_sentence:** `null`
- **publishable:** `false`
- **Purpose:** detect sarcasm/negation near positive phrases.
- **Positive prototypes:** “ну да, конечно, лучшие организаторы”; “ага, очень ждали, смешно”; “не сказал бы, что это ожидаемое событие”; “любимый актёр? точно нет”
- **Action:** Raise risk flag; do not use as positive evidence without verifier.

### 59. `factual_conflict_ticket_status`
- **public_sentence:** `null`
- **publishable:** `false`
- **Purpose:** detect contradiction between comments and canonical ticket_status.
- **Positive prototypes:** “canonical ticket_status=available, comments say sold out”; “canonical ticket_status=sold_out, comments ask how to buy tickets”
- **Action:** Do not publish strong factual phrase; downgrade to availability discussion; emit possible_ticket_status_conflict.

## Display ordering

1. anticipation/interest; 2. artists/program/organizers; 3. tickets/demand; 4. practical questions; 5. frustration/barriers. Red `sad_red` cards are capped and should not be first if a strong positive or neutral item exists.

## Versioning and cache invalidation

- Any public sentence/prototype/threshold change increments `phrase_bank_version` or a subordinate prototype version.
- Verifier cache key includes `phrase_bank_version`, `verifier_policy_version`, event facts fingerprint, evidence fingerprint and model id.
- Changing a phrase from vector-only to verifier-required must suppress new vector-only publications until groups are reprocessed.
