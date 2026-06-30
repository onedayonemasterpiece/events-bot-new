# Requirements: trip-recomendation

Status: reconciled draft

## Product intent

- Сформировать самостоятельный туристический продукт: однодневные маршрут-рекомендации из Калининграда в города/места Калининградской области с понятным транспортом, событиями/местами посещения и практическими советами.
- Использовать уже накопленную базу событий, регулярные туристические активности организаций и транспортные возможности перевозчиков, чтобы рекомендовать поездки, которые реально можно совершить.
- Поддерживать как нейтральные редакционные рекомендации, так и партнёрские/промо-сценарии, где маршрут помогает продвигать конкретного перевозчика, направление или туристическую организацию.
- Публиковать маршрут так, чтобы он был пригоден для отдельного VK/Telegram/MAX поста без необходимости вручную собирать расписания, ссылки, телефоны и рекомендации.
- Дать фиче возможность позже работать вместе с social-discovery / Subscriber Acquisition: рекомендовать не паблик в целом, а конкретный уже собранный маршрут там, где контекст обсуждения делает такую рекомендацию полезной.

## Requirements

### Route inputs and inventory

- Система должна уметь строить маршрут на основе нескольких типов входных данных:
  - перевозчики и их направления/остановки/станции, например пригородные поезда по Калининградской области;
  - конкретные будущие события из базы анонсов;
  - места/локации, где проходят события;
  - организации с регулярными туристическими активностями, в том числе экскурсии и другие recurring/non-event activities.
- Инвентарь регулярных туристических организаций должен позволять хранить контакты, ссылки, телефоны, расписание/правила посещения и признаки туристической релевантности.
- При формировании маршрута система должна отличать ситуативные события от регулярных туристических предложений, но уметь объединять их в один маршрут.
- Примеры источников/кейсов из intake: Калининградская пригородная пассажирская компания, направление Калининград — Зеленоградск-2, конный завод Георгенбург. Эти примеры не фиксируют MVP-обязательство, пока не выбраны начальные партнёры/направления.

### Route composition

- Основной формат маршрута — преимущественно поездка на один день с выездом из Калининграда в город/место области и возвращением в тот же день.
- Маршрут должен включать:
  - рекомендуемый транспорт туда и обратно;
  - расписание отправления/возвращения или несколько допустимых вариантов времени;
  - стоимость проезда, если она доступна;
  - места и события, которые стоит посетить;
  - прямые ссылки и телефоны мест/организаций, куда пользователь едет;
  - практические рекомендации: что взять с собой, чего ожидать, как подготовиться;
  - краткое описание ожидаемых впечатлений/сценария поездки.
- Система должна поддерживать маршруты не только «точка А → точка Б», но и составные маршруты, если их можно реалистично собрать по расписанию транспорта.
- Приоритетом для автогенерации являются маршруты, где сходятся несколько факторов: интересное событие/фестиваль, подходящее расписание транспорта, доступность возвращения, сезонность/выходные или другой контекстный повод. Наличие выходного дня является полезным сигналом, но не обязательным условием.
- Маршрут может строиться вокруг ожидаемого фестиваля или другого заметного события, если оно повышает ценность поездки.

### Transport and carrier scenarios

- Перевозчик может быть редакционно выбранным элементом маршрута или партнёрским интересантом.
- Система должна поддерживать промо-сценарий, где маршрут продвигает услуги конкретного перевозчика или конкретное направление перевозчика.
- Промо-сценарии могут использоваться для направлений с недостаточной загрузкой или низкой осведомлённостью аудитории, например малоизвестная станция/остановка.
- Партнёрский маршрут не должен скрывать, что он основан на конкретном перевозчике/направлении, если для публикации требуется маркировка или редакционное пояснение.

### Publication formats

- VK-формат должен поддерживать карусель/набор карточек: несколько визуальных карточек с этапами/акцентами маршрута плюс тело поста.
- Тело поста должно содержать самодостаточное описание маршрута, прямые ссылки и телефоны мест/организаций, транспортные детали и базовые рекомендации.
- Для Telegram и MAX требуется компактный формат, пригодный для одной ёмкой картинки или другого нативного формата, который сохраняет ключевую структуру маршрута; точный шаблон остаётся предметом UX-выбора.
- Визуальная генерация должна переиспользовать уже существующие в системе подходы к карточкам/каруселям там, где они подходят, вместо разработки полностью отдельной карточной механики без необходимости.

### Reviews and feedback loop

- Маршрутные публикации могут включать отзывы людей, которые уже съездили по маршруту, если источник отзыва понятен и публикация соответствует правилам модерации/согласия.
- Отзывы могут приходить от партнёров или от пользователей, которые воспользовались маршрутом.
- MVP-совместимый сбор обратной связи: мотивировать людей писать в комментариях к маршрутной публикации, что они съездили, а затем собирать и агрегировать эти комментарии для последующих маршрутов/отзывов.
- Система должна отделять фактическую обратную связь о маршруте от рекламных или нерелевантных комментариев перед использованием в новых публикациях.

### Social discovery / acquisition integration

- Фича может интегрироваться с Subscriber Acquisition Discovery: искать публичные обсуждения, где уместно аккуратно рекомендовать конкретный маршрут из накопленной базы маршрутов.
- В таком сценарии продвигается не общий паблик, а конкретный маршрут, релевантный вопросу/обсуждению.
- Рекомендация маршрута во внешних обсуждениях должна наследовать ограничения Subscriber Acquisition: полезность, точность, антиспам, редкие и уместные ответы, ручная проверка/теневой режим до автоматизации.

### Retrieval, matching, and route graph requirements

- Подбор событий, мест и регулярных активностей для маршрута должен поддерживать семантический поиск: маршрутная задача не сводится к exact keyword matching.
- Помимо семантической близости, подбор должен учитывать графовые/структурные ограничения: транспортные связи, станции/остановки, время в пути, окна между событиями, возможность вернуться в тот же день и совместимость нескольких точек маршрута.
- Route ranking должен комбинировать минимум три класса сигналов: содержательная привлекательность события/места, реализуемость транспорта и публикационный/партнёрский контекст.
- Система должна сохранять объяснимость: почему выбран маршрут, какие факторы сработали, какие транспортные ограничения были учтены.

## Technical recommendations / findings

- Технически выявлено: в проекте уже есть фича `promo-campaigns`, поэтому carrier/route promotion следует моделировать как расширение campaign/activity surface или совместимый campaign layer, а не как полностью отдельный промо-механизм.
- Технически выявлено: в проекте уже есть карточные/VK-карусельные подходы (`afishaengagement`, engagementcard-подходы), поэтому VK route cards следует начинать с переиспользования существующих renderer/palette/layout практик.
- Технически выявлено: в проекте уже есть `Subscriber Acquisition` с Discovery-only shadow/manual-review моделью; маршрутные рекомендации во внешних обсуждениях должны подключаться к этому surface как новый тип recommendation target, не обходя антиспам и review gates.
- Технически рекомендовано: canonical facts о событиях, публикациях и scheduler/review state должны оставаться связанными с core events-bot storage; отдельная Supabase/Postgres/pgvector сторона допустима как retrieval/search sidecar или как future subproject boundary, но не должна становиться новым источником истины для событий без отдельного архитектурного решения.
- Технически рекомендовано: для semantic retrieval переиспользовать существующий проектный паттерн Supabase/pgvector search sidecar из unsigned-personalization, если он доступен в целевой ветке, и дополнять его route graph constraints; если sidecar недоступен, зафиксировать совместимый интерфейс retrieval layer до выбора конкретного хранилища.

## Open questions

- Какие перевозчик/направление/город взять первым MVP-кейсом: Калининградская пригородная пассажирская компания / пригородные поезда, Калининград — Зеленоградск-2, Георгенбург или другой партнёрский маршрут?
- Что является первым целевым каналом публикации: отдельное VK-сообщество, существующее VK-сообщество, Telegram, MAX или набор поверхностей сразу?
- Для MVP маршрут должен быть строго однодневным с выездом из Калининграда и возвращением в тот же день, или допускаются первые составные/многоостановочные маршруты при доказанной реализуемости расписания?
- Можно ли использовать партнёрские отзывы в публикациях сразу после ручной модерации, или нужны отдельные правила согласия/атрибуции до публичного использования?
- Кто утверждает начальный список регулярных туристических организаций и мест, которые можно включать в маршруты до автоматического discovery?

## Decisions log

- 2026-06-30: Reconciled the initial voice-note intake into route inputs, composition, carrier/promo scenarios, publication formats, social-discovery integration, and route graph/retrieval requirements.
- 2026-06-30: Reconciled the follow-up intake about reviews/feedback and vector/graph storage direction; no product conflict was found.
- 2026-06-30: Classified storage/vector-search choices as technical recommendations rather than user poll questions: use existing project storage/retrieval patterns by default and only split into a separate subproject/database after a dedicated architecture decision.

## Archived intake 2026-06-30T12:05:37+00:00

Status: resolved / archived 2026-06-30

### Resolution

Integrated into Product intent, Requirements, Open questions, Decisions log, and Technical recommendations / findings above. No contradiction with previously recorded canonical requirements was found because the previous canonical sections were still TBD.

### Source files

- [source/voice_AgADvJMAAqf3GUo.oga](source/voice_AgADvJMAAqf3GUo.oga)
- [source/voice_AgADvpMAAqf3GUo.oga](source/voice_AgADvpMAAqf3GUo.oga)
- [source/voice_AgADwJMAAqf3GUo.oga](source/voice_AgADwJMAAqf3GUo.oga)
- [source/voice_AgADwZMAAqf3GUo.oga](source/voice_AgADwZMAAqf3GUo.oga)
- [source/voice_AgADxZMAAqf3GUo.oga](source/voice_AgADxZMAAqf3GUo.oga)
- [source/voice_AgADEpQAAqf3GUo.oga](source/voice_AgADEpQAAqf3GUo.oga)
- [source/voice_AgADFJQAAqf3GUo.oga](source/voice_AgADFJQAAqf3GUo.oga)
- [source/voice_AgADGJQAAqf3GUo.oga](source/voice_AgADGJQAAqf3GUo.oga)
- [source/voice_AgADGZQAAqf3GUo.oga](source/voice_AgADGZQAAqf3GUo.oga)
- [source/voice_AgADG5QAAqf3GUo.oga](source/voice_AgADG5QAAqf3GUo.oga)
- [source/voice_AgADHpQAAqf3GUo.oga](source/voice_AgADHpQAAqf3GUo.oga)

### Reconciliation checklist

- [x] Compare with previous requirements.
- [x] If user notes include automatic voice transcripts, treat them as noisy input: recover likely context but ask about uncertain fragments instead of guessing.
- [x] If there is a contradiction, ask which requirement wins: old, new, or another resolution.
- [x] Move resolved statements into the canonical sections above and remove/close this pending intake.

## Archived intake 2026-06-30T13:59:31+00:00

Status: resolved / archived 2026-06-30

### Resolution

Integrated into Reviews and feedback loop plus Technical recommendations / findings. The note about a separate project/database, vector search, and graph search was treated as technical direction; project docs show comparable existing storage/retrieval patterns, so no product poll is needed at requirements-reconciliation time.

### Source files

- [source/voice_AgADRJQAAqf3GUo.oga](source/voice_AgADRJQAAqf3GUo.oga)
- [source/voice_AgADSZQAAqf3GUo.oga](source/voice_AgADSZQAAqf3GUo.oga)

### Reconciliation checklist

- [x] Compare with previous requirements.
- [x] If user notes include automatic voice transcripts, treat them as noisy input: recover likely context but ask about uncertain fragments instead of guessing.
- [x] If there is a contradiction, ask which requirement wins: old, new, or another resolution.
- [x] Move resolved statements into the canonical sections above and remove/close this pending intake.
