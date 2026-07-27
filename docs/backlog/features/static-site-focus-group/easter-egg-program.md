# Пасхалки фокус-группы: правила результата и карта размещений

> **Статус:** продуктовый контракт прототипа; конкурс не объявлен и начисление,
> влияющее на приз, не запущено.
> **Требования:** R17–R19.
> **Owner decision:** один приз — **ровно два билета в театр**. Победитель
> определяется прежде всего по доле собранной коллекции, затем по ограниченному
> и проверяемому участию в исследовании.
> **Связанные документы:** [общий прототип фокус-группы](product-prototype.md),
> [каноника пасхалок сайта](../../../features/static-site-easter-eggs/README.md).

## 1. Что принято, а что ещё не является правилами

Эта программа — ограниченное исключение из prize-free MVP общей механики
пасхалок. Последнее решение владельца заменяет прежнюю идею равновесной заявки:
для фокус-группы коллекция и **широта** участия должны определять результат.
Однако текущие экраны, локальный `localStorage` и demo leaderboard не создают
право на приз и не являются доказательством результата.

До legal/partner/privacy/anti-fraud gate интерфейс показывает только:

> **Правила готовятся.** Прототип показывает, как может считаться коллекция и
> исследовательская активность. Сейчас действия на сайте не начисляют конкурсные
> баллы и не создают право на два билета. Сроки, организатор, театр, спектакли,
> ограничения и порядок получения будут опубликованы отдельно до начала.

Нельзя показывать `Вы участвуете`, `Ваш конкурсный балл`, дату выбора победителя
или обещание `на любой спектакль`, пока эти условия не опубликованы и не приняты.
После gate допустимая точная формула награды: **«Один приз — два билета в
театр»**. Название театра, доступные даты/спектакли, места, срок использования и
бронирование не додумываются интерфейсом.

## 2. Версии и неизменяемый снимок

Один исследовательский цикл фиксирует четыре независимые версии:

| Поле | Prototype value | Что фиксирует |
|---|---|---|
| `program_id` | `focus-2026-01` | cohort, окно и timezone |
| `rules_version` | `focus-prize-pending-v1` | формулу, caps, eligibility и tie-break |
| `collection_version` | `focus-eggs-v1` | состав и denominator коллекции |
| `placement_version` | `focus-eggs-placement-v1` | разрешённые responsive anchors и prerequisites |

После публикации правил версия не редактируется задним числом. Исправление
создаёт новую версию и migration decision с причиной. В immutable closing
snapshot входят participant public id, четыре версии, серверные receipts,
исключённые/credited placements, rank tuple и hash snapshot. Raw NPS value,
текст отзыва, email и inferred interests в leaderboard snapshot не входят.

Одинаковое окно доступности обязательно для всех допущенных участников. Поздний
вход допустим для prize eligibility только при заранее опубликованном enrollment
cutoff либо эквивалентном catch-up window. Недоступность обязательной находки из-за
ошибки, отсутствия контента или safety pause не разрешается тайно компенсировать
другой случайной позицией.

## 3. Rank tuple: коллекция всегда важнее активности

Один суммарный `0…100` отклонён: при сложении активный кликер мог бы обойти
участника с большей коллекцией. Leaderboard сортируется лексикографически:

```text
1. collection_coverage = distinct_verified_eggs / eligible_eggs
2. participation_points = 0…40
3. participation_category_breadth = 0…7
4. audited tie draw among exact ties
```

Доли коллекции сравниваются как рациональные числа, без округления. При общем
неизменном denominator это просто число разных найденных объектов. Участник с
большей долей коллекции всегда выше участника с меньшей долей, независимо от
баллов активности. `participation_category_breadth` — число категорий из таблицы
ниже, где получен хотя бы один балл.

UI не склеивает показатели в вводящий в заблуждение total:

```text
Коллекция: 8 из 12        ← основной результат
Участие: 27 из 40         ← дополнительный результат
Охвачено способов: 6 из 7
Место: демо, пока правила не опубликованы
```

### 3.1. Ограниченный participation score

| Категория | Баллы | Cap и проверяемый receipt | Что принципиально не оценивается |
|---|---:|---|---|
| Ответ relationship NPS | 4 | один успешно полученный ответ за цикл | значение `0…10`, положительность |
| Likes | до 4 | 2 за первый reaction receipt на каждом из максимум двух разных событий | понравилось ли событие редакции |
| Dislikes | до 4 | симметрично likes: 2 × максимум два других события | критичность и «неудобный» сигнал |
| Текстовый feedback страницы | до 6 | 3 × максимум две разные page families; received, non-empty, non-duplicate | тон, длина, похвала, ручная «ценность» |
| Search | до 6 | 2 × максимум три qualified search session, не более одной в local programme day | наличие результатов, покупка, клик по билету |
| Saves / calendar | до 6 | 2 × максимум три разных event id, один receipt на событие | длительность хранения и последующая покупка |
| Разные page families | до 10 | 2 × максимум пять разных family с `meaningful_use` | пассивные refresh/pageview и время любой ценой |
| **Итого** | **до 40** | только idempotent bounded receipts | sentiment, выручка и объём текста |

Approved page families первой версии:

`listings | search | event_detail | festivals | for_me | feedback |
saves_calendar | collection`.

`meaningful_use` определяется для family заранее: Search — отправленный
непустой запрос; event detail — открытие facts section; festivals — открытие
программы/фильтра; `for_me` — явное изменение интереса или открытие «Почему это»;
feedback — успешно полученный допустимый ответ; saves/calendar — открытие списка
или действие с событием; listings — использование фильтра/переход к карточке;
collection — открытие истории найденного объекта. Сам `page_view` баллов не даёт.

NPS остаётся аналитически отдельным relationship measure. Prize ledger получает
только `relationship_nps_answered` и receipt id; score `0…10` хранится и
анализируется в feedback-контуре, не копируется в leaderboard и не влияет на
баллы. Ответ `0` и ответ `10` дают одинаковые 4 балла. Критический текст,
`dislike` и негативная usefulness-оценка считаются на тех же условиях, что
положительные.

### 3.2. Idempotency и изменение решения

- Один event id не может закрыть одновременно like и dislike cap. Первый
  point-bearing reaction receipt закрепляет только исследованный объект; человек
  может затем исправить свою текущую реакцию без потери баллов и без новых баллов.
- Undo save/calendar не отнимает уже подтверждённый exploration receipt: участие
  не должно заставлять хранить ненужное событие. Повторный toggle не даёт баллы.
- Повтор текста, whitespace-only, автоповтор запроса, reload и повторная доставка
  одного `idempotency_key` не создают receipt.
- Search session считается и при честном `0 результатов`: проверка пустого
  состояния полезна. Не более одной point-bearing session за календарный день в
  timezone программы; normalized query должен отличаться от уже засчитанных.
- Feedback считается после технического `received`, а не после редакционного
  одобрения. Модератор не может лишить баллов за мнение, низкую оценку, краткость
  или сообщение об ошибке.

## 4. Leaderboard, tie-break и проверка результата

Участник всегда видит собственные receipts и объяснение каждого изменения.
Cohort leaderboard показывает только opt-in pseudonym, collection fraction,
participation points/category breadth, rules version и время последнего
серверного пересчёта. Raw feedback, NPS, запросы Search, список событий и email
не публикуются. Отказ от публичного pseudonym не исключает из результата.

При точном равенстве первых трёх ключей скорость не используется: earliest-find
наказывает поздно приглашённых, людей с assistive technology и участников после
outage. Равные участники попадают в аудируемый tie draw:

1. до закрытия публикуется commitment к случайному seed и алгоритм;
2. после immutable snapshot seed раскрывается;
3. для каждого tied public id вычисляется документированный hash/HMAC rank;
4. выигрывает минимальное значение; входной список и проверка сохраняются;
5. alternate выбирается тем же порядком, если это разрешено правилами.

Конкретный алгоритм, источник seed и независимый witness утверждаются legal и
anti-fraud review. Пока они не утверждены, интерфейс пишет `Порядок разрешения
ничьей ещё не утверждён`, а не использует скрытый случайный выбор.

## 5. Anti-abuse без наказания за критику

Production result требует server-owned membership и append-only/idempotent
receipts. LocalStorage может визуализировать прототип, но не подтверждает ни
membership, ни призовой результат.

Обязательные ограничения:

- один receipt на заданные `participant + action kind + subject + rules version`;
- caps из таблицы применяются server-side до обновления leaderboard;
- reaction subjects — разные canonical event ids; page feedback — разные family;
- Search хранит privacy-reviewed normalized/hash evidence, а не публикует query;
- rate limit и bot/anomaly flag не удаляют результат автоматически;
- invalidation возможна только по опубликованному объективному правилу
  (например, подтверждённая автоматизация или чужой account), с reason code,
  audit trail, уведомлением и appeal window;
- moderator никогда не оценивает «правильность» NPS, sentiment, длину feedback
  или согласие с командой;
- share, invite, покупка, ticket click, длительность сессии, скорость, streak и
  повторные действия не дают advantage;
- организатор замораживает snapshot и проверяет победителя до уведомления.

## 6. Accessibility и равные пути

Каждый prize-relevant egg имеет один `egg_id` и эквивалентные mobile, desktop и
accessible anchors. Найти его можно touch, keyboard и screen reader; motion,
hover, звук, точное наведение, QR и наличие двух устройств не обязательны.
`prefers-reduced-motion` отключает декоративное движение, но не объект и не
действие. Target не меньше применимого design-system minimum, focus order
следует DOM, accessible name сообщает `Пасхалка: [имя]` и состояние `Найдено`.

Цель исследования — проверить и телефон, и компьютер, поэтому UI может
предлагать необязательную миссию `Попробовать на другом устройстве` и измерять
её отдельно. Она не добавляет collection item, participation points или
tie-break advantage: отсутствие компьютера/телефона не лишает шанса.

Accessible equivalent не обходит prerequisite. После выполнения того же
действия/контентного условия на странице коллекции разблокируется спокойная
keyboard/screen-reader ссылка к тому же `egg_id`. Она нужна вместо скрытого
жеста или сложного responsive layout, но не выдаёт находку до prerequisite.

## 7. Placement matrix `focus-eggs-placement-v1`

В таблице `после N-го` означает semantic insertion boundary среди canonical
органических объектов, а не CSS `:nth-child`. Promo, skeleton, ad и сама
пасхалка не меняют ordinal. Anchor получает стабильный key; reorder, reload,
reaction и resize не reroll-ят уже назначенный `placement_bundle_id`.

| Egg | Проверяемая функция | Mobile anchor | Desktop anchor | Prerequisite | Accessible equivalent | Fail-closed |
|---|---|---|---|---|---|---|
| `FG-E01` | Search | `search.results.after-4` после 4-й карточки | после 4-го canonical result в grid/list | непустой запрос и ≥4 renderable results | после того же запроса — ссылка из Search status/collection task | при 0–3 результатах объект отсутствует, не переносится к input |
| `FG-E02` | Event detail | `event.facts.after-primary` перед ticket block | конец facts column перед booking aside | canonical event с датой и местом | кнопка после landmarks `Основные сведения`; тот же prerequisite | при неполных facts или конфликте ticket CTA отсутствует |
| `FG-E03` | Today/tomorrow listing | `listing.primary.after-6` | после 6-й organic card | ≥6 renderable событий | DOM-кнопка после 6-й карточки, доступная клавиатурой | при короткой выдаче нет объекта и fallback после последней запрещён |
| `FG-E04` | Weekend / long listing | `listing.weekend.after-4` | после 4-й weekend card | ≥4 renderable события | collection task ведёт к тому же list boundary | при короткой/ошибочной выдаче отсутствует |
| `FG-E05` | Festivals directory | `festivals.directory.after-3` | после 3-й festival card | ≥3 опубликованных фестиваля | кнопка после 3-й карточки в DOM | при <3 фестивалях отсутствует |
| `FG-E06` | Festival detail/programme | `festival.program.after-3` | после 3-го event/program item | ≥3 renderable programme items | task link к programme landmark после выполнения prerequisite | при пустой/короткой программе отсутствует |
| `FG-E07` | `Для меня` | `for-me.recommendations.after-4` | после 4-й объяснимой рекомендации | явный interest choice и ≥4 карточки | после interest choice — доступная task-ссылка к блоку рекомендаций | без consent/choice или при <4 рекомендациях отсутствует |
| `FG-E08` | Relationship NPS | `feedback.nps.receipt` в нейтральном acknowledgment | тот же logical acknowledgment | любой успешно received NPS `0…10` | обычный focusable acknowledgment, без motion requirement | без receipt отсутствует; значение ответа не влияет |
| `FG-E09` | Текстовый page feedback | `feedback.text.receipt` | тот же logical acknowledgment | received non-empty, non-duplicate feedback | ссылка из confirmation/collection task | без receipt отсутствует; sentiment и длина не влияют |
| `FG-E10` | Like | `reaction.like.receipt` у статуса реакции | то же рядом с reaction status, не внутри card link | первый like receipt на canonical event | отдельная кнопка после status announcement | без receipt отсутствует; покупка не требуется |
| `FG-E11` | Dislike | `reaction.dislike.receipt` у статуса реакции | симметрично like | первый dislike receipt на другом canonical event | тот же доступный status/button path | без receipt отсутствует; критический сигнал равноправен |
| `FG-E12` | Saves / calendar | `saved.events.after-3` **сразу после третьего текущего элемента** | после 3-й карточки saved/calendar grid | список содержит ≥3 разных renderable event items | после третьего item — DOM-кнопка и task-ссылка к тому же anchor | при 0–2 текущих элементах отсутствует; после 2-го/empty-state не переносится |

### 7.1. Обязательный сценарий третьего события

`FG-E12` появляется только когда текущий канонический список `Мои события /
Календарь` действительно отрисовал минимум три разных event item. При добавлении
третьего события artifact вставляется **один раз после третьего элемента**. Если
в списке пока два события, пасхалки нет. Skeleton, expired/filtered row и promo
не считаются третьим объектом. Если находка уже подтверждена, последующее удаление
события не стирает коллекцию; повторное достижение трёх элементов не создаёт
вторую находку.

### 7.2. Preflight и изменение доступности

Перед открытием цикла automated preflight проверяет каждый required placement
на mobile и desktop fixtures/real candidate content: anchor uniqueness,
prerequisite count, keyboard order, accessible name, reduced motion, no overlap
с ticket/booking, отсутствие horizontal overflow и отсутствие private data в
public HTML. Denominator коллекции фиксируется только после preflight.

Если prerequisite не выполнен во время визита, слой **fail closed**: основной
сайт остаётся полезным, artifact не рендерится, и система не подставляет его в
более лёгкое место. Если обязательный placement стал недоступен после freeze:

1. он помечается `paused_unavailable` с evidence;
2. оператор восстанавливает исходный anchor либо включает заранее проверенный
   equivalent placement той же сложности/функции;
3. если это невозможно в опубликованное окно, применяется заранее описанный
   outage credit или объект исключается для всех из denominator;
4. каждое решение версионируется и видно в audit/leaderboard explanation.

Нельзя индивидуально reroll-ить сложную находку после hint, dislike, resize или
жалобы. Safety blocker может переместить её только в заранее утверждённый
эквивалент для всей затронутой группы.

## 8. Legal, partner и release gates

До любого outcome должны быть опубликованы и приняты:

1. организатор, территория, возраст/eligibility, cohort и способ подтверждения;
2. точные start/end/enrollment cutoff и timezone;
3. один приз — два билета в театр; театр/партнёр, спектакли, даты, места,
   исключения, бронирование, срок и alternate;
4. версии collection/placements/rules, rank tuple, caps и tie algorithm;
5. outage/accessibility credit, disqualification evidence и appeal;
6. privacy purpose, data fields, lawful basis/consent, retention, access/delete и
   запрет публикации raw NPS/feedback/search history;
7. partner name/logo/IP permission, tax/fulfilment и cancellation terms;
8. immutable snapshot, independent verification, notification и claim deadline;
9. support contact, incident/kill switch и правила изменения только новой
   версией.

Acceptance rules не могут быть prechecked и не должны связывать маркетинговое
согласие с prize eligibility. Участник может дать низкий NPS, dislike или
критический текст без уменьшения результата. До прохождения gates prize state
остаётся `hidden`/`pending_approval`, leaderboard — `demo`, а вся локальная
коллекция помечена `На этом устройстве · не конкурсный результат`.

## 9. Acceptance checklist прототипа

- [ ] Везде один приз означает ровно два билета в театр, не два приза.
- [ ] Collection coverage — первый rank key; participation ограничен `0…40`.
- [ ] Ответ NPS считается без значения; raw NPS аналитически отделён.
- [ ] Like и dislike симметричны; критический feedback не штрафуется.
- [ ] Ни длина текста, ни sentiment, ни share/invite/purchase/spam не дают баллы.
- [ ] Caps, receipts, snapshot, tie draw и invalidation объяснимы участнику.
- [ ] Mobile/desktop/accessibility используют один `egg_id` без device advantage.
- [ ] Каждый anchor versioned, semantic, preflighted и fail closed.
- [ ] `FG-E12` отсутствует до реально отрисованных трёх saved/calendar events.
- [ ] Без legal/partner/privacy gate copy честно говорит `Правила готовятся`.
- [ ] LocalStorage нигде не называется доказательством membership или результата.
