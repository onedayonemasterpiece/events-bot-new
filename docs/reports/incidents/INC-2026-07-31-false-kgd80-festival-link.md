# INC-2026-07-31-false-kgd80-festival-link Ложная привязка событий к «80 историй о главном»

Status: open
Severity: sev2
Service: Telegram Monitoring / Smart Update / event public surfaces
Opened: 2026-07-31
Closed: —
Owners: events-bot
Related incidents: `INC-2026-04-10-tg-monitoring-festival-bool`, `INC-2026-05-05-80-stories-source-coverage`, `INC-2026-07-15-tg-rich-medallion-rendering-gaps`, `INC-2026-07-21-faberge-tg-public-writer-gap`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/llm/prompts.md`, `docs/operations/incident-management.md`, `docs/operations/release-governance.md`

## Summary

Публикация [@kldevents/3010](https://t.me/kldevents/3010) ошибочно получила
медальон, ссылку и хэштег фестиваля «80 историй о главном». Исходный пост
Зеленоградской библиотеки говорил только о 80-летии образования
Калининградской области и не называл кампанию. Это повторяющийся production
дефект: аудит всех 39 событий с такой связью нашёл четыре неподтверждённые
привязки — события `6129`, `6427`, `6991`, `7332`.

## User / Business Impact

- Пользователи видели неверную принадлежность четырёх событий к отдельному
  фестивальному проекту.
- Ошибка распространялась в Telegram, managed VK, Telegraph, фестивальную
  страницу и статический каталог.
- Смешение юбилея области с конкретной кампанией ухудшает доверие к афише и
  загрязняет программу фестиваля.

## Detection

- Пользователь сообщил о повторной ложной привязке и указал Telegram post
  `3010`.
- В автоматических проверках отсутствовал hard negative для общего выражения
  «80-летие Калининградской области».
- Production runtime file mirror сохранил полную цепочку импорта; single grep
  был дополнен поиском по event id, source URL, Telegram message id, job id и
  временному окну.

## Timeline

- 2026-07-30 18:45:49 UTC — VK auto-import создаёт event `7332` без festival.
- 2026-07-31 00:26 UTC — Telegram Monitoring повторно импортирует собственный
  post `@kldevents/2988`; producer возвращает `festival="80 историй о главном"`
  без literal campaign anchor.
- 2026-07-31 00:26:42 UTC — Smart Update принимает поле и создаёт
  `festival_queue.id=1315`.
- 2026-07-31 01:14–01:15 UTC — downstream-задачи обновляют VK, Telegraph,
  calendar и связанные данные.
- 2026-07-31 08:00:26 UTC — Telegram publisher обновляет событие как
  RichMessage `3010` уже с ложным фестивальным медальоном.
- 2026-07-31 — пользователь сообщает об инциденте; production audit находит
  ещё три неподтверждённые связи (`6129`, `6427`, `6991`).

## Root Cause

1. Gemma extract/rescue prompts приводили «80 историй о главном» как близкий
   пример, но не запрещали отождествлять кампанию с общим 80-летием области.
2. Telegram server-import и центральная Smart Update boundary доверяли
   `candidate.festival`, не проверяя наличие literal campaign name/hashtag,
   `kgd80.ru` или curated festival-source binding в текущем источнике.
3. Мониторинг собственного `@kldevents` повторно подал уже опубликованное
   событие в Smart Update; ошибочный enrichment изменил canonical event, после
   чего штатные publishers честно размножили неверную связь.

## Contributing Factors

- Не было source-grounded negative replay с формулировкой «80-летие
  Калининградской области».
- Festival queue принимала ошибочное поле после Smart Update.
- Исторический аудит принадлежности к кампании не был частью publication gate.

## Automation Contract

### Treat as regression guard when

- меняются Telegram Monitoring extract/rescue prompts;
- меняются `EventCandidate.festival`, festival queue или Smart Update merge;
- меняется KGD80 medallion/publication logic;
- переобрабатываются собственные Telegram posts.

### Affected surfaces

- `kaggle/TelegramMonitor/telegram_monitor.py`;
- `source_parsing/telegram/handlers.py`;
- `festival_grounding.py` и `smart_event_update.py`;
- production SQLite `event`, `event_source`, `festival_queue`, `joboutbox`;
- Telegram `@kldevents`, managed VK, Telegraph, KGD80 festival page и static
  site.

### Mandatory checks before closure or deploy

- negative replay: общее 80-летие не становится KGD80;
- positive replay: literal name, separator-style hashtag или `kgd80.ru`
  сохраняют KGD80;
- replay проходит Telegram server-import и настоящий Smart Update на shadow DB;
- отдельный VK/central-boundary hard negative проходит без festival queue;
- production audit всех KGD80 rows не оставляет неподтверждённых связей;
- все четыре Telegram posts и связанные current public surfaces проверены после
  ремонта;
- deployed SHA достижим из `origin/main`, worktree чистый.

### Required evidence

- deployed SHA и Fly release;
- pytest output для incident replay;
- redacted runtime-log extracts и production audit в
  `artifacts/codex/INC-2026-07-31-false-kgd80-link/`;
- live Telegram/VK/Telegraph/static verification;
- confirmation, что fix reachable from `origin/main`.

## Immediate Mitigation

- Подготовлен полный inventory неподтверждённых KGD80 связей.
- Pending festival queue row `1315` включён в production repair plan, чтобы
  ошибочная связь не вернулась до следующей обработки.

## Corrective Actions

- Extract и оба rescue prompts различают общий юбилей области и literal KGD80
  campaign anchor.
- Telegram import boundary fail-closed отбрасывает неподтверждённый KGD80.
- Центральный Smart Update guard применяет тот же контракт для VK и остальных
  source types; curated KGD80 festival sources сохраняют явную серию.
- Добавлен source-faithful incident replay с negative и positive controls.

## Follow-up Actions

- [ ] Проверять campaign-association precision при последующих KGD80 массовых
  импортах.
- [ ] Сохранить audit query как часть incident evidence; не добавлять
  title-keyword classifier вместо LLM-first extraction.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

Literal anchor contract теперь действует дважды: producer prompt должен вернуть
правильный смысл, а narrow source-grounding guard не позволяет известному
campaign-specific полю изменить canonical row без доказательства в источнике.
Guard не классифицирует произвольные фестивали и не подменяет LLM.
