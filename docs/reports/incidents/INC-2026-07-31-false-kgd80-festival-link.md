# INC-2026-07-31-false-kgd80-festival-link Ложная привязка событий к «80 историй о главном»

Status: closed
Severity: sev2
Service: Telegram Monitoring / Smart Update / event public surfaces
Opened: 2026-07-31
Closed: 2026-07-31
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
- Ошибка распространялась в Telegram, managed VK, Telegraph и фестивальную
  страницу; canonical static inputs также были загрязнены, хотя сообщённое
  событие `7332` в текущий публичный root не попало.
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

- Выполнен полный inventory: из 39 KGD80-связей четыре оказались
  неподтверждёнными (`6129`, `6427`, `6991`, `7332`).
- У всех четырёх canonical rows снята ложная связь; `festival_queue.id=1315`
  удалён после резервного копирования.
- Telegram posts `770`, `1357`, `2650`, `3010` исправлены на месте с
  сохранением message id. Managed VK post `8453` исправлен на месте; устаревший
  managed post `7784` удалён после подтверждения owner, потому что VK уже не
  разрешал его редактировать.
- Пересобраны четыре Telegraph event pages, KGD80 festival page и festival
  navigation. Повторный production audit оставил 35 подтверждённых связей и
  ноль неподтверждённых.

## Corrective Actions

- Extract и оба rescue prompts различают общий юбилей области и literal KGD80
  campaign anchor.
- Telegram import boundary fail-closed отбрасывает неподтверждённый KGD80.
- Центральный Smart Update guard применяет тот же контракт для VK и остальных
  source types; curated KGD80 festival sources сохраняют явную серию.
- Добавлен source-faithful incident replay с negative и positive controls.

## Follow-up Actions

- [x] Проверять campaign-association precision при последующих KGD80 массовых
  импортах.
- [x] Сохранить audit query как часть incident evidence; не добавлять
  title-keyword classifier вместо LLM-first extraction.

## Release And Closure Evidence

- deployed SHA: `1632ce819272ee1bd86f0c9984c9e8bce664a955`, достижим
  из `origin/main`; behavioral commits `3d2720fb` и `ee0cda8d`.
- deploy path: PR
  [#152](https://github.com/onedayonemasterpiece/events-bot-new/pull/152) и
  [#153](https://github.com/onedayonemasterpiece/events-bot-new/pull/153),
  Fly release `v1785`, image
  `deployment-01KYVRFXCX0NV0RSE32JQMCFF6`, status `complete`.
- regression checks: incident replay `5 passed`; расширенный релевантный набор
  `83 passed`; `py_compile` для изменённых runtime modules; в обоих PR зелёные
  `python-ci` и `static-browser-release-gate`.
- post-deploy verification: `/healthz` вернул `ok=true`, `ready=true`,
  `issues=[]`; deployed smoke отбрасывает общее 80-летие, сохраняет literal
  campaign anchor и curated source binding. Production readback подтвердил:
  `35` grounded KGD80 rows, `0` ungrounded; все четыре Telegram posts без
  KGD80; VK `8453` без KGD80, VK `7784` удалён; Telegraph event/festival pages
  без четырёх ложных событий.
- static-site verification: текущий public root не содержал event `7332`.
  Во время forced rebuild обнаружен отдельный ранее известный stale
  secret-candidate job и нехватка volume capacity; assertion-gated recovery
  сохранён в incident artifacts и ведётся по regression contracts
  `INC-2026-07-18-static-snapshot-disk-pressure` /
  `INC-2026-07-19-static-site-stale-builder-lease`. Это не оставляет ложную
  фестивальную связь на public root и не блокирует closure этого incident.
- evidence: redacted runtime extracts, production backups/audits и live
  readbacks сохранены в
  `artifacts/codex/INC-2026-07-31-false-kgd80-link/` (не коммитятся).

## Prevention

Literal anchor contract теперь действует дважды: producer prompt должен вернуть
правильный смысл, а narrow source-grounding guard не позволяет известному
campaign-specific полю изменить canonical row без доказательства в источнике.
Guard не классифицирует произвольные фестивали и не подменяет LLM.
