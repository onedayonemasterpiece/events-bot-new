# Задание кодовому агенту: Hero-talk + существующий EventsBot MCP → production

> **Редакция 2, 6 сентября 2026.** Прямое задание на разработку, интеграцию, тестирование и поэтапный production release. Не исследование и не только release handoff.
> **Задача результата:** [#643](https://github.com/onedayonemasterpiece/events-bot-new/issues/643).
> **Основание:** [исследование #642](autofill-mvp.md), [контентные/MCP-сценарии](owner-mcp-mvp.md), [Hero owner #291](https://github.com/onedayonemasterpiece/events-bot-new/pull/291), [главная #641](https://github.com/onedayonemasterpiece/events-bot-new/pull/641).
> **Обязательное уточнение владельца:** промо-движок уже работает; Hero-talk становится ещё одной активностью существующих промо-кампаний. Недавно написанные owner/partner MCP-функции нужно сохранить, закончить, дополнить управлением Hero-talk и довести до production.

---

Ты — кодовый агент в рабочем окружении `onedayonemasterpiece/events-bot-new`. Доведи существующий MCP-продукт и общий Hero-talk до проверенного рабочего production-результата. Не начинай с нового общего аудита и не заканчивай на документах, таблицах, `tools/list`, feature-OFF deploy или предложении кому-то продолжить.

## 1. Результат для владельца и партнёра

**Владелец** через действующий EventsBot MCP читает текущее наполнение Hero-talk сверху и в конце страниц, получает статистику и причины непоказа, задаёт темы/события/функции, загружает картинку, передаёт точные фразы и цепочки, получает preview, публикует, изменяет, приостанавливает и восстанавливает версии. Автоматическое наполнение работает без ежедневного ручного написания; обе поставки используют один renderer и библиотеку.

**Партнёр** через уже разработанную отдельную OAuth-проекцию подключается без обязательной регистрации в Telegram, получает назначенные организацию/портфель/права, создаёт событие с текстом и афишей, проходит необходимое согласование, получает canonical event ID и видит реальные статусы публикаций. Отдельным действием создаёт/запрашивает промо через существующий движок и управляет разрешёнными активностями. Владелец может менять права, приостанавливать и отзывать доступ. Hero-talk доступен партнёру только в пределах явно выданного права на соответствующую activity; глобальное редакционное управление Hero по умолчанию owner-only.

Это продолжение двух существующих продуктов, не новый MCP-сервер, рекламная платформа или параллельный event pipeline.

## 2. Какие прежние формулировки заменены этим поручением

| Прежняя граница | Действующее поручение |
|---|---|
| Исследование/первый Hero handoff: «не выполнять production» | То относилось к исследовательскому окну. Эта задача включает разработку, merge, штатный deploy и активацию готовых функций после обязательных проверок |
| Safe-implementation §8 / старые PR: «ChatGPT пишет, Codex только разворачивает готовое» | В этом задании кодовый агент также дописывает недостающее. Не писать крупные изменения на deployment checkout: разработка/CI отдельно, deploy из clean exact main |
| Hero-research описывает семь новых tools/свои scopes/две таблицы как стартовую форму | Это предложение интерфейса, не обязательная новая платформа. Сначала использовать свежие MCP services, schemas, staging, access policy и operation ledger. Добавлять только отсутствующее |
| Hero MVP откладывает весь partner MCP вместе с paid/video/cross-device расширениями | Рабочий новый owner/partner MCP входит в результат сейчас. Video mosaic, новая платёжная модель и расширенная narrative memory по-прежнему не входят |
| Promo понимается как отдельный Hero campaign subsystem | Запрещено. `surface=hero_talk` — новая активность реальной существующей кампании |

Эти уточнения имеют приоритет над ранними исследовательскими предложениями. Безопасность, фактические права, consent, mandatory tests, A=S=P и явное согласование конкретного публичного контента не отменяются. Не включать существующие paused campaigns и не добавлять Hero activity всем кампаниям автоматически.

## 3. Fresh-read: продолжить именно написанный код

Проверено при подготовке задания; перед работой перечитай свежие heads, diff и последние comments:

| Уже существующий PR | Проверенная ветка / SHA | Фактический объём |
|---|---|---|
| [#618 — R0](https://github.com/onedayonemasterpiece/events-bot-new/pull/618) | `feature/events-mcp-queue-observability-r0` @ `9dd31c5ed36c7c20da2dd8fa0480e1cb771eadf6` | Owner queue read в существующем `operations_snapshot`; детали через `fetch(job:...)`; без новых tools |
| [#623 — R1](https://github.com/onedayonemasterpiece/events-bot-new/pull/623) | `feature/events-mcp-owner-create-r1` @ `4ae7b69fa63c809c806a4ab94c3917028860bb8e` | `event_create_prepare/commit`, `event_operation_get`, canonical `event_change_log`, существующий Smart Update/fan-out |
| [#640 — partner](https://github.com/onedayonemasterpiece/events-bot-new/pull/640) | `feature/events-mcp-partner-product-20260906` @ `9bd684d51fb8184ca0bafa9856378c839e50179c` | Partner OAuth/PKCE, policy/portfolio, owner create/read/access-change, scoped partner reads; это ещё не полный mutation product |

На срезе все три PR открыты, не merged. #640 основан на #623, тот — на #618. В прочитанном `private_events_mcp/partner_tools.py` явно `event_operations=False`, `promo_operations=False`. Значит нельзя просто добавить Hero tools к read-only партнёру и назвать весь продукт законченным. PR #640 отдельно перечисляет незавершённые event/media/review/recovery, promo, lifecycle и publication receipt работы; сверь их с кодом, не с заголовком PR. Старые числа тестов — historical evidence, не PASS будущей интеграции.

Сначала прочитай актуальные `AGENTS.md`, `docs/README.md`, `docs/routes.yml`, repository workflow и release governance. Затем предметные владельцы:

- `docs/operations/private-events-mcp.md`;
- `docs/operations/private-events-mcp-event-operations-to-be.md` и `...-safe-implementation.md`;
- `docs/testing/private-events-mcp-event-operations-scenarios.v2.yml`;
- `docs/features/promo-campaigns/README.md`, `partner-promo.md`;
- Hero README, это задание, AutoFill/owner MCP материалы;
- релевантные media, analytics, onboarding, LLM gateway и static release contracts.

Рабочая база должна включить свежий main **и** не потерять #618/#623/#640. Проверь ancestry/merge-base; продолжи существующий stack либо создай согласованную integration-ветку, перенеся его целиком в проверенном порядке. Не клонируй реализацию заново из старого main. Не reset/force-push чужую незавершённую работу. При параллельных изменениях повторно fetch перед интеграцией и сохранить новые commits.

Августовский #291 — только владелец Hero-документов/donor evidence, не база runtime. Переноси необходимые документы адресно, не merge весь старый onboarding/runtime stack. Новые понятные небольшие PR допустимы поверх текущих; связывай их с #643 и существующими PR. Не делай один гигантский непроверенный release.

Первый технический результат — короткая таблица «уже есть / не подключено / отсутствует / нужно для release» с реальными symbol/file refs. Она нужна для продолжения кода, не как самостоятельный финал задачи.

## 4. Промо: только расширение существующих активностей

Canonical owner остаётся `promo.py` и существующие `promo_campaign`, `promo_target`, `promo_activity`, `promo_exposure`. В коде уже есть `create_partner_event_promo_campaign`, `PartnerActivitySpec`, `add_partner_activity_to_campaign`; их реальное состояние проверь на integration HEAD. Telegram `/promo` и `handlers/partner_promo_cmd.py` — действующие consumers, а не второй источник истины.

Добавь `surface=hero_talk` в существующий registry/validation/human-readable labels/activity configuration и исполнение. Home и page-end — placements **одной activity family**, а не две новые кампании. У активности — допустимые placements, связь с контентом/цепочкой и только действительно необходимые параметры выбора/частоты.

Обязательное поведение:

- К существующей кампании добавляется новая `PromoActivity`, сохраняются campaign ID, target, приоритет, срок, owner/organization и статус; остальные активности не перезаписываются.
- Campaign pause/archive/expiry управляют Hero так же, как другими активностями. Activity OFF отключает только Hero; placement OFF может отключить только верх/низ. Эти масштабы действия видны в preview/readback.
- Создание/редактирование кампании и её activity через MCP вызывают те же application services, что существующие интерфейсы; не копируют правила и не делают прямые ad-hoc SQL mutations.
- При отвязке/замене креатива не теряется campaign provenance. Нельзя обойти pause, переписав origin на organic.
- Живой festival/program target остаётся динамическим: новые пригодные события программы включаются по существующим правилам, а не через ручную поддержку вечного списка ID.
- Report и карточка кампании видят Hero activity, её состояние, показы/переходы и причины непоказа. Существующий `/promo` должен хотя бы корректно отображать и управлять новой активностью; второй UI-проект не нужен.
- Не смешивай публикационные единицы существующего exposure с browser visibility. Расширяй существующий типизированный accounting/report contract минимально. Нельзя строить независимый campaign counter или молча обходить cap. Неподдерживаемую политику отклоняй с конкретной причиной.
- Партнёрская campaign authorization идёт через текущие principal/tenant/organization/portfolio services. Не создавай fake Telegram user ради нового OAuth-партнёра. Если существующий helper принимает только Telegram ID, выдели минимальную общую actor-aware policy boundary с compatibility adapter и regression tests старого UI.

Hero content/program storage может хранить фразы, их версии, зависимости и editorial schedule. Оно **не** хранит второй независимый campaign lifecycle, бюджет или права партнёра. Собственные некampанийные заметки владельца не требуют фиктивной промо-кампании.

## 5. MCP: закончить существующий продукт и добавить Hero

Переиспользуй `private_events_mcp/event_create.py`, `event_create_adapter.py`, `partner_access.py`, `partner_tools.py`, `oauth.py`, `server.py`, `integration.py`, tool catalog/config/access policy и имеющиеся regression suites. Их наличие проверяется по свежей ветке. Не заводи второй server/process/listener/OAuth issuer, event DB, Smart Update, outbox или provider layer.

Доведи существующие owner/partner workflows: текст и афиша → ingestion/media → review при необходимости → accepted canonical ID → обычный fan-out → достоверный publication readback; отдельная promo операция → существующая campaign/activity → фактический execution/report. Реализуй необходимое durable recovery; network timeout не создаёт второй Event, campaign или publication. Source packet, media refs, review и operation status должны переживать restart.

Возьми актуальный полный v2 registry и привяжи его сценарии к стадиям R0 → R1 → R1b → R2 → R3 → R4. Пройди этапы по действующему safe-implementation contract, а не включай их одним флагом. Owner edit/lifecycle, receipts/reconciliation и связанные notices/history выполняй в их предусмотренном порядке. Не выдавай URL/время добавления события за подтверждение публикации. Не включай отдельные notices/badges, пока их prerequisites не закрыты. Уже готовые безопасные этапы можно выпускать раньше, но это не завершение всего поручения.

Hero MCP должен покрыть следующие действия; предлагаемые имена из исследования допустимы, но обязательны **возможности и совместимость**, а не ровно семь новых названий:

| Возможность | Обязательный результат |
|---|---|
| Current inventory/detail | Draft/ready/active/paused/expired, exact revision, фактическая доставка, campaign/activity binding, допустимые placements; не «один текст у всех» |
| Preview | Точные фразы/порядок/ссылки/картинка и результат выбора для заданного route/context; без exposure и публикации |
| Statistics | Actual period/data_as_of, impressions/CTA/действия, denominator/coverage, suppressions; operational health отдельно |
| Media input | File или разрешённый existing asset → проверенный долгоживущий ref; staged private asset не публичен до publish |
| Draft/assisted/verbatim | Тема либо exact copy, selected events, links, media, timeframe, placements; без скрытого включения кампании |
| Publish/edit/pause/resume/archive/rollback | Existing prepare/commit semantics, actor binding, expected revision, идемпотентность, exact readback |
| Operation status/recovery | Прогресс и terminal outcome существующей логической операции; unknown не означает безопасный повтор |

`verbatim` сохраняет текст, пунктуацию и порядок; не исправляет юмор и не генерирует short variant без разрешения. Не помещается — явная ошибка/предложенная новая версия, не обрезание. Если literal claim устарел — снять, а не переписать автоматически. Модель может предлагать, но не менять уже утверждённую версию под её старым ID.

Новые owner Hero operations добавляй в текущий full resource ChatGPT/OpenCode. Partner resource видит только разрешённое подмножество своих объектов; расширение global editorial прав требует явной owner policy. Существующая read-only Codex MCP projection остаётся прежней: это не ограничивает разработку кодовым агентом в checkout.

Используй текущие scopes/role/policy/operation patterns; новую capability family добавляй только при действительном отсутствии подходящей. Старые grants не получают write scopes молча. На каждом read/write/status/refresh повторно проверяй текущие права; suspend/revoke/credential rotation прекращают доступ, включая ранее подготовленные операции и поздний worker до mutation boundary. Не выполнять queued write от уже отозванного партнёра. Уже совершённый подтверждённый эффект не «отменять» догадкой.

## 6. Общий Hero renderer и автоматическое содержание

Три режима сходятся в одной библиотеке: automatic; owner brief → assisted draft; owner exact text → verbatim. Контент: события, фестивали, работающие функции, собственные новости, шутки/мемы, контекстное продолжение и промо через реальные activities. Изображение события связано с canonical объектом; owner meme/рисунок имеет явный editorial role, не fake event. Event Media gate, exact SHA/CDN и пригодная geometry переиспользуются.

LLM готовит заранее: детерминированный source shortlist → Writer → независимая semantic/style/chain проверка → validators → immutable pack. Нет LLM на page view/scroll/action. Числа, даты, цена, возраст, links и entities source-bound. Короткая связная цепочка 1–3 сцен, один доминирующий следующий шаг, связанные inline links допустимы. Отдельные short/normal variants сохраняют смысл/IDs. Модель/бюджет — через текущий разрешённый registry и общий atomic limiter, не прямые SDK/key calls. Source/model/prompt/schema/compiler fingerprints, bounded repair и warm replay без новых provider sends обязательны.

Реализуй гибридное обновление из исследования: утреннее/дневное пополнение и accepted-change triggers, bounded debounce/reconciliation и восстановление после restart в существующем scheduler. Начальные предлагаемые настройки: 06:00/16:00, календарный проход 00:05, debounce 5 минут/max15, reconciliation5 минут, всё в `Europe/Kaliningrad`. Это config defaults для проверки, не повод вызывать модель без новых данных. Research лимит64 attempts/day и конкретный model ID не становятся новой обязательной квотой: используй более строгие актуальные approvals/budgets и документируй фактическое решение.

Изменение lifecycle/цены/программы, campaign pause, activity OFF или capability OFF сначала запрещает несовместимое содержание, затем при необходимости запускает replacement. Никакая старая job не активирует отменённую revision. Новые тексты публикуются лёгким существующим managed-storage/runtime путём; full site build нужен для shell/нового public route, не для каждой реплики.

Immutable pack не равен актуальному разрешению показа. Реализуй bounded validity/control projection в существующем runtime/transport; initial freshness target — не более60 секунд старого разрешения в выполняющейся foreground-вкладке, без ожидания LLM/build. Candidate index и active revisions обновляются без нового Astro build. Control не кэшируется SW как вечный контент; expiry/offline/no-JS/BFCache/resume дают полезный evergreen fallback. Не обещать удалённо стереть замороженный кадр.

Сохраняй выбранную читаемую цепочку до явного продолжения/нового контекста; late reply не перетасовывает её. Safety revoke снимает согласованно copy/CTA/media без новой рекламной ссылки под пальцем. Typed fragments, finite cursor, first-tap, reduced-motion и terminal image сохранены.

`home_hero` и `page_end` используют общий resolver/render primitives. Page-end знает route/entity/action outcome: не дублирует related, не подтверждает save до ACK, не рекламирует вместо search recovery. Нет рекламы неготового voice/artifact/club. Home hero-event входит в общий бюджет30 и не дублируется в начале feed. Search/quick nav/mobile bottom nav сохраняются. Служебные страницы исключены, общие Floating Islands и две колонки выходных не перерабатывать ради этой задачи.

Astro/UI SoT/Penpot обновляются согласованно в том же change package на одном fixture corpus. Начни с фактического согласованного Hero donor/component registry; не придумывай новую версию/визуальное approval. Не откладывай SoT «на потом» и не выдавай недоступную Penpot-проверку за PASS.

## 7. Статистика и наблюдаемость — часть работающего результата

Подключи Hero к существующим product analytics и promo reports. Qualified exposure — реальная допустимая видимость текста, не скачивание pack, каждый tile или скрытый page-end. Общий session/campaign cap действует между placements и remounts; точные units и scope видны в отчёте. Campaign exposure/click не обучает organic taste.

Сильные действия считаются из authoritative receipts; слабая visibility — через общий bounded consented pipeline. `unavailable`/`insufficient_data` не заменять нулём. При первом production запуске честное «данных ещё нет» допустимо, но обязательно проверить доставку маркированного smoke-event до агрегата: вечная заглушка stats не является реализацией. Smoke/preview/test не загрязняют production KPI. Не вводить новую clickstream DB, отдельный consent персонализации или raw profile/search text в telemetry.

Owner status различает database commit, pack readiness, active revision, публичную доступность и наблюдавшиеся показы. При отсутствии live readback — `delivery_pending/failed`, а не «готово». Диагностика причины непоказа для context fixture не выдаётся за измеренную долю посетителей.

## 8. Production — входит в это задание

После соответствующих tests/reviews сам выполни integration/merge и штатный поэтапный release. Не заканчивай на «нужен отдельный deploy prompt» и не проси повторно общее разрешение делать уже порученный production release.

Проверенный `docs/operations/release-governance.md` требует **`scripts/deploy_fly_main.sh` из clean exact `origin/main`**. Не прямой `flyctl deploy`, не GitHub Actions deploy, не self-hosted runner. Старое общее указание в AGENTS о прямом flyctl не переноси в задачу поверх адресного release contract. Перед фактическим запуском сверить свежую версию script/runbook. Новый related UI также нужно доставить штатным static-site publisher; один Fly deploy не обновляет Astro HTML, JS, CDN/SW или отдельный serverless runtime.

Порядок каждой выпущенной стадии:

1. Relevant mandatory tests и incident regressions; backup/recovery plan и additive migration rehearsal на копии, включая restart и совместимость прежнего binary.
2. Reviewed commits в main; чистая exact-SHA сборка. Никакой разработки на deploy checkout.
3. Deploy code с новой write capability OFF; startup/health/schema/version/read-only checks.
4. Isolated live acceptance через настоящий OAuth → HTTP MCP → DB/worker/provider boundary с test tenant/private destinations и без случайных публичных fake events.
5. Активировать прошедшие gates owner/partner/Hero возможности, обеспечить необходимые consent/token refresh; старые токены не расширять. Реальная интерактивная авторизация пользователя — точный blocker, если её нельзя выполнить законно инструментами.
6. Для Hero доставить frontend + control/worker + content, проверить настоящий production route и actual MCP readback; сохранить component/pack/campaign/activity/build IDs.
7. Проверить pause/revoke/rollback, переживание restart, queues и отсутствие регрессий действующих promo activities/бота/сайта. Затем следующая стадия без остановки на промежуточном checkpoint.

Production code/capability release не даёт разрешения публиковать произвольные коммерческие тексты, рассылать реальные сообщения партнёрам, менять бюджеты/тарифы или оживлять paused кампании. Используй уже одобренные публичные материалы и isolated fixtures. Для новой конкретной публичной кампании/визуального owner sign-off нужен его фактический approval; отсутствие такого approval укажи точно и продолжи остальную независимую работу, не объявляй весь релиз запрещённым автоматически.

Flags OFF — безопасный deploy step, но не конечный результат. Если обязательная стадия действительно blocked, сохрани код/тесты, точное evidence, выполненные release stages и единственное необходимое действие владельца. Не обходи blocker скрытым снижением safety gate и не называй весь продукт DEPLOYED_ACCEPTED.

## 9. Приёмочные проверки

Сохрани IDs `HT-AF-01…22` и зарегистрируй сценарии в существующем registry; не создавай новый test framework. Приёмка ранее не запускалась этим документом.

| ID | Обязательная проверка |
|---|---|
| HT-AF-01 | Cold/warm compile: same plan/hash, warm0 model sends/новых content writes |
| HT-AF-02 | Invalid model output/facts/links/truncation/quota → bounded retry/defer, valid fallback |
| HT-AF-03 | Cancel/reschedule/price/identity change → старый dependency больше не получает permit |
| HT-AF-04 | Kaliningrad day/start/deadline boundaries без LLM |
| HT-AF-05 | Pause/OFF/owner edit vs late job → no stale activation |
| HT-AF-06 | Новое событие живой программы входит без ручного списка IDs |
| HT-AF-07 | Verbatim exact text/order/identities через edit/render/rollback, stale literal не переписывается |
| HT-AF-08 | Exact photo/meme role, права, entity/crop match, missing-media fallback |
| HT-AF-09 | Stale index, publish crash, missing pack, live readback и обновление без full rebuild |
| HT-AF-10 | SW/offline/hidden/resume/BFCache/clock jump, измеренная граница freshness |
| HT-AF-11 | No-JS/reduced-motion/mobile/desktop, first-click/finite cursor, navigation и layout |
| HT-AF-12 | Общий home/page-end/cross-tab cap, no duplicate exposure from remount |
| HT-AF-13 | Page/action context, pending/success/undo/search error, без false success/duplicate related |
| HT-AF-14 | Disabled/mastered/dismissed capability, absent delta/cohort → честный fallback |
| HT-AF-15 | MCP auth/actor/resource/revision/idempotency/timeout, no duplicate mutation |
| HT-AF-16 | Prompt injection/HTML/URL/private assets → no execution/SSRF/secret leak |
| HT-AF-17 | Analytics denied/down/delayed/test → корректные coverage/denominators, site работает |
| HT-AF-18 | Promo units/caps: не смешивать публикации и browser impressions |
| HT-AF-19 | Additive schema, previous binary/restart/worker lease recovery |
| HT-AF-20 | Astro=executable SoT=Penpot на общих facts/media/state fixtures |
| HT-AF-21 | Полный owner Hero путь, включая production readback |
| HT-AF-22 | Полный automatic путь, source update, нагрузка и actual model/control budget |

Дополнительно к существующему MCP v2 registry добавь release regressions **MCP-HT-01…06**:

| ID | Сквозной сценарий |
|---|---|
| MCP-HT-01 | Existing campaign с работающей другой activity → добавить Hero → обе видны в том же campaign → выключить только Hero → исходная activity неизменна |
| MCP-HT-02 | Campaign pause из существующего `/promo`/shared service → Hero снимается; resume проходит current gates; отдельный Hero state не противоречит campaign |
| MCP-HT-03 | Owner назначил двух изолированных партнёров → OAuth без Telegram → event+афиша/review/accepted ID → publication status → отдельное promo действие; чужие IDs/read/status запрещены |
| MCP-HT-04 | Partner подготовил write → owner изменил portfolio/права или suspend/revoke → commit/refresh/queued worker запрещены до mutation boundary; другой tenant продолжает работу |
| MCP-HT-05 | Existing owner/partner MCP + Telegram/VK/manual intake + старые promo activities + Codex readonly projection не регрессировали после общего release |
| MCP-HT-06 | Production exact main/backend/site versions → authenticated tools/capabilities → реальные операции/readback → marked telemetry receipt → pause/restart/rollback; не только local mock |

Обязательные end-to-end приёмочные истории: owner Hero inventory→asset→verbatim→preview→publish→production page→stats→edit→pause→generic→validated rollback; canonical event/program→auto copy→active pack→page; existing campaign→Hero activity→common pause; partner lifecycle→event/media→publication→promo→access revoke.

## 10. Что вернуть в конце

В GitHub сохранить код, schemas/prompts, tests, обновлённые canonical docs/routes/CHANGELOG и release evidence; отчёт в #643, ссылки из текущего MCP stack. Owner уточнения про activity/reuse/production перенести в canonical Hero/MCP/promo docs, не оставлять только в комментарии.

Финальный отчёт: использованные/merged PR и SHAs; что именно дописано; реально выполненные tests и terminal status; deployed backend/site/pack versions; включённые capabilities/actions; проверенные owner/partner journeys; public Hero URL; как владельцу открыть current/stats/edit; rollback target; фактические расходы/лимиты; оставшиеся конкретные blockers. Secret MCP paths, login codes и токены не публиковать в GitHub.

`SOURCE_READY`, `ISOLATED_LIVE_VERIFIED`, `DEPLOYED_ACCEPTED` различать. Нельзя назвать весь продукт готовым, если партнёр всё ещё read-only, Hero только в preview, новые actions остаются OFF, stats всегда stub, кампания продублирована в Hero DB, pause ждёт LLM/full build или новая функция не дошла до production.

Работай до этого результата, малыми проверяемыми шагами. Не делегируй владельцу ручной перенос JSON, SQL, merge и штатный deploy, которые доступны твоему окружению. Экономь модельный ресурс соразмерно задаче; не создавай дополнительный оркестратор ради продолжения существующей разработки.
