# Постановка на реализацию: Hero-talk AutoFill + Owner MCP

> Тип: продуктовая задача кодовому агенту для реализации, не повторный research prompt и не поручение автономно менять production.
> Основание: [исследование #642](autofill-mvp.md), [owner MCP contract](owner-mcp-mvp.md), [Hero owner #291](https://github.com/onedayonemasterpiece/events-bot-new/pull/291), [home #641](https://github.com/onedayonemasterpiece/events-bot-new/pull/641).
> Все проверки ниже — **критерии будущей приёмки**, не уже полученные PASS. Исследование не выполнило runtime implementation, model/browser/load probes или deployment.

---

Ты реализуешь в `onedayonemasterpiece/events-bot-new` работоспособное автонаполнение общего Hero-talk и управление им владельцем через EventsBot MCP.

## Продуктовый результат

Сайт сам пополняет библиотеку коротких связанных рассказов из canonical событий, живых программ, разрешённых кампаний и действительно работающих возможностей. На главной и в конце пользовательских страниц показывается подходящая цепочка: краткие фразы со ссылками и мозаика одного соответствующего изображения. Владелец через MCP читает текущее наполнение/статистику, добавляет свои темы, точные формулировки и цепочки, события, мемы/шутки/картинки, публикует, редактирует, ставит на паузу и откатывает версии.

Законченная задача — **не таблицы и tools/list**, а наблюдаемый цикл от подготовки до реального renderer и проверенного отзыва. Автоматическая и ручная поставка сходятся в одном механизме. LLM никогда не вызывается на просмотр/скролл/открытие page-end/save action.

## 1. Перед изменениями

Полностью прочитай два документа выше, актуальные AGENTS.md, docs README/routes и владельцев Hero, media, promo, analytics, onboarding, private MCP, LLM gateway и static build. Исследовательские исходники перечислены в §14 проекта; не повторяй широкое исследование вместо разработки.

Fresh-read main, #291, #641, #587 и текущий partner/event MCP release. Зафиксируй actual SHAs. Исследование смотрело main `6fddf14aeb983f97bde96e5963e1c9a9ddf72590`; это не требование откатить более новый код к этому SHA. #291 — августовский документационный stack на onboarding branch. **Не начинай runtime-разработку от старой ветки и не merge её целиком.** Создай рабочую ветку от актуального main, перенеси только принятые Hero documents/contracts с сохранением ссылок; сохрани параллельные улучшения Search/Floating Islands/MCP.

Ключевые реальные anchors, а не выдуманные готовые функции:

| Владелец | Найденный anchor | Изменение |
|---|---|---|
| Hero UI | `site/src/components/HomeHeroTalk.astro` | Migration donor → общий renderer + generic plan, без отдельного нового marketing hero |
| Canonical facts | `static_site_release.py`, public projection/revision; Smart Update fan-out | Dependency packets, hooks после accepted commit; без семантического перепарсинга всей базы |
| Сборка | `scripts/request_static_site_build.py` → `main.enqueue_static_site_build_request` | Shell/public route receipts; не full rebuild на каждую фразу |
| Media | `event_media.py`, EventPoster/approved projection | Reuse exact SHA/role/geometry/CDN, отдельная явная роль owner editorial image |
| Promo | `promo.py`, существующие campaign/target/activity services | Add `surface=hero_talk` и typed placement config; сохранить source ownership/caps/units |
| MCP | `private_events_mcp/integration.py`, `server.py`, config/access policy | Owner tools внутри existing aiohttp/OAuth; не менять Codex readonly surface |
| LLM | `google_ai/client.py` и shared limiter | Dedicated approved Hero Writer/Reviewer policy; no raw SDK/bypass |
| Аналитика | общий site analytics transport/registry и product action receipts | Bounded session summaries и агрегатное owner readout, не новая clickstream DB |

Названия новых модулей (`hero_talk/`, `site/src/lib/heroTalk/` и т.п.) выбери по актуальной структуре. Не создавать второй scheduler/server/outbox/promo model/profile. Изменение публичного UI означает одновременное обновление executable UI SoT и его проекции: A=S=P проверяется на **одном замороженном корпусе**, не по unrelated screenshots.

## 2. Инварианты, которые нельзя потерять

- Один механизм, `home_hero` и typed `page_end`; служебные страницы исключены. Search, quick nav и mobile bottom navigation сохраняются. Не перерабатывай двухколоночные выходные и общие острова ради этой задачи.
- 1–3 сцены одной цепи, finite cursor, stable first-click links, useful first HTML. Есть normal/short variants и точные identities. Verbatim не перезаписывается/не обрезается; owner-provided short variant только явно.
- Home concrete event входит в общий task budget 30 и не дублируется в feed. No autoplay unrelated campaigns; page-end не второй related feed и не реклама вместо empty/error recovery.
- Только source-bound copy/facts/links/media. Canonical hide/lifecycle/expiry/capability сильнее pin/priority. Фото, текст и CTA атомарны. Мем/иллюстрация имеет редакционный origin, не fake event.
- Own editorial promo включается через существующую activity. Один campaign qualified exposure/session суммарно home/page-end; точные глобальные caps без подходящего accounting — fail-closed. Не смешивать publication count и browser impression. Paid/partner/legal release отдельно.
- Generic evergreen first scene, immutable packs и свежий bounded permit — разные объекты. No-store control + explicit SW network-only. Новые candidates узнаются через `inventory_revision/index_ref` из control; без нового Astro build.
- Без permit/JS/сети — полезная generic сцена; page-end без полезного валидного следующего шага может отсутствовать без пустого блока. Не показывать старую кампанию под видом last-good. Server revoke не ждёт Writer.
- Не вводить новый consent-переключатель персонализации. Analytics consent и product/action state остаются у общих owners; отсутствие analytics не ломает сайт.
- No secrets/PII/raw profile/query/private brief в public pack или telemetry. Existing scoped OAuth плюс новая Hero capability family; tool hints не авторизация.

## 3. Реализация последовательными проверяемыми пакетами

Пакеты — порядок разработки, не разрешение остановиться после первого. Сохраняй код/тесты регулярно. При действительном blocker сохраняй законченный результат, точный дефект зависимости и runnable handoff; не объявляй проект готовым по одной схеме.

### A. Общий domain и управляемые дословные цепочки

Реализуй typed program/chain/fact/media/permit schemas, validators, deterministic compiler и небольшое additive storage. Как стартовая форма предложены `hero_talk_program` + `hero_talk_change_log`; уже существующий подходящий ledger переиспользуй. История и source hashes immutable; status/active pointer меняются CAS. Нет новых несовместимых `JobTask/JobStatus` values, сетевых вызовов внутри transaction и массовых startup backfills.

Добавь семь tools из owner contract. Draft/read/preview не публикуют и не создают impressions. `prepare/commit/operation_get` имеют actor binding, exact digest, expiry, expected revision, idempotency/recovery. Asset staging проверяет файл, права и долговечный controlled ref; transient social ref не public URL. Пауза эффективна сразу в DB state. В этом пакете уже должен пройти полный локальный verbatim create/edit/pause/rollback тест, не требующий LLM.

### B. Общий renderer и лёгкая доставка

Реализуй immutable packs/index в existing object publication abstraction и bounded live eligibility endpoint в существующем runtime/OperationCatalog. Продолжительность permit ≤60 s; refresh 30 s только для видимого foreground, единый page coordinator. Source revisions проверяются при новом permit; смена `inventory_revision` открывает новый index. Старый pack не становится новым active сам по себе.

Подключи home и page-end к одним schemas/resolver/render primitives. Context matrix: home, event/festival event, collection, today/tomorrow/weekend/date, search results/empty/error, For Me, club — только реально существующие public routes. Учитывай ACK/undo, exact page entity, current constraints, уже показанные related blocks и общие task/campaign caps. Optional dependencies дают честный fallback, а не требуют внедрить весь голосовой поиск в этот PR.

Сохрани fonts/icons/crops/geometry из общего DS. Мозаика одной image без fetch на каждый tile, lazy page-end image, terminal persistence. Update Astro + executable SoT + Penpot projection contract совместно, на общем fixture corpus. Отдельно различай automated parity и owner visual acceptance; отсутствие доступа к Penpot не называй A=S=P PASS.

### C. Автоматическая поставка

Реализуй source adapters к canonical events/programs, promo и released capabilities; проверяемый shortlist, writer/reviewer task prompts, fact/link slots, source fingerprint cache. Расписание и debounce из проекта: 06:00/16:00 generation delta, 00:05 time eligibility, accepted-update debounce 5 min/max15, reconcile каждые5 min. Existing scheduler drain, durable lease/backpressure, no separate daemon/Kaggle runtime.

Нормальная модельная работа 2 calls/brief; один repair с повторным review, максимум4. Initial feature ceiling64 physical attempts/UTC day плюс общий provider limiter. Writer policy новой публичной поверхности включать только после owner sample acceptance. Реальный registry/config может опередить исследовательский model ID: зафиксируй разрешённую explicit mapping, не переключай модель молча. Identical warm replay — 0 provider sends и 0 новых content writes.

Раздели exact value rebinding и semantic change; semantic decision не заменять regex. Critical accepted fact change отзывает прежнюю eligibility сразу, а replacement генерируется позже. Retry stale job не восстанавливает paused programme. Owner-locked тексты не переезжают от auto refill.

### D. Кампании, статистика и продуктовая приёмка

Добавь Hero activity к действующему promo owner, dynamic festival membership и inherited pause/window/target restrictions. Programme/placement pause и global campaign pause имеют явный разный scope. Promo resolver не принимает unsupported exact reach cap и не обновляет legacy video exposure units browser clicks.

Интегрируй observed events в общий analytics pipeline, с существующими privacy/transport/retention budgets. Добавь только необходимые bounded fields. `hero_talk_stats` показывает coverage, denominators, actual data_as_of, technical metrics, sampled suppression, unavailable/insufficient вместо выдуманного нуля. Product actions берутся из authoritative receipts, не из optimistic click.

Собери isolated candidate с реальными renderer/worker/MCP adapters и замороженным небольшим корпусом. По свежим исходникам соблюди обычные release gates; флаги production default-off. Canary/публикация корня требуют отдельного owner authorization — выполнение этой постановки само по себе не даёт его.

## 4. Минимальная acceptance matrix

Идентификаторы `HT-AF-*` добавить в существующий central static-site scenario registry. Это не новый test framework. Pure tests в pytest/существующем JS runner, browser cases в текущем Playwright pipeline. Числа ниже — ожидаемые assertions, не результаты настоящих прогонов.

| ID | Сценарий | Что должно быть доказано |
|---|---|---|
| HT-AF-01 | Cold compile / exact warm replay | Same normalized pack/plan hash; warm0 provider sends,0 content writes |
| HT-AF-02 | Writer/Reviewer invalid JSON, invented fact/link, truncation, quota | Invalid не active; bounded retry/defer, last-valid только compatible, generic сохраняется |
| HT-AF-03 | Event cancelled/rescheduled/price changed/identity merged | Old dependent version перестаёт получать permit; review не обязателен для отзыва |
| HT-AF-04 | Event start/deadline/day boundary | Fake clock Kaliningrad; expired/date-sensitive narrative не переживает окно; provider0 |
| HT-AF-05 | Campaign pause/activity off/owner edit во время generation | Late worker CAS → superseded; не восстанавливает старый state |
| HT-AF-06 | Новое событие живой festival programme | Dynamic membership попадает в следующий пригодный compile без ручной правки ID list |
| HT-AF-07 | Verbatim lifecycle | Exact Unicode text/order/IDs после save/preview/render/edit; old literal false claim блокируется, не переписывается |
| HT-AF-08 | Owner meme image / missing photo / wrong entity/crop | Явная editorial role; exact source; unknown rights и cross-event ref не публикуются; text fallback |
| HT-AF-09 | Fresh control, stale CDN index, missing pack, publish crash | No stale authorization; active только после hashes/readback; inventory refresh без Astro rebuild |
| HT-AF-10 | PWA Cache API/offline/hidden/restore/BFCache/clock jump | SW не сохраняет permit; generic на resume до проверки; awake stale exposure ≤60 s; без LLM |
| HT-AF-11 | No-JS/reduced motion/320–390 mobile/desktop | Полезный static текст и доступные ссылки; no infinite cursor/CLS/crop break; navigation не перекрыта |
| HT-AF-12 | Home/page-end/cross-tab/same chain remount | Один campaign session exposure; unseen page-end не impression; недоступный coordinator не снимает cap |
| HT-AF-13 | Page-end matrix, save pending/success/undo, search error | Exact context; no false success, no duplicate related, no promo instead of recovery |
| HT-AF-14 | New capability disabled, mastered, dismissed; return delta absent | Suppression работает; нет обещания неготового voice/artifact/club; count и destination один cohort |
| HT-AF-15 | MCP auth/CAS/idempotency/expired prep/timeout | Wrong principal/resource/Codex denied; один commit/generation; status read не повторяет write |
| HT-AF-16 | Prompt injection / arbitrary URL/HTML / private asset leak | Данные не выполняются; SSRF/escaping/scopes enforced; no secret fields в public objects |
| HT-AF-17 | Analytics denied/down/delayed + тестовые показы | Сайт работает; корректные unknown/coverage/denominator; tests/preview не production statistics |
| HT-AF-18 | Cap units / unsupported global cap | Не складываются publication и viewer units; unsupported accounting даёт явный fail-closed |
| HT-AF-19 | Migration restart/old binary read/worker lease recovery | Additive compatibility, no hot-table backfill/new enum writes; durable recovery без duplicate generation |
| HT-AF-20 | Общий fixture Astro ↔ executable SoT ↔ Penpot | Одинаковые object/chain/media IDs и состояния; фактическая геометрия/экспорт, не synthetic approval |
| HT-AF-21 | Full owner product journey | Inventory→asset→verbatim→preview→publish→page→stats→edit→pause→generic→validated rollback |
| HT-AF-22 | Full automatic product journey + operational load | New canonical programme→W/R→pack→real renderer; model/read/byte budgets и permit SLO измерены |

Искусственные события/кампании/ассеты явно fixture-only, не старые production dates. Нужны future, ongoing, ended, cancelled, changed-price, dynamic festival, disabled capability, owned meme, no-media, hidden entity и no-analytics случаи. Не надо огромной synthetic персональной базы, чтобы проверить эти инварианты.

## 5. Что вернуть владельцу и следующему release-agent

Сохрани код, prompts, schemas, tests и обновлённые owner docs в GitHub. В отчёте дай exact commit/PR, команды реально выполненных проверок, PASS/FAIL/BLOCKED по матрице, provider sends, hashes, preview/candidate identity, migration/rollback instructions и diff scope. Нельзя выдавать спроектированные tests или mock-provider review за live model/browser acceptance.

Покажи несколько конкретных цепочек: catalog event, festival/own promo, working capability, contextual result/recovery и дословную owner joke с мозаикой. Визуальные доказательства должны соответствовать тем же фактам/ассетам, что в SoT. Дай владельцу реальные MCP операции чтения/изменения, либо честно укажи, что текущий сервер ещё не обновлён.

Не закрывай задачу как реализованную, если нет B–D, если publish заканчивается только в БД, если статистика всегда synthetic, если page-end отсутствует, если pause ждёт полной сборки или если exact copy переписывается моделью. Отдельно перечисли optional dependencies, которые корректно gated, и реальные blockers, мешающие обязательному результату.

Если работа идёт в ChatGPT с coding agent, аналитические/редакторские задачи допустимо вынести в отдельное окно только явным bounded handoff с полным контекстом; это не условие завершения. Для Codex выбирай минимально достаточную модель/reasoning по доступному контракту, не назначай автоматически максимальный режим. Не создавай новую систему оркестрации вокруг небольшой функции.
