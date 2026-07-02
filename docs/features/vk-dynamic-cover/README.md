# VK Dynamic Cover

Каноника MVP для автоматической обложки VK-сообщества.

## Цель

- Временно подсвечивать важные фестивали/промо в шапке VK-сообщества, не теряя связь с визуалом `Полюбить Калининград`.
- Генерировать wide cover `1920×768` для desktop VK и mobile slide assets `1080×1920` для будущих живых обложек.
- Давать суперадмину ручной контроль через `/cover`, включая выключение автоматики поверх промо.
- До отдельного approval-flow не менять VK-обложку автоматически: генерация отправляется
  в Telegram админу как proposal без публикации.

## MVP Поведение

- `/cover status` показывает включена ли автоматика, срок активной временной обложки и размер истории.
- `/cover preview` генерирует wide PNG и первые mobile PNG и отправляет их в Telegram как документы без пережатия.
- `/cover request` выбирает до трёх актуальных фестивалей из `Festival`, генерирует wide/mobile pack и отправляет proposal в Telegram админу без VK upload.
- `/cover apply` в текущем MVP намеренно оставлен безопасным alias к `/cover request`: он генерирует proposal, но не публикует VK cover. Прямой upload должен появиться только через отдельный approval-flow с явным подтверждением.
- `/cover save_default` скачивает текущую VK-обложку сообщества и сохраняет её на сервере как дефолт для восстановления.
- `/cover restore` восстанавливает сохранённую дефолтную обложку. Если дефолт ещё не сохранён, команда не подменяет его шаблоном и просит сначала сохранить текущую обложку.
- `/cover on` / `/cover off` включает или выключает автоматику; при `off` промо/cron не должны менять обложку.
- `/cover history` показывает последние смены.
- В сообщении `/cover status` есть inline-кнопки для preview/request/save default/restore/on-off/history/status.
- Scheduler `vk_dynamic_cover_expiry` раз в час проверяет `vk_dynamic_cover_active_until`; после истечения срока возвращает сохранённую дефолтную обложку.

## Дизайн

- Wide cover использует левый editorial brand-block с референс-логотипом из `docs/backlog/features/vk-dynamic-cover/photo_2025-02-02_11-08-25.jpg`.
- Фестивали раскладываются в диагональные панели с badge-периодом, крупным названием, короткой подписью и тонкими акцентными разделителями.
- Палитра единая для всего pack: глубокий editorial dark/navy/green, тёплый акцент только для дат, правил и небольших маркеров. Это снижает визуальный шум и держит обложку профессиональной.
- Mobile assets сохраняют примерно 20% brand-связи через логотип/подпись; первый slide работает как общий editorial cover недели, следующие slides — по одному фестивалю с асимметричной композицией.

## VK Upload

- Wide cover публикуется через `photos.getOwnerCoverPhotoUploadServer` -> upload `file` -> `photos.saveOwnerCoverPhoto`.
- Для upload нужен user token (`VK_USER_TOKEN`, локальный fallback `VK_ACCESS_TOKEN4`), потому что VK photo/upload методы часто недоступны group token.
- Target group: `VK_DYNAMIC_COVER_GROUP_ID`, fallback `VK_EVENTS_GROUP_ID`, затем `VK_AFISHA_GROUP_ID`.
- Сохранение дефолта читает текущую обложку через `groups.getById(fields=cover)`, выбирает самое крупное изображение, нормализует его в `1920×768` JPEG и кладёт в `VK_DYNAMIC_COVER_STORAGE_DIR` (`/data/vk_dynamic_cover` на production, локально `artifacts/codex/vk-dynamic-cover`).
- В текущем proposal-only MVP upload path используется только для `/cover restore`
  сохранённого дефолта и для будущего approval-flow. `/cover request` и `/cover
  apply` VK API не вызывают.

## Festival Data MVP

- Первый целевой набор: `80 историй о главном` и `Кантата`.
- Production-аудит 2026-06-07 показал split между событиями и справочником:
  обе сущности есть в `event.festival` и `festival_queue`, у `80 историй о
  главном` также есть активная `promo_campaign`, но строк в таблице `Festival`
  для `80 историй о главном` и `Кантата` нет. Поэтому `/fest` их не показывает,
  а текущий renderer, который читает `Festival`, не может использовать их как
  полноценные фестивали.
- Причина: `/fest` строится только по таблице `Festival`; `ensure_initial_80_stories_campaign`
  создаёт промо-таргет по future events, но не материализует `Festival`. Авторан
  `festival_queue` на production выключен по умолчанию, поэтому pending rows не
  превращаются в справочник.
- Для MVP нужен targeted light-monitor: брать whitelisted festival labels из
  событий/очереди, материализовать или обновлять `Festival`, подтягивать сайт,
  VK/social links, logo, базовую палитру и период. Запуск должен быть
  ограничен whitelist и `--limit`, без полного прохода по старой очереди.
- Варианты названия `Кантата` (`VI Международный фестиваль классической музыки
  «Кантата»`, `Кантаты`, `Кантата.Россия`) нельзя сливать регулярками вслепую:
  normalizer должен давать confidence и писать evidence, какие события/источники
  привязаны к каноническому фестивалю.

## Kaggle Light Monitor Guardrails

- Существующий Universal Festival Parser сейчас нельзя запускать как основу MVP
  без аудита: код и документация всё ещё помечены как parser version `1.0.0` и
  описывают Gemma 3-27B, тогда как актуальные лимиты/модели GoogleAI уже другие.
- В `kaggle/UniversalFestivalParser/src/rate_limit.py` есть token-bucket limiter,
  но для нового light-flow этого недостаточно: нужны hard `--limit`, `--timeout`,
  dry-run, whitelist фестивалей, сохранение `rate_usage.json` и fail-fast при
  quota/rate-limit errors без бесконечного retry.
- Light-monitor не должен расходовать массовую LLM-квоту, пока для 80/Kantata
  достаточно deterministic enrichment: VK `groups.getById` для логотипа, Pillow
  quantize для палитры, базовые факты из event/festival_queue/source links.

## State

MVP не добавляет таблицы и хранит состояние в `setting`:

- `vk_dynamic_cover_enabled`
- `vk_dynamic_cover_active_until`
- `vk_dynamic_cover_last_state`
- `vk_dynamic_cover_history`
- `vk_dynamic_cover_default_state`

## Ограничения

- Mobile live covers в MVP только генерируются и отправляются в preview. Их автоматическая публикация в VK намеренно не включена до отдельной проверки стабильного API/прав доступа для live cover slots.
- Извлечение логотипов фестивалей из VK/сайтов и видео/футажей остаётся следующим этапом. Сейчас используются текстовые фестивальные панели и имеющиеся `Festival` metadata.
- Wide cover proposal не считается согласованной или опубликованной обложкой,
  пока нет отдельной кнопки approve и записи approval-state.

## Проверки

- Unit: `tests/test_vk_dynamic_cover.py`.
- Live smoke: `/cover save_default`, `/cover preview`, `/cover request`,
  визуально проверить wide/mobile PNG в Telegram. До реализации approval-flow
  `/cover apply` должен вести себя так же, как request, и не менять VK cover.
  `/cover restore` проверяется отдельно как аварийное восстановление сохранённого
  дефолта.
