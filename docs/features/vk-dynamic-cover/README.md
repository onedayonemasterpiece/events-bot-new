# VK Dynamic Cover

Каноника MVP для автоматической обложки VK-сообщества.

## Цель

- Временно подсвечивать важные фестивали/промо в шапке VK-сообщества, не теряя связь с визуалом `Полюбить Калининград`.
- Генерировать wide cover `1920×768` для desktop VK и mobile slide assets `1080×1920` для будущих живых обложек.
- Давать суперадмину ручной контроль через `/cover`, включая выключение автоматики поверх промо.

## MVP Поведение

- `/cover status` показывает включена ли автоматика, срок активной временной обложки и размер истории.
- `/cover preview` генерирует wide PNG и первые mobile PNG и отправляет их в Telegram как документы без пережатия.
- `/cover apply` выбирает до трёх актуальных фестивалей из `Festival`, генерирует wide/mobile pack, публикует wide cover через VK owner-cover upload flow и записывает историю.
- `/cover save_default` скачивает текущую VK-обложку сообщества и сохраняет её на сервере как дефолт для восстановления.
- `/cover restore` восстанавливает сохранённую дефолтную обложку. Если дефолт ещё не сохранён, команда не подменяет его шаблоном и просит сначала сохранить текущую обложку.
- `/cover on` / `/cover off` включает или выключает автоматику; при `off` промо/cron не должны менять обложку.
- `/cover history` показывает последние смены.
- В сообщении `/cover status` есть inline-кнопки для preview/apply/save default/restore/on-off/history/status.
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

## Проверки

- Unit: `tests/test_vk_dynamic_cover.py`.
- Live smoke перед production apply: `/cover save_default`, `/cover preview`, визуально проверить wide/mobile PNG, затем `/cover apply` на целевом VK-сообществе, убедиться, что wide cover сменилась, и проверить `/cover restore`.
