# Видеогайды «Как быстро найти событие»

> Статус: **post-release planned**.
> Product: сайт «Полюбить Калининград Анонсы» (`kenigevents.ru`).
> Release dependency: стабильный production static site, завершённый D10 Telegraph
> cutover и зафиксированные navigation/search/event-action flows.

## Цель

После публичного релиза выпустить короткие понятные видеогайды, которые показывают,
как быстро найти подходящее событие на сайте без изучения всех разделов вручную.
Гайды не являются release blocker самого сайта: они снимаются только на уже
стабильном production-интерфейсе, чтобы не устареть до публикации.

## Первая серия

1. **Быстрый старт:** `Сегодня`, `Завтра`, `Выходные`, категории и переход к событию.
2. **Точный поиск:** как сформулировать запрос по жанру, времени, месту, цене или
   признаку «бесплатно» и уточнить результат.
3. **Сохранить найденное:** как открыть страницу нужного occurrence, добавить в
   избранное/календарь и воспользоваться доступными действиями события.

Если отдельная функция ещё не прошла production acceptance, её нельзя показывать
как доступную. Персональные рекомендации и авторизация входят только после своего
стабильного релиза.

## Формат

- основной mobile vertical master для коротких публикаций;
- отдельная desktop запись там, где путь действительно отличается;
- один ролик решает одну задачу и показывает путь от намерения до открытой страницы
  подходящего события;
- русский текст/озвучка и обязательные читаемые субтитры;
- видимые taps/cursor, спокойный темп, без скрытых или смонтированных «успешных»
  состояний;
- бренд и название: **«Полюбить Калининград Анонсы»**.

Точные длительность, aspect variants и каналы публикации утверждаются вместе со
storyboard. Один исходный master может давать версии для сайта, Telegram, VK и MAX,
но публикация в каждый канал требует отдельного preview/owner approval.

## Production safety

- записывать только canonical production `https://kenigevents.ru/`, не preview/lab;
- использовать чистый тестовый профиль и не показывать email, токены, персональные
  интересы, admin UI, private URLs или уведомления других пользователей;
- выбирать реальные актуальные события с запасом по дате либо использовать
  утверждённый evergreen demonstration path;
- URL, подписи и CTA должны совпадать с фактическим интерфейсом;
- устаревший ролик снимается с публикации или получает замену после изменения
  навигации, поиска, названий действий или canonical URL policy.

## Acceptance и evidence

Для каждого ролика сохраняются:

- guide id/version, цель и target audience;
- production SHA/build/manifest и дата записи;
- storyboard, script и перечень показанных URL/actions;
- source master и hashes финальных exports;
- mobile/desktop QA, subtitles/transcript, privacy review;
- список approved publication targets и публичные URL;
- owner approval и дата следующей проверки актуальности.

Гайд принят, когда новый пользователь по нему воспроизводит сценарий на текущем
production-сайте, открывает корректное событие/occurrence, не сталкивается с
несуществующим control и не раскрывает персональные данные.

## Обновление и rollback

- изменение показанного flow создаёт review task по всем зависимым guide ids;
- старую версию не перезаписывать без version/evidence: сначала подготовить и
  проверить замену, затем обновить публикации;
- при критической неточности временно удалить/скрыть ролик и оставить текстовую
  инструкцию, а не направлять пользователей по неверному пути.

## Отдельный post-release workstream: видеоматериалы источников на event page

> Статус: **planned, после стабильного production release**. Это не часть
> видеогайдов выше и не blocker первого static-site release. Канонический
> контент события остаётся текстом и фактами; видео — только проверенное
> обогащение конкретного события.

### Цель

Находить в первичных/доверенных источниках событий выразительные ролики, доказывать
их связь с одним конкретным событием и показывать их на статической event page без
перепривязки к похожему фестивалю, площадке или прошлому выпуску.

Поток должен использовать уже существующий media intake (`event_media_asset`) как
хранилище/идемпотентный фундамент, но добавить для static export явный,
source-grounded video manifest. Он **не** означает, что любой ролик из канала
площадки можно автоматически использовать на каждой её карточке.

### Discovery и eligibility

1. По каждому подключённому источнику собирать видео-кандидаты вместе с source URL,
   сообщением/attachment id, SHA-256, MIME, размером, длительностью, native
   `width × height`, poster frame и временем публикации.
2. LLM-first review оценивает смысловую связь ролика с title/date/occurrence/venue
   события; детерминированно разрешается лишь измерить orientation, размер,
   дубликат и техническую пригодность. Результат хранит evidence span/source URL и
   один из verdict: `approved`, `needs_review`, `rejected`, `ambiguous`.
3. Автоматическое прикрепление допускается только для `approved` и ровно одного
   canonical `event_id`. Видео из multi-event анонса, общая атмосферная съёмка
   площадки, прошлогодний отчёт или ролик другого выпуска остаются
   `needs_review`/`ambiguous` и не попадают на страницу по совпадению названия
   площадки.
4. До публичной проекции обязательны права/terms-check для повторного хранения или
   embed, техническая проверка playable URL, poster frame, duration/size budget,
   вирус/unsafe-media gate и безопасный fallback без видео.

### Гипотеза показа по ориентации

| Исходник | Предлагаемый destination | Статус решения |
| --- | --- | --- |
| Горизонтальный (`width >= height`) | **candidate** в существующей media gallery/carousel рядом с фото | Гипотеза, а не дефолт. Нужны отдельные visual QA, accessibility/performance evidence и owner decision: горизонтальный ролик может оказаться неуместным в карусели. |
| Вертикальный (`height > width`) | Блок `Видео события` в теле описания, после source-grounded текста/фактов и до источников/CTA | Целевой post-release direction. Показывать только approved event-local ролики. |
| Квадратный/неизвестный/неподдерживаемый | Не выбирать по одному aspect ratio | `needs_review` либо обычная ссылка/fallback до принятого renderer rule. |

Ориентация — не оценка качества и не доказательство связи с событием. В частности,
вертикальный fire-ролик из канала фестивальной площадки нельзя автоматически
добавлять к другому, даже очень похожему фестивалю той же площадки.

### Контракт вертикального блока

- Заголовок блока: `Видео события`; каждое превью имеет source attribution,
  доступное имя и кнопку/контрол `Воспроизвести`.
- На desktop блок — responsive CSS grid: **2–3 ролика в ряд** (три при достаточной
  ширине и подходящем числе approved items; два — устойчивый fallback), без
  горизонтального скролла и без обрезания вертикального кадра.
- На телефоне — **минимум две колонки** для eligible vertical clips; карточка
  сохраняет вертикальное соотношение, touch target запуска остаётся доступным, а
  длинный список не превращается в бесконечную ленту. Точная минимальная ширина,
  подписи и fallback для очень узких экранов подтверждаются отдельной a11y/visual
  матрицей, но не должны молча регрессировать к одному ролику в ряд.
- Видео не autoplay и не включает звук без действия пользователя. Playback
  открывается inline или в доступном modal/fullscreen player; keyboard, focus
  return, captions/транскрипт при наличии, reduced-data fallback и poster-only
  состояние обязательны.
- Сначала в HTML выводятся title, attribution и poster; player/largest media bytes
  lazy-load только после user intent. Ошибка CDN/codec/source не ломает описание,
  даты, CTA или остальную media gallery.

### Этапы доставки

1. **Audit/shadow.** Собрать репрезентативный ledger по Telegram/VK/сайтам:
   vertical/horizontal, event-local verdict, duplicate/multi-event/fail reasons,
   размер, права и потенциальный destination. Никаких публичных вставок.
2. **Data contract.** Версионировать event-video manifest и export: provenance,
   `event_id`, verdict, orientation, role, poster/play URL, duration, captions,
   availability/expiry и content hash. Rebuild должен происходить при изменении
   approved set и не подменять уже опубликованное видео без revision/evidence.
3. **Renderer canary.** Сначала закрытый/static preview на small curated set;
   раздельно проверить горизонтальную gallery hypothesis и vertical description
   grid на 390 px, 768 px и desktop, с no-JS/error/slow-network/a11y cases.
4. **Public canary.** Включить малую approved выборку, измерить playback intent,
   failed starts, CLS/LCP/bytes, source mismatch reports и не ухудшить базовые
   event actions. Расширять только после owner approval и rollback drill.

### Release/rollback evidence

Для каждого публичного ролика нужны event URL/id, исходный public URL, review
verdict и evidence, manifest revision/hash, presentation role, права/terms verdict,
desktop/mobile screenshots, playable/error-path evidence и owner approval. Rollback
должен уметь удалить один asset/revision из manifest и пересобрать страницу без
удаления события, фото, source facts или других роликов.
