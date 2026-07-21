# Фирменная бирка на изображениях Telegram/VK

> **Статус:** planned for release; implementation/configuration is intentionally
> not started by this documentation task.
> **Activation:** `2026-07-30 18:00 Europe/Kaliningrad`
> (`2026-07-30T16:00:00Z`).

## Цель и scope

С указанного release timestamp все новые управляемые публикации отдельных
событий в Telegram `@kldevents` и VK `VK_EVENTS_GROUP_ID` должны использовать
фирменный derivative изображения с биркой **«Полюбить Калининград / Анонсы»**,
визуально совпадающей с утверждённой биркой мобильного сайта.

Бирка наносится только на каждое публикуемое изображение, для которого текущий
media gate достоверно подтвердил `image_text_mode=visual_only` и отсутствие
значимого OCR-текста. `ocr_text`, `unknown`, ошибка/устаревший OCR и документы
fail closed: они публикуются без бирки, а не получают рискованный overlay.

Исходное approved-изображение остаётся неизменным. Для social publication
создаётся отдельный content-addressed derivative с версией шаблона, source hash,
позиционированием и output hash. Один и тот же принятый derivative используется
Telegram и VK; двойное нанесение бирки запрещено.

## Визуальный и quality contract

- использовать canonical asset/tokens из
  [brand lockups](../static-site-pages/design-system/brand-lockups.md), а не
  рисовать похожий логотип независимо в social publisher;
- форма, цвета, typography и safe padding соответствуют mobile-site brand tag;
- положение выбирается только из безопасных углов/зон и не закрывает лицо,
  основной объект, focal/saliency region или важную event detail;
- если безопасной зоны нет, публикация использует оригинал без бирки;
- не увеличивать низкое разрешение и не ухудшать исходник повторным циклом
  JPEG; output сохраняет достаточное разрешение для full-screen Telegram/VK;
- края, alpha, текст и логотип остаются резкими на 1×/2× просмотре; нет matte,
  halo, banding, clipped corners, растяжения или цветового сдвига;
- бирка читается в Telegram/VK thumbnail, но не конкурирует с событием;
- multi-photo post обрабатывает каждое eligible изображение отдельно;
- renderer deterministic: одинаковые source/template/placement inputs дают тот
  же output hash.

До активации owner принимает SHA-bound visual matrix как минимум на landscape,
portrait, square, тёмной/светлой, face-heavy и low-resolution фотографиях. Для
каждого примера сохраняются source/output dimensions и hashes, OCR verdict,
placement, full-size PNG/WebP/JPEG comparison и реальные Telegram/VK screenshots.

## Activation contract

Планируемый режим имеет состояния `off -> shadow -> on` и отдельный аварийный
rollback `on -> off`. Activation timestamp хранится с timezone и годом,
проверяется в UTC и `Europe/Kaliningrad`; сравнение только по строке или локальному
server timezone запрещено.

На границе времени:

- только новые `tg_event_publish`/`vk_sync` используют branded derivative;
- pending jobs проверяют effective publish time, а не время создания job;
- старые публичные посты массово не редактируются;
- Telegram и VK включаются одним release decision, но остаются независимыми
  delivery surfaces: сбой одной сети не блокирует вторую;
- отключение возвращает publishers к исходным approved images без удаления
  source/derivative/evidence объектов.

## Предрелизный VK postponed smoke

До production activation разрешён тест только через VK postponed queue:

1. выбрать approved future event и по одному representative image class;
2. создать managed VK-пост с publish time достаточно далеко в будущем;
3. через VK API проверить community author, текст, attachments, full-size image,
   отсутствие OCR-overlay, резкость/цвет/placement и postponed status;
4. сохранить post id, source/output hashes и screenshot evidence;
5. удалить тест из отложки и повторно подтвердить через API, что postponed/live
   post отсутствует.

Тест не должен успеть стать публичным. Если он вышел live, это не считается
успешным smoke: пост удаляется, случай фиксируется как release blocker. Telegram
до включения проверяется offline/private preview; публичный `@kldevents` test post
этой задачей не разрешён.

## Acceptance и rollback

Release gate закрыт, когда:

- activation зафиксирован как `2026-07-30 18:00 Europe/Kaliningrad` /
  `2026-07-30T16:00:00Z`;
- eligibility использует текущий OCR/media verdict, а `unknown` fail closed;
- visual matrix принята без размытия, перекрытий, crop/face/OCR дефектов;
- VK postponed smoke создан, проверен и удалён с API evidence;
- Telegram/VK payload parity и independent-failure behavior проверены;
- activation и rollback rehearsal привязаны к clean `origin/main` SHA;
- мониторинг различает `eligible_branded`, `ineligible_original`,
  `unsafe_placement_original`, render failure и delivery failure.

После включения sampled posts обеих сетей проверяются в реальных клиентах. При
ложном `visual_only`, закрытом лице/объекте, размытии, неправильном logo asset или
surface drift режим немедленно возвращается в `off`; уже опубликованные посты
исправляются только отдельным управляемым repair-решением.

## Связанные документы

- [Event media gate](README.md)
- [Static-site brand lockups](../static-site-pages/design-system/brand-lockups.md)
- [Telegram event publishing](../tg-publishing/README.md)
- [VK publishing](../vk-publishing/README.md)
- [Full release readiness](../../reports/static-personal-announcements-release-readiness-2026-07-11.md)
