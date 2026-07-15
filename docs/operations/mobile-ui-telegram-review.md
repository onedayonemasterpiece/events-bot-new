# Mobile UI review через Telegram

## Назначение

Для любой задачи с мобильным web/UI видом Telegram forum является обязательным каналом входных комментариев и визуальной приёмки. Это относится к responsive-вёрстке, mobile-only интерфейсам, мобильным viewport-тестам, жестам, motion, скриншотам и публикации mobile preview.

Forum: `KenigEvents · UI review`  
Entry link: <https://t.me/c/4337049383/1>  
Peer: `-1004337049383`

Automation skill: `.codex/skills/telegram-mobile-ui-review/SKILL.md`.

## Обязательный pre-work gate

1. Сформулировать короткое стабильное имя задачи/треда.
2. До изменений реализации получить inventory форумных topics.
3. Если тред задачи существует — прочитать все новые сообщения, открыть/скачать все изображения и вложения, включить комментарии в requirements/checklist.
4. Если треда нет — создать его и отправить task card со scope, текущим стендом/веткой и вопросами для ревью. Новый пустой тред означает, что входящих комментариев пока нет.
5. При возобновлении долгой задачи повторить чтение непосредственно перед продолжением реализации.

Недоступность Telegram или разрешённой human session — явный blocker этого gate. Она не позволяет считать мобильную задачу готовой и не должна замалчиваться.

## Обязательная публикация evidence

После появления проверяемого render/preview отправить в тот же task thread:

- скриншот минимальной поддерживаемой ширины;
- скриншот репрезентативного телефона;
- для motion/gesture — короткое видео либо последовательность фаз;
- URL и точный build/version;
- что именно оценивать и что сейчас out of scope.

После отправки нужно проверить message IDs чтением треда и ещё раз проверить новые пользовательские комментарии до закрытия задачи. Desktop-only contact sheet не заменяет мобильные артефакты.

## Session и artifacts

- Использовать локальную human session `TELEGRAM_AUTH_BUNDLE_E2E` или `TELEGRAM_SESSION` из канонического `/home/dev/projects/events-bot-new/.env`.
- Не использовать `TELEGRAM_AUTH_BUNDLE_S22`: она зарезервирована для Kaggle/remote monitoring.
- Не печатать и не коммитить session strings, API hash или bundle.
- Inspection JSON, скачанные комментарии/медиа и send receipts хранить в `artifacts/codex/<task>/` и не коммитить.

## Текущий briefing lab

Для typed-briefing mobile review создан topic `Typed briefing hero · mobile`, topic id `6`: <https://t.me/c/4337049383/6>. Комментарий `20` уточнил post-exposure action semantics. Сообщения пользователя `55–57` с аннотированным screenshot затем отвергли public meta/pause/pace, однофразовую остановку и nested desktop media frame; они были прочитаны до коррекции, scope receipt — `58`. Текущий immutable URL `preview-20260715t1853-briefing-lab-d6bf2e1e` доставлен message `66`; `320×568`, `390×844` active/terminal, desktop-wide и slow-chain WebM — messages `67–71`. Все delivery receipts verified; повторное чтение после `71` не обнаружило новых входящих комментариев.

Для future prototype конкретных date listings создан topic `Date listings · mobile`, topic id `33`: <https://t.me/c/4337049383/33>. Первичная requirements-card о deep-linkable дате, раскрытии следующей секции после подзаголовка и no-JS/SEO/history/mobile gates доставлена с message id `34`; commit-bound summary — message `36`, оба верифицированы повторным чтением. Новых входящих после message `36` не было; визуальных артефактов пока нет, потому что это этап спецификации, а не UI-прототип.
