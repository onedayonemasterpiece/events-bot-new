# Видеоанонс экскурсии

> **Status:** feature concept / design spec, не production-ready.  
> **Scope:** отдельный story-native видеоанонс одной экскурсии, собранный из уже принятой CherryFlash guide-excursion 3D-сцены и пользовательского прототипа продолжения.

## Идея

Фича делает короткий вертикальный видеоанонс конкретной экскурсии: сначала зритель видит знакомое CherryFlash-открытие для экскурсионного промо с объёмной 3D-подачей, затем ролик переходит в более прямой анонс/добивку по референсу из пользовательского видео.

Целевой продукт — не замена `CrumpleVideo` и не общий ежедневный CherryFlash-обзор, а отдельный формат для продвижения одной экскурсии гида:

1. **P0/P1 старт:** использовать начало “как в части про экскурсии”, именно с 3D/Blender depth treatment из CherryFlash guide promo.
2. **P2 продолжение:** после 3D-старта вести ролик “как в” локальном референсе `VID_20260708_142351.mp4`.
3. **P3 CTA:** завершать читаемым бронированием/контактом, не ломая screenshotable CTA-контракт guide promo.

## Источник 3D-основы

Интегрированный Blender-шаблон CherryFlash guide render зафиксирован как исходная точка:

- GitHub branch: `https://github.com/onedayonemasterpiece/events-bot-new/tree/hotfix/cherryflash-guide-blender-prod`
- Branch/SHA: `3a2ebd0d0afbdaaa54c86767cd69ddd1204b7753`
- Локальная проверка: `git ls-remote origin refs/heads/hotfix/cherryflash-guide-blender-prod` возвращает тот же SHA.
- Ближайшая каноника текущего поведения: `docs/features/cherryflash/README.md`, раздел **Guide excursion promo scene**.

Важно: Blender-шаблон здесь трактуется как источник стартовой 3D-сцены и motion-language. Он не должен заново переизобретать карточку экскурсии.

## Локальный артефакт-референс

Пользовательский MP4 перенесён из корня `artifacts/` в feature-scoped ignored folder:

```text
artifacts/features/excursion-video-announce/VID_20260708_142351.mp4
artifacts/features/excursion-video-announce/SHA256SUMS
artifacts/features/excursion-video-announce/metadata.txt
```

Метаданные при упаковке фичи (`2026-07-08`):

- размер: `1,070,231` байт;
- MP4 track: `576x1280`;
- duration: `6.607s`;
- SHA-256: `0e28a196155b212f10bf9cfe017bc706d09dd8e35f96ac12afb42ecf7e36afbf`.

Артефакт не коммитится по проектному правилу `artifacts/*`; этот документ фиксирует путь и checksum, чтобы файл не потерялся локально и чтобы его можно было заново приложить к будущей реализации.

## Визуальный контракт

### Начало ролика

- Берём avatar-led CherryFlash guide promo как golden start: top label/icon, герой-аватар, название, дата, CTA/contact.
- Сохраняем утверждённую палитру CherryFlash: teal/blue background, cream text, orange accents.
- 3D участвует как глубина, matte-плоскости, свет/halo и parallax; продуктовая UI-карточка остаётся читаемой.
- Не допускаются: static first frame, brown/copper drift, trapezoid text, fog/noise, тяжёлые 3D-блоки поверх текста или CTA.

### Продолжение после 3D-старта

- Пользовательский `VID_20260708_142351.mp4` считается reference artifact для второй части ролика.
- Пока не утверждён shot-by-shot breakdown, нельзя заменять референс абстрактной “похожей” сценой; следующая реализация должна либо разобрать видео на ключевые фазы, либо приложить storyboard рядом с 3D-стартом.
- Формат должен оставаться story-first: безопасная вертикальная композиция, короткая длительность, читаемый CTA.

### CTA/contact

- Telegram booking показывать как `@username`.
- Телефон показывать как номер телефона.
- VK-only booking показывать компактно (`vk.com/<slug>` или `VK`), а не человеко-описанием без действия.
- CTA должен полностью попадать в safe area и быть читаемым на скриншоте.

## Runtime relationship

Потенциальные reuse-точки из существующей кодовой базы:

- `video_announce/cherryflash_excursions.py` — выбор и подготовка guide-excursion promo occurrence.
- `video_announce/scenario.py` — встраивание guide promo в CherryFlash flow.
- `video_announce/assets/cherryflash_icons/` — утверждённые SVG Repo-derived icon masks.
- `kaggle/CherryFlash/` — ближайший Kaggle runtime для CherryFlash render/publish path.

Новая фича должна оставаться отдельным режимом/профилем, чтобы не дестабилизировать ежедневный `popular_review` и существующий `/v` CrumpleVideo.

## Acceptance gate для будущей реализации

Перед публикацией кандидата нужно приложить:

1. Storyboard с первым 3D-кадром, reading pose, переходом в reference-continuation, CTA и exit.
2. Side-by-side против утверждённой CherryFlash guide promo v7-геометрии для стартовой части.
3. Проверки motion:
   - frame 1→2 и 2→3 имеют ненулевую дельту;
   - нет статического плато в начале;
   - background depth layers продолжают двигаться в P2/P3;
   - CTA полностью внутри safe area.
4. Проверки качества:
   - текст/дата/CTA читаются;
   - нет перспективной деформации названия и контактов;
   - нет цветового дрейфа от CherryFlash-палитры.
5. Внешний acceptance review только допустимого класса: Gemini Pro (`gemini-3-pro-preview` / `gemini-3.1-pro-preview`) или Opus, если задача доходит до production-кандидата.

## Open questions

- Нужен ли отдельный операторский запуск (`/v` → “Видеоанонс экскурсии”) или формат сначала живёт как ручной render/publish profile.
- Будет ли продолжение полностью воспроизводить пользовательский MP4 или использовать его как motion/reference moodboard.
- Нужны ли отдельные размеры: текущий reference `576x1280`, CherryFlash guide promo `720x1280`; production target нужно зафиксировать перед рендером.
- Какой источник аудио/саунд-дизайна использовать для отдельного экскурсионного анонса.
