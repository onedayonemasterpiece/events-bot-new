Ресёрч

Главная цель: создать узнаваемый векторный графический образ здания или объекта в двуцветном формате (линии и фон/home/dev/projects/events-bot-new/docs/features/countur_svg_generator/requirements/requirements.md)

Цель: из фотографии создавать контурное векторное изображение, которое с ровными линиями, скруглениями и окружностями, которое как будто бы по контурам здания и объекта нарисовал графический дизайнер или специалист по чертежам

Примеры прикладываю

И прикладываю предварительную проработку /home/dev/projects/events-bot-new/docs/features/countur_svg_generator/requirements/contour_svg_generator_spec_v0_2.md

Среда работы: kaggle

Канонический контракт текущей версии:

- актуальная техническая версия: `contour_svg_generator_engineering_spec_v0_3.md`;
- `contour_svg_generator_spec_v0_2.md` оставлен только как исторический pointer;
- итоговые артефакты: `final.svg`, `preview.png`, `final.meta.json`,
  `top_alternatives/`, `candidates/`, `leaderboard.csv`,
  `ranking_report.json`, `debug/*`;
- финальный путь: `multi-state masks -> candidate line graph -> semantic
  pruning -> primitive rendering -> final.svg`;
- raster/ControlNet/vector-trace результаты разрешены только как proposal/debug
  sources и не могут напрямую стать `final.svg`;
- Gemini вызывается только через общий `google_ai.client.GoogleAIClient` /
  `contour_svg.llm_gateway` с контролем лимитов; прямое создание provider SDK
  client в фиче запрещено;
- если обязательная neural/Gemini/Kaggle стадия недоступна, запуск должен
  завершиться явной ошибкой до финального экспорта.


# Источники технические
Библиотека архиетктурных элементов /home/dev/projects/events-bot-new/docs/features/countur_svg_generator/requirements/architecture_elements_library_v0_1.md

Рекомендуемый набор инструментов /home/dev/projects/events-bot-new/docs/features/countur_svg_generator/requirements/models_tools_catalog_v0_1.md

Обновлённые расширенные требования версии 0.3 /home/dev/projects/events-bot-new/docs/features/countur_svg_generator/requirements/contour_svg_generator_engineering_spec_v0_3.md

Потенциальный бэклог (требуется проанализировать) /home/dev/projects/events-bot-new/docs/features/countur_svg_generator/requirements/implementation_backlog_v0_1.md

# Идеи применения
## Идея фото
### Половинка здания контурными линиями
Фотография которая наполовину заменена контурными линиями
Рядом может быть текст и может отсутствать

### Контурные линии поверх фотографии
Фотография может быть fade или ещё какой-то способ её обработки с потерей детализации а поверх наносятся контурные линии, образуя дизайнерский элемент

## Идея видеоролика на основе веторизация - восстановленное фото
Генерировать видеоролик сценарного типа
1. сначала рисуются отдельные линии постепенно создавая полное изображение в векторе
2. потом оно трансформируется в реальное (исходное фото) возможно с какими-то фильтрами
3. Итог фотореалистичность

Это видео можно будет использовать в просветительских целях рассказывая об объектах искусства и объектах культурного наследия сопровождая субтитрами и так далее
