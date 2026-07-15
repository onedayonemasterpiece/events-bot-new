# Mobile event UI lab — 2026-07-15

Статус: **preview-эксперимент, не production contract**.

## Решения после повторного аудита

Предыдущая формулировка про «перекрывающий контент lockup» снята: фиксированная
терракотовая бирка — намеренный брендовый приём. Также не считаются дефектами и
не меняются в этом эксперименте:

- инверсия светлых desktop и графитовых mobile discovery cards;
- различная media policy для OCR-постеров и фотографий;
- терракотовая декоративная линия у decision block.

Проверяются только два подтверждённых расхождения:

1. большой rounded container вокруг editorial prose уменьшает полезную ширину;
2. calendar/share/like выглядят как разрозненные secondary actions.

## Матрица 2×2

| Вариант | Description | Actions | Что изолирует |
|---|---|---|---|
| `control` | текущая card | текущие | точка сравнения |
| `open-prose` | открытый молочный canvas | текущие | только экономию ширины текста |
| `action-dock` | текущая card | графитовый dock | только группировку действий |
| `open-prose-action-dock` | открытый canvas | графитовый dock | совместный результат |

Open prose убирает только фон, border, radius, shadow и лишний inner padding у
описания. Source gate остаётся отдельным компактным объектом. Action dock
сохраняет текстовые labels для calendar/share и touch targets не менее `44px`;
icon-only вариант не принимается из-за неоднозначности и accessibility.

## Реальные stress-cases

- event `5658`: фотография, ticket CTA, calendar/share/like, длинный prose;
- event `5761`: visual/free-like и другой набор действий;
- event `5878`: OCR poster в `contain`, без унификации с photo hero.

Индекс lab публикует все 12 комбинаций в `390×844` iframe и даёт отдельную
ссылку каждой комбинации: `/lab/event-mobile/`.

Preview build: `preview-20260715t-mobile-ui-variants-v1`.

## Visual QA

- проверены все `4 × 3` cases на ширинах `360`, `390`, `430` и `768px`;
- у action dock все targets имеют высоту `48–52px`; на `360px` три secondary
  actions остаются в одной строке, а при двух actions share занимает остаток;
- open prose даёт тексту на `390px` дополнительные `26px` полезной ширины;
- фото и OCR poster продолжают использовать исходные разные композиции;
- у текущего mobile hero сохранён существующий `12px` full-bleed overhang — это
  общий control invariant, а не эффект какого-либо варианта.

## External consultation

Консультация выполнена через `agy` моделью `Gemini 3.1 Pro (High)`. Gemini
предложил control, seamless prose и grouped actions. Для более честного решения
предложение преобразовано в факторную матрицу 2×2: так можно отдельно принять
open prose, отдельно action dock или только их комбинацию. Raw response хранится
в локальном, некоммитимом artifact
`artifacts/codex/static-mobile-ui-variants-20260715/gemini-3.1-pro-high-variant-review.raw.md`.

## Acceptance gate

До переноса в основную event page требуется выбор владельца продукта по двум
независимым вопросам: `open prose — да/нет`, `action dock — да/нет`. Discovery,
brand tag, hero composition/media semantics и sticky CTA этим решением не
переутверждаются.
