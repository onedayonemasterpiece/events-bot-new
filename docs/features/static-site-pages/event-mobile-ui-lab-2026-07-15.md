# Mobile event UI lab — 2026-07-15

Статус: **preview-эксперимент, не production contract**.

Решение владельца после проверки на реальном Android: **open prose принят**,
graphite action dock принят как направление. Следующий `accepted-v2` candidate
исправляет три обнаруженных на телефонных скриншотах дефекта, не переписывая
исходную матрицу 2×2.

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
| `accepted-v2` | открытый canvas | адаптивный графитовый dock | принятые решения + mobile corrections |

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

Preview builds:

- original matrix: `preview-20260715t-mobile-ui-variants-v1`;
- accepted candidate: `preview-20260715t-mobile-ui-accepted-v2`.

## Accepted v2 corrections

Телефонные screenshots подтвердили:

1. hero был шириной `100vw`, но начинался от левого padding родительского
   `page-shell`, поэтому выходил вправо на `12px`; v2 центрирует full-bleed
   относительно viewport через симметричные `calc(50% - 50vw)` margins;
2. дата/время терялись после массивного action dock; v2 выделяет `when` и
   `where` отдельными semantic spans и усиливает `when` цветом, размером и весом;
3. квадратный текстовый poster event `5761` ошибочно имел fixture status
   `visual_only` и получал photo cover/parallax zoom; v2 явным review override
   проверяет `poster-stage + poster-billboard`, не меняя framing настоящей photo.

На `<380px` accepted v2 оставляет label ровно у одного secondary action, а
остальные сохраняет как `48px` icon controls. Выбор выполняется при статическом
рендере без hydration shift: ticket label имеет приоритет; при calendar+share
чётные event id показывают calendar, нечётные — share; если calendar отсутствует,
label получает share. На `390px+` видны обе подписи.

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

Повторный screenshot audit той же Pro-моделью подтвердил все три дефекта и
рекомендовал viewport-centred full-bleed, контрастную semi-bold date/time строку,
contain-oriented OCR policy и детерминированную server-rendered label priority.
Первый расширенный вызов истёк по времени; успешный узкий повтор сохранён в
`artifacts/codex/mobile-ui-telegram-review-20260715/gemini-3.1-pro-high-screenshot-audit-retry.raw.md`.

## Acceptance gate

До переноса в основную event page требуется принять либо отклонить три v2 детали:
усиленную date/time строку, OCR-safe override при ошибочной fixture classification
и event-parity compact-label policy. Discovery, brand tag и sticky CTA этим
решением не переутверждаются.
