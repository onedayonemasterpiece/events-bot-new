export type MobileEventReviewVariant = {
  slug: 'control' | 'open-prose' | 'action-dock' | 'open-prose-action-dock' | 'accepted-v2' | 'accepted-v3' | 'accepted-v4' | 'accepted-v5';
  label: string;
  shortLabel: string;
  note: string;
  proseTreatment: 'card' | 'open';
  actionsTreatment: 'current' | 'dock';
  revision: 'v1' | 'v2' | 'v3' | 'v4';
};

export type MobileEventReviewScenario = {
  slug: 'photo-paid' | 'visual-free' | 'ocr-poster';
  eventId: number;
  label: string;
  note: string;
  composition: 'photo-cinematic-sheet' | 'poster-billboard';
  v2Composition?: 'photo-cinematic-sheet' | 'poster-billboard';
  v2ModeOverride?: 'photo-cover' | 'poster-stage';
};

export const MOBILE_EVENT_REVIEW_VARIANTS: MobileEventReviewVariant[] = [
  {
    slug: 'control',
    label: 'Control · текущая mobile',
    shortLabel: 'Control',
    note: 'Текущий rounded description container и текущая раскладка действий. Нужен как честная точка сравнения.',
    proseTreatment: 'card',
    actionsTreatment: 'current',
    revision: 'v1',
  },
  {
    slug: 'open-prose',
    label: 'Open prose · только текст',
    shortLabel: 'Open prose',
    note: 'Убирает внешний card у длинного текста и возвращает боковое пространство. Действия намеренно остаются текущими.',
    proseTreatment: 'open',
    actionsTreatment: 'current',
    revision: 'v1',
  },
  {
    slug: 'action-dock',
    label: 'Action dock · только действия',
    shortLabel: 'Action dock',
    note: 'Сохраняет текущий description card, но собирает secondary actions в один тёплый графитовый узел.',
    proseTreatment: 'card',
    actionsTreatment: 'dock',
    revision: 'v1',
  },
  {
    slug: 'open-prose-action-dock',
    label: 'Combined · open prose + action dock',
    shortLabel: 'Combined',
    note: 'Проверяет совместный результат двух независимых изменений без правок brand tag, media policy и discovery cards.',
    proseTreatment: 'open',
    actionsTreatment: 'dock',
    revision: 'v1',
  },
  {
    slug: 'accepted-v2',
    label: 'Accepted v2 · исправленный candidate',
    shortLabel: 'Accepted v2',
    note: 'Open prose и graphite dock плюс исправленный full-bleed hero, усиленные дата/время, OCR-safe framing и детерминированная compact-label policy.',
    proseTreatment: 'open',
    actionsTreatment: 'dock',
    revision: 'v2',
  },
  {
    slug: 'accepted-v3',
    label: 'Accepted v3 · feedback candidate',
    shortLabel: 'Accepted v3',
    note: 'V2 без регрессий плюс явный selected-like state, крупнее secondary icons, container-aware labels, weekday-first дата/время и более высокий OCR decision overlap.',
    proseTreatment: 'open',
    actionsTreatment: 'dock',
    revision: 'v3',
  },
  {
    slug: 'accepted-v4',
    label: 'Accepted v4 · owner correction',
    shortLabel: 'Accepted v4',
    note: 'Сохраняет принятую дату/время, возвращает OCR-параллакс без zoom и layout gap, упрощает active like и вводит чистый вертикальный ритм между информационными поверхностями.',
    proseTreatment: 'open',
    actionsTreatment: 'dock',
    revision: 'v4',
  },
  {
    slug: 'accepted-v5',
    label: 'Accepted v5 · gradient continuation',
    shortLabel: 'Accepted v5',
    note: 'Инвертирует OCR-parallax в ожидаемую сторону и соединяет rounded decision с medallions + open prose единой безрамочной gradient-continuation surface.',
    proseTreatment: 'open',
    actionsTreatment: 'dock',
    revision: 'v4',
  },
];

export const MOBILE_EVENT_REVIEW_SCENARIOS: MobileEventReviewScenario[] = [
  {
    slug: 'photo-paid',
    eventId: 5658,
    label: 'Фото + билет + 3 secondary actions',
    note: '«Гараж» — stress-case для calendar/share/like и длинного editorial prose.',
    composition: 'photo-cinematic-sheet',
  },
  {
    slug: 'visual-free',
    eventId: 5761,
    label: 'Visual/free-like + меньше действий',
    note: '«Выставка фэнтези-картин» — проверка multi-day/free-like decision flow и длинной программы.',
    composition: 'photo-cinematic-sheet',
    v2Composition: 'poster-billboard',
    v2ModeOverride: 'poster-stage',
  },
  {
    slug: 'ocr-poster',
    eventId: 5878,
    label: 'OCR poster',
    note: '«Песни СССР» — проверка contain-постера и перехода к тексту без изменения OCR media policy.',
    composition: 'poster-billboard',
  },
];

export const MOBILE_EVENT_REVIEW_CASES = MOBILE_EVENT_REVIEW_VARIANTS.flatMap((variant) =>
  MOBILE_EVENT_REVIEW_SCENARIOS.map((scenario) => ({ variant, scenario })),
);

export const mobileEventReviewPath = (variant: MobileEventReviewVariant['slug'], scenario: MobileEventReviewScenario['slug']) =>
  `/lab/event-mobile/examples/${variant}/${scenario}/`;
