export type MobileEventReviewVariant = {
  slug: 'control' | 'open-prose' | 'action-dock' | 'open-prose-action-dock';
  label: string;
  shortLabel: string;
  note: string;
  proseTreatment: 'card' | 'open';
  actionsTreatment: 'current' | 'dock';
};

export type MobileEventReviewScenario = {
  slug: 'photo-paid' | 'visual-free' | 'ocr-poster';
  eventId: number;
  label: string;
  note: string;
  composition: 'photo-cinematic-sheet' | 'poster-billboard';
};

export const MOBILE_EVENT_REVIEW_VARIANTS: MobileEventReviewVariant[] = [
  {
    slug: 'control',
    label: 'Control · текущая mobile',
    shortLabel: 'Control',
    note: 'Текущий rounded description container и текущая раскладка действий. Нужен как честная точка сравнения.',
    proseTreatment: 'card',
    actionsTreatment: 'current',
  },
  {
    slug: 'open-prose',
    label: 'Open prose · только текст',
    shortLabel: 'Open prose',
    note: 'Убирает внешний card у длинного текста и возвращает боковое пространство. Действия намеренно остаются текущими.',
    proseTreatment: 'open',
    actionsTreatment: 'current',
  },
  {
    slug: 'action-dock',
    label: 'Action dock · только действия',
    shortLabel: 'Action dock',
    note: 'Сохраняет текущий description card, но собирает secondary actions в один тёплый графитовый узел.',
    proseTreatment: 'card',
    actionsTreatment: 'dock',
  },
  {
    slug: 'open-prose-action-dock',
    label: 'Combined · open prose + action dock',
    shortLabel: 'Combined',
    note: 'Проверяет совместный результат двух независимых изменений без правок brand tag, media policy и discovery cards.',
    proseTreatment: 'open',
    actionsTreatment: 'dock',
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
