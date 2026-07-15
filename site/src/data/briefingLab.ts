export type BriefingLabFragment = {
  text: string;
  accent?: boolean;
  breakAfter?: boolean;
  href?: string;
};

export type BriefingLabScenario = {
  id: string;
  label: string;
  fragments: readonly BriefingLabFragment[];
  ctaLabel: string;
  ctaHref: string;
};

// Fixed, deliberately labelled DEMO fixtures. Counts are not production claims.
// The order is the single coherent play-all narrative used by the lab.
export const briefingLabScenarios = [
  {
    id: 'today_count', label: 'Сегодня',
    fragments: [
      { text: 'Сегодня —' }, { text: '18 идей.', accent: true, breakAfter: true, href: '/segodnya/' },
      { text: 'Выберите' }, { text: 'свой ритм.' },
    ],
    ctaLabel: 'Открыть сегодня', ctaHref: '/segodnya/',
  },
  {
    id: 'tomorrow_count', label: 'Завтра',
    fragments: [
      { text: 'Завтра —' }, { text: '12 идей.', accent: true, breakAfter: true, href: '/zavtra/' },
      { text: 'План' }, { text: 'уже здесь.' },
    ],
    ctaLabel: 'Посмотреть завтра', ctaHref: '/zavtra/',
  },
  {
    id: 'weekend_count', label: 'Выходные',
    fragments: [
      { text: '24 идеи', accent: true, href: '/vyhodnye/' }, { text: 'на выходные.', breakAfter: true },
      { text: 'От музея' }, { text: 'до сцены.' },
    ],
    ctaLabel: 'Собрать выходные', ctaHref: '/vyhodnye/',
  },
  {
    id: 'exhibitions_count', label: 'Выставки',
    fragments: [
      { text: 'Сейчас —' }, { text: '7 выставок.', accent: true, breakAfter: true, href: '/vystavki/' },
      { text: 'Начните' }, { text: 'с центра.' },
    ],
    ctaLabel: 'Выбрать выставку', ctaHref: '/vystavki/',
  },
  {
    id: 'free_count', label: 'Бесплатно',
    fragments: [
      { text: '6 событий', accent: true, href: '/segodnya/' }, { text: 'без билета.', breakAfter: true },
      { text: 'Выберите' }, { text: 'своё.' },
    ],
    ctaLabel: 'Найти бесплатное', ctaHref: '/segodnya/',
  },
  {
    id: 'tonight_count', label: 'Вечером',
    fragments: [
      { text: 'Вечером —' }, { text: '9 событий.', accent: true, breakAfter: true, href: '/segodnya/' },
      { text: 'Город' }, { text: 'не спит.' },
    ],
    ctaLabel: 'Выбрать план на вечер', ctaHref: '/segodnya/',
  },
  {
    id: 'newly_added_count', label: 'Новое',
    fragments: [
      { text: 'Новых —' }, { text: '11 событий.', accent: true, breakAfter: true, href: '/populyarnoe/' },
      { text: 'Смотрите' }, { text: 'новое.' },
    ],
    ctaLabel: 'Открыть новое', ctaHref: '/populyarnoe/',
  },
  {
    id: 'catalog_generic', label: 'Каталог',
    fragments: [
      { text: 'С чего' }, { text: 'начать?', accent: true, breakAfter: true, href: '/segodnya/' },
      { text: 'Выберите' }, { text: 'дату или формат.' },
    ],
    ctaLabel: 'Открыть афишу', ctaHref: '/segodnya/',
  },
] as const satisfies readonly BriefingLabScenario[];

export const briefingLabFallback = {
  id: 'neutral_fallback', label: 'Нейтральный fallback',
  fragments: [
    { text: 'Город' }, { text: 'продолжается.', accent: true, breakAfter: true },
    { text: 'Найдите' }, { text: 'свой план.' },
  ],
  ctaLabel: 'Открыть афишу', ctaHref: '/segodnya/',
} as const satisfies BriefingLabScenario;

export const briefingLabCategories = [
  { label: 'Сегодня', href: '/segodnya/' },
  { label: 'Завтра', href: '/zavtra/' },
  { label: 'Выходные', href: '/vyhodnye/' },
  { label: 'Выставки', href: '/vystavki/' },
  { label: 'Популярное', href: '/populyarnoe/' },
] as const;

export const briefingLabFixtureIds = [6607, 5373, 6020] as const;
export const briefingLabExcludedFixtureId = 6045;
export const defaultBriefingLabScenario = briefingLabScenarios[0];
