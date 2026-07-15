export type BriefingLabFragment = {
  text: string;
  accent?: boolean;
  breakAfter?: boolean;
  href?: string;
};

export type BriefingLabScenarioFamily = 'count' | 'education' | 'signal';
export type BriefingLabCursor = 'bar' | 'underscore';
export type BriefingLabAction = 'share_event' | 'like_event' | 'not_interested';

export type BriefingLabScenario = {
  id: string;
  label: string;
  family: BriefingLabScenarioFamily;
  fragments: readonly BriefingLabFragment[];
  ctaLabel: string;
  ctaHref: string;
  cooldownDays: number;
  maxExposuresPerYear?: number;
  actionKind?: BriefingLabAction;
  actionSuccessSuppressDays?: number;
  demoSignal?: boolean;
  demoValidUntil?: string;
  cursor: BriefingLabCursor;
  cursorLinger?: boolean;
};

// Fixed lab fixtures, not production claims. The deck deliberately mixes useful
// counts, action education and curiosity-led DEMO signals.
export const briefingLabScenarios = [
  {
    id: 'today_count', label: 'Сегодня', family: 'count', cooldownDays: 1, cursor: 'bar',
    fragments: [
      { text: 'Сегодня —' }, { text: '18 идей.', accent: true, breakAfter: true, href: '/segodnya/' },
      { text: 'Какая' }, { text: 'зацепит вас?' },
    ],
    ctaLabel: 'Посмотреть идеи', ctaHref: '/segodnya/',
  },
  {
    id: 'tomorrow_count', label: 'Завтра', family: 'count', cooldownDays: 1, cursor: 'underscore',
    fragments: [
      { text: 'Завтра —' }, { text: '12 поводов', accent: true, breakAfter: true, href: '/zavtra/' },
      { text: 'выйти' }, { text: 'из дома.' },
    ],
    ctaLabel: 'Заглянуть в завтра', ctaHref: '/zavtra/',
  },
  {
    id: 'weekend_count', label: 'Выходные', family: 'count', cooldownDays: 1, cursor: 'bar', cursorLinger: true,
    fragments: [
      { text: '24 идеи', accent: true, href: '/vyhodnye/' }, { text: 'на выходные.', breakAfter: true },
      { text: 'С чего' }, { text: 'начнём?' },
    ],
    ctaLabel: 'Собрать выходные', ctaHref: '/vyhodnye/',
  },
  {
    id: 'share_education', label: 'Как поделиться', family: 'education', cooldownDays: 30, maxExposuresPerYear: 3,
    actionKind: 'share_event', actionSuccessSuppressDays: 90, cursor: 'underscore', cursorLinger: true,
    fragments: [
      { text: 'Есть' }, { text: 'с кем пойти?', accent: true, breakAfter: true },
      { text: 'Нажмите' }, { text: '«Поделиться».' },
    ],
    ctaLabel: 'Показать в ленте', ctaHref: '#events',
  },
  {
    id: 'like_education', label: 'Зачем ставить лайк', family: 'education', cooldownDays: 30, maxExposuresPerYear: 3,
    actionKind: 'like_event', actionSuccessSuppressDays: 90, cursor: 'bar',
    fragments: [
      { text: 'Событие' }, { text: 'понравилось?', accent: true, breakAfter: true },
      { text: 'Отметьте' }, { text: 'сердцем.' },
    ],
    ctaLabel: 'Показать в ленте', ctaHref: '#events',
  },
  {
    id: 'not_interested_education', label: 'Не моё', family: 'education', cooldownDays: 30, maxExposuresPerYear: 3,
    actionKind: 'not_interested', actionSuccessSuppressDays: 90, cursor: 'underscore',
    fragments: [
      { text: 'Не ваше?', accent: true, breakAfter: true },
      { text: 'Нажмите' }, { text: '«Не интересно».' },
    ],
    ctaLabel: 'Показать в ленте', ctaHref: '#events',
  },
  {
    id: 'frequently_forwarded', label: 'Часто пересылают', family: 'signal', cooldownDays: 14,
    demoSignal: true, demoValidUntil: '2099-12-31T23:59:59Z', cursor: 'bar', cursorLinger: true,
    fragments: [
      { text: 'Это событие' }, { text: 'часто пересылают.', accent: true, breakAfter: true },
      { text: 'Что в нём' }, { text: 'нашли?' },
    ],
    ctaLabel: 'Узнать, что там', ctaHref: '/populyarnoe/',
  },
  {
    id: 'anticipated_person', label: 'Кого особенно ждут', family: 'signal', cooldownDays: 30,
    demoSignal: true, demoValidUntil: '2099-12-31T23:59:59Z', cursor: 'underscore', cursorLinger: true,
    fragments: [
      { text: 'В комментариях' }, { text: 'ждут гостя.', accent: true, breakAfter: true },
      { text: 'Угадаете' }, { text: 'кого?' },
    ],
    ctaLabel: 'Открыть обсуждаемое', ctaHref: '/populyarnoe/',
  },
] as const satisfies readonly BriefingLabScenario[];

export const briefingLabFallback = {
  id: 'neutral_fallback', label: 'Нейтральный fallback', family: 'count', cooldownDays: 1, cursor: 'bar',
  fragments: [
    { text: 'Город' }, { text: 'не ждёт.', accent: true, breakAfter: true },
    { text: 'Что удивит' }, { text: 'сегодня?' },
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
