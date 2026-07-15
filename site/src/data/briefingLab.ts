export type BriefingLabFragment = {
  text: string;
  accent?: boolean;
  breakAfter?: boolean;
  href?: string;
  eventId?: number;
};

export type BriefingLabScenarioFamily = 'count' | 'education' | 'signal' | 'welcome';
export type BriefingLabCursor = 'bar' | 'underscore';
export type BriefingLabAction = 'share_event' | 'like_event' | 'not_interested';
export type BriefingLabMediaMode = 'small' | 'wide';

export type BriefingLabScenario = {
  id: string;
  label: string;
  family: BriefingLabScenarioFamily;
  fragments: readonly BriefingLabFragment[];
  ctaLabel: string;
  ctaHref?: string;
  ctaEventId?: number;
  cooldownDays: number;
  maxExposuresPerYear?: number;
  actionKind?: BriefingLabAction;
  actionSuccessSuppressDays?: number;
  demoSignal?: boolean;
  demoValidUntil?: string;
  cursor: BriefingLabCursor;
  nextScenarioId?: string;
  media?: { eventId: number; mode: BriefingLabMediaMode; assetSourceOrder?: number };
};

// Fixed lab fixtures, never live production claims. Scenarios marked as a
// DEMO signal demonstrate an input contract, not an assertion about today.
export const briefingLabScenarios = [
  {
    id: 'today_count', label: 'Сегодня', family: 'count', cooldownDays: 1, cursor: 'bar',
    fragments: [
      { text: 'Сегодня —' }, { text: '18 идей.', accent: true, breakAfter: true, href: '/segodnya/' },
      { text: 'Какая' }, { text: 'зацепит вас?' },
    ],
    ctaLabel: 'Посмотреть идеи', ctaHref: '/segodnya/', nextScenarioId: 'frequently_forwarded',
  },
  {
    id: 'tomorrow_count', label: 'Завтра', family: 'count', cooldownDays: 1, cursor: 'underscore',
    fragments: [
      { text: 'Завтра —' }, { text: '12 поводов', accent: true, breakAfter: true, href: '/zavtra/' },
      { text: 'выйти' }, { text: 'из дома.' },
    ],
    ctaLabel: 'Заглянуть в завтра', ctaHref: '/zavtra/', nextScenarioId: 'anticipated_person',
  },
  {
    id: 'weekend_count', label: 'Выходные', family: 'count', cooldownDays: 1, cursor: 'bar',
    fragments: [
      { text: '24 идеи', accent: true, href: '/vyhodnye/' }, { text: 'на выходные.', breakAfter: true },
      { text: 'С чего' }, { text: 'начнём?' },
    ],
    ctaLabel: 'Собрать выходные', ctaHref: '/vyhodnye/', nextScenarioId: 'weather_water_demo',
  },
  {
    id: 'greeting_day', label: 'Добрый день', family: 'welcome', cooldownDays: 1, cursor: 'bar',
    fragments: [
      { text: 'Добрый день!', accent: true, breakAfter: true },
      { text: 'Что сегодня' }, { text: 'вас удивит?' },
    ],
    ctaLabel: 'Посмотреть идеи', ctaHref: '/segodnya/', nextScenarioId: 'today_count',
  },
  {
    id: 'local_keska', label: 'По-калининградски', family: 'welcome', cooldownDays: 60, maxExposuresPerYear: 3, cursor: 'underscore',
    fragments: [
      { text: 'Мы говорим', breakAfter: true },
      { text: 'по-калининградски.', accent: true, breakAfter: true },
      { text: 'И скажем' }, { text: '«кеска».' },
    ],
    ctaLabel: 'Найти повод выйти', ctaHref: '/segodnya/', nextScenarioId: 'festival_demo',
  },
  {
    id: 'smart_search_education', label: 'Умный поиск', family: 'education', cooldownDays: 30, maxExposuresPerYear: 3, cursor: 'bar',
    fragments: [
      { text: 'Можно просто спросить:', breakAfter: true },
      { text: '«Куда с ребёнком?»', accent: true, href: '/poisk/' },
    ],
    ctaLabel: 'Попробовать умный поиск', ctaHref: '/poisk/', nextScenarioId: 'share_education',
  },
  {
    id: 'share_education', label: 'Как поделиться', family: 'education', cooldownDays: 30, maxExposuresPerYear: 3,
    actionKind: 'share_event', actionSuccessSuppressDays: 90, cursor: 'underscore',
    fragments: [
      { text: 'Есть' }, { text: 'с кем пойти?', accent: true, breakAfter: true },
      { text: 'Нажмите' }, { text: '«Поделиться».' },
    ],
    ctaLabel: 'Показать в ленте', ctaHref: '#events', nextScenarioId: 'like_education',
  },
  {
    id: 'like_education', label: 'Зачем ставить лайк', family: 'education', cooldownDays: 30, maxExposuresPerYear: 3,
    actionKind: 'like_event', actionSuccessSuppressDays: 90, cursor: 'bar',
    fragments: [
      { text: 'Событие' }, { text: 'понравилось?', accent: true, breakAfter: true },
      { text: 'Отметьте' }, { text: 'сердцем.' },
    ],
    ctaLabel: 'Показать в ленте', ctaHref: '#events', nextScenarioId: 'not_interested_education',
  },
  {
    id: 'not_interested_education', label: 'Не моё', family: 'education', cooldownDays: 30, maxExposuresPerYear: 3,
    actionKind: 'not_interested', actionSuccessSuppressDays: 90, cursor: 'underscore',
    fragments: [
      { text: 'Не ваше?', accent: true, breakAfter: true },
      { text: 'Нажмите' }, { text: '«Не интересно».' },
    ],
    ctaLabel: 'Показать в ленте', ctaHref: '#events', nextScenarioId: 'smart_search_education',
  },
  {
    id: 'frequently_forwarded', label: 'Часто пересылают', family: 'signal', cooldownDays: 14,
    demoSignal: true, demoValidUntil: '2026-07-30T23:59:59+02:00', cursor: 'bar',
    fragments: [
      { text: 'Часто пересылают', accent: true, eventId: 6466 },
      { text: '«Планету Океан».', accent: true, breakAfter: true, eventId: 6466 },
      { text: 'Заглянем?' },
    ],
    ctaLabel: 'Открыть «Планету Океан»', ctaEventId: 6466, nextScenarioId: 'anticipated_person',
  },
  {
    id: 'anticipated_person', label: 'Кого особенно ждут', family: 'signal', cooldownDays: 30,
    demoSignal: true, demoValidUntil: '2099-12-31T23:59:59Z', cursor: 'underscore',
    fragments: [
      { text: 'В комментариях' }, { text: 'ждут гостя.', accent: true, breakAfter: true },
      { text: 'Угадаете' }, { text: 'кого?' },
    ],
    ctaLabel: 'Открыть обсуждаемое', ctaHref: '/populyarnoe/', nextScenarioId: 'anticipated_person_named',
  },
  {
    id: 'anticipated_person_named', label: 'Гость — называем имя', family: 'signal', cooldownDays: 30,
    cursor: 'bar',
    fragments: [
      { text: 'В Светлогорск едет', breakAfter: true },
      { text: 'Татьяна Куртукова.', accent: true, eventId: 6020 },
      { text: 'Пойдём?' },
    ],
    ctaLabel: 'Посмотреть концерт', ctaEventId: 6020,
    media: { eventId: 6020, mode: 'small' }, nextScenarioId: 'rare_event',
  },
  {
    id: 'rare_event', label: 'Редкое событие', family: 'signal', cooldownDays: 30, cursor: 'underscore',
    fragments: [
      { text: 'Редкий формат:', breakAfter: true },
      { text: 'Вертинский.', accent: true, eventId: 5373 }, { text: 'Идём?' },
    ],
    ctaLabel: 'Открыть кабаре', ctaEventId: 5373, nextScenarioId: 'festival_demo',
    media: { eventId: 5373, mode: 'wide', assetSourceOrder: 3 },
  },
  {
    id: 'weather_water_demo', label: 'Погода → море', family: 'signal', cooldownDays: 7,
    demoSignal: true, demoValidUntil: '2026-08-31T23:59:59+02:00', cursor: 'bar', nextScenarioId: 'unusual_format_demo',
    fragments: [
      { text: 'Обещают' }, { text: 'ясные выходные.', accent: true, breakAfter: true },
      { text: 'Махнём' }, { text: 'на море?', accent: true, href: '/poisk/' },
    ],
    ctaLabel: 'Найти событие у моря', ctaHref: '/poisk/',
  },
  {
    id: 'storm_weekend_demo', label: 'Шторм → в уют', family: 'signal', cooldownDays: 7,
    demoSignal: true, demoValidUntil: '2026-08-31T23:59:59+02:00', cursor: 'underscore', nextScenarioId: 'storm_lecture_art_demo',
    fragments: [
      { text: 'В выходные —' }, { text: 'ветер и шторм.', accent: true, breakAfter: true },
      { text: 'Пойдём на лекцию?' },
    ],
    ctaLabel: 'Показать первую лекцию', ctaEventId: 3592,
  },
  {
    id: 'storm_lecture_art_demo', label: 'Шторм → лекция 1', family: 'signal', cooldownDays: 7,
    demoSignal: true, demoValidUntil: '2026-07-19T23:59:59+02:00', cursor: 'bar', nextScenarioId: 'storm_lecture_cinema_demo',
    fragments: [
      { text: 'Можно послушать', breakAfter: true },
      { text: 'о монументальном искусстве.', accent: true, eventId: 3592 },
    ],
    ctaLabel: 'Открыть первую лекцию', ctaEventId: 3592,
  },
  {
    id: 'storm_lecture_cinema_demo', label: 'Шторм → лекция 2', family: 'signal', cooldownDays: 7,
    demoSignal: true, demoValidUntil: '2026-08-03T23:59:59+02:00', cursor: 'underscore', nextScenarioId: 'unusual_format_demo',
    fragments: [
      { text: 'Или —', breakAfter: true },
      { text: 'как снимали Калининград.', accent: true, eventId: 5077 },
    ],
    ctaLabel: 'Открыть вторую лекцию', ctaEventId: 5077,
  },
  {
    id: 'festival_demo', label: 'Pianissimo', family: 'signal', cooldownDays: 7,
    demoSignal: true, demoValidUntil: '2026-07-16T23:59:59+02:00', cursor: 'underscore',
    fragments: [
      { text: 'Pianissimo.', accent: true, eventId: 5294, breakAfter: true },
      { text: 'Максим Милославский.', accent: true, eventId: 5294 },
    ],
    ctaLabel: 'Посмотреть концерт', ctaEventId: 5294, nextScenarioId: 'unusual_format_demo',
  },
  {
    id: 'unusual_format_demo', label: 'Необычный формат', family: 'signal', cooldownDays: 14,
    demoSignal: true, demoValidUntil: '2099-12-31T23:59:59Z', cursor: 'bar',
    fragments: [
      { text: 'Иногда план —', breakAfter: true },
      { text: 'паблик-ток.', accent: true }, { text: 'Послушаем город?' },
    ],
    ctaLabel: 'Найти необычное', ctaHref: '/poisk/', nextScenarioId: 'weekend_count',
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
