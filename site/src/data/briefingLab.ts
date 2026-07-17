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
export type BriefingLabMediaMode = 'small' | 'wide' | 'mosaic';
export type BriefingLabMediaLayout = 'panorama' | 'portrait-single' | 'portrait-collage';
export type BriefingLabMediaFit = 'cover' | 'contain';

export type BriefingLabScenario = {
  id: string;
  label: string;
  family: BriefingLabScenarioFamily;
  /** Review-deck scenarios never enter the ordinary narrative queue. */
  reviewOnly?: boolean;
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
  media?: {
    eventId: number;
    mode: BriefingLabMediaMode;
    assetSourceOrder?: number;
    /** Several exact sources form contiguous panels; they are never shuffled per tile. */
    assetSourceOrders?: readonly number[];
    layout?: BriefingLabMediaLayout;
    fit?: BriefingLabMediaFit;
    /** Scene-specific aspect safeguard; used only when a proven face interval needs more height. */
    mosaicColumns?: number;
    /** Portrait panels are rendered much narrower than the panoramic shell. */
    minSourceWidth?: number;
    focusX?: number;
    focusY?: number;
    /** Explicit lab curation: raster was checked to contain no competing text. */
    ocrSafe?: boolean;
    /** Runtime fail-closed ceiling for cover upscaling. */
    maxUpscale?: number;
  };
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
    media: { eventId: 6607, mode: 'mosaic', assetSourceOrder: 0, focusX: 50, focusY: 48 },
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
    media: { eventId: 5373, mode: 'mosaic', assetSourceOrder: 2, focusX: 54, focusY: 45 },
  },
  {
    id: 'greeting_day', label: 'Добрый день', family: 'welcome', cooldownDays: 1, cursor: 'bar',
    fragments: [
      { text: 'Добрый день!', accent: true, breakAfter: true },
      { text: 'Что сегодня' }, { text: 'вас удивит?' },
    ],
    ctaLabel: 'Посмотреть идеи', ctaHref: '/segodnya/', nextScenarioId: 'today_count',
    media: { eventId: 6466, mode: 'mosaic', assetSourceOrder: 0, focusX: 58, focusY: 50, ocrSafe: true },
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
    media: { eventId: 6607, mode: 'mosaic', assetSourceOrder: 0, focusX: 50, focusY: 48 },
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
    demoSignal: true, demoValidUntil: '2026-07-30T23:59:59+02:00', cursor: 'underscore',
    fragments: [
      { text: 'Часто пересылают', accent: true, eventId: 6466 },
      { text: '«Планету Океан».', accent: true, breakAfter: true, eventId: 6466 },
      { text: 'Заглянем?' },
    ],
    ctaLabel: 'Открыть «Планету Океан»', ctaEventId: 6466, nextScenarioId: 'anticipated_person',
    media: { eventId: 6466, mode: 'mosaic', assetSourceOrder: 0, focusX: 58, focusY: 50, ocrSafe: true },
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
    media: { eventId: 6020, mode: 'mosaic', assetSourceOrder: 0, focusX: 50, focusY: 58 }, nextScenarioId: 'live_meeting_mosaic',
  },
  {
    id: 'live_meeting_mosaic', label: 'Живая встреча · мозаика', family: 'signal', cooldownDays: 30,
    cursor: 'underscore',
    fragments: [
      { text: 'Живая встреча', breakAfter: true },
      { text: 'Алексей Мышкин.', accent: true, eventId: 6112, breakAfter: true },
      { text: '13 августа.' },
    ],
    ctaLabel: 'Открыть встречу', ctaEventId: 6112,
    media: { eventId: 6112, mode: 'mosaic', assetSourceOrder: 0, focusX: 61, focusY: 48 }, nextScenarioId: 'rare_event',
  },
  {
    id: 'rare_event', label: 'Редкое событие', family: 'signal', cooldownDays: 30, cursor: 'underscore',
    fragments: [
      { text: 'Редкий формат:', breakAfter: true },
      { text: 'Вертинский.', accent: true, eventId: 5373 }, { text: 'Идём?' },
    ],
    ctaLabel: 'Открыть кабаре', ctaEventId: 5373, nextScenarioId: 'festival_demo',
    media: { eventId: 5373, mode: 'mosaic', assetSourceOrder: 2, focusX: 54, focusY: 45 },
  },
  {
    id: 'weather_water_demo', label: 'Погода → море', family: 'signal', cooldownDays: 7,
    demoSignal: true, demoValidUntil: '2026-08-31T23:59:59+02:00', cursor: 'underscore', nextScenarioId: 'unusual_format_demo',
    fragments: [
      { text: 'Обещают' }, { text: 'ясные выходные.', accent: true, breakAfter: true },
      { text: 'Махнём' }, { text: 'на море?', accent: true, href: '/poisk/' },
    ],
    ctaLabel: 'Найти событие у моря', ctaHref: '/poisk/',
    media: { eventId: 6466, mode: 'mosaic', assetSourceOrder: 0, focusX: 58, focusY: 50, ocrSafe: true },
  },
  {
    id: 'storm_weekend_demo', label: 'Если шторм → в уют', family: 'signal', cooldownDays: 7,
    demoSignal: true, demoValidUntil: '2026-07-24T23:59:59+02:00', cursor: 'underscore', nextScenarioId: 'storm_lecture_science_demo',
    fragments: [
      { text: 'Если прогнозируют' }, { text: 'шторм —', accent: true, breakAfter: true },
      { text: 'может, в уют?' },
    ],
    ctaLabel: 'Показать лекцию', ctaEventId: 5803,
    media: { eventId: 5803, mode: 'mosaic', assetSourceOrder: 1, focusX: 50, focusY: 43 },
  },
  {
    id: 'storm_lecture_science_demo', label: 'Шторм → лекция', family: 'signal', cooldownDays: 7,
    demoSignal: true, demoValidUntil: '2026-07-24T23:59:59+02:00', cursor: 'bar',
    fragments: [
      { text: 'Шоу-лекция:', breakAfter: true },
      { text: 'Суперспособности.', accent: true, eventId: 5803 },
    ],
    ctaLabel: 'Открыть лекцию', ctaEventId: 5803,
    media: { eventId: 5803, mode: 'mosaic', assetSourceOrder: 1, focusX: 50, focusY: 43 },
  },
  {
    id: 'festival_demo', label: 'Pianissimo', family: 'signal', cooldownDays: 7,
    demoSignal: true, demoValidUntil: '2026-07-16T23:59:59+02:00', cursor: 'underscore',
    fragments: [
      { text: 'Pianissimo.', accent: true, eventId: 5294, breakAfter: true },
      { text: 'Максим Милославский.', accent: true, eventId: 5294 },
    ],
    ctaLabel: 'Посмотреть концерт', ctaEventId: 5294, nextScenarioId: 'unusual_format_demo',
    media: { eventId: 5294, mode: 'mosaic', assetSourceOrder: 0, focusX: 50, focusY: 40 },
  },
  {
    id: 'unusual_format_demo', label: 'Необычный формат', family: 'signal', cooldownDays: 14,
    demoSignal: true, demoValidUntil: '2099-12-31T23:59:59Z', cursor: 'bar',
    fragments: [
      { text: 'Иногда план —', breakAfter: true },
      { text: 'паблик-ток.', accent: true }, { text: 'Послушаем город?' },
    ],
    ctaLabel: 'Найти необычное', ctaHref: '/poisk/', nextScenarioId: 'weekend_count',
    media: { eventId: 6112, mode: 'mosaic', assetSourceOrder: 0, focusX: 61, focusY: 48 },
  },
] as const satisfies readonly BriefingLabScenario[];

// A deliberately manual visual-review deck. These are grounded event objects,
// but they do not enter the ordinary narrative queue, cooldown memory or
// three-step automatic chain. Every asset was inspected at source resolution
// for the absence of competing raster copy and has a curated focal cover.
export const briefingLabMediaReviewScenarios = [
  {
    id: 'media_review_planet_ocean', label: 'Фото 01 · Планета Океан', family: 'signal', reviewOnly: true,
    cooldownDays: 0, cursor: 'underscore',
    fragments: [{ text: 'Планета Океан.', accent: true, eventId: 6466, breakAfter: true }, { text: 'Заглянем?' }],
    ctaLabel: 'Открыть событие', ctaEventId: 6466,
    media: { eventId: 6466, mode: 'mosaic', assetSourceOrder: 0, focusX: 58, focusY: 50, ocrSafe: true },
  },
  {
    id: 'media_review_ivana_kupala', label: 'Фото 02 · Ивана Купала', family: 'signal', reviewOnly: true,
    cooldownDays: 0, cursor: 'underscore',
    fragments: [{ text: 'Ивана Купала.', accent: true, eventId: 6525, breakAfter: true }, { text: 'Поедем?' }],
    ctaLabel: 'Открыть праздник', ctaEventId: 6525,
    media: { eventId: 6525, mode: 'mosaic', assetSourceOrder: 0, focusX: 52, focusY: 50, ocrSafe: true },
  },
  {
    id: 'media_review_region_80', label: 'Фото 03 · 80 лет области', family: 'signal', reviewOnly: true,
    cooldownDays: 0, cursor: 'underscore',
    fragments: [{ text: 'Области — 80.', accent: true, eventId: 6559, breakAfter: true }, { text: 'Празднуем?' }],
    ctaLabel: 'Открыть концерт', ctaEventId: 6559,
    media: { eventId: 6559, mode: 'mosaic', assetSourceOrder: 0, focusX: 56, focusY: 48, ocrSafe: true },
  },
  {
    id: 'media_review_writing_kaliningrad', label: 'Фото 04 · Пишу из Калининграда', family: 'signal', reviewOnly: true,
    cooldownDays: 0, cursor: 'underscore',
    fragments: [{ text: '«Пишу сегодня', breakAfter: true }, { text: 'из Калининграда…»', accent: true, eventId: 6611 }],
    ctaLabel: 'Открыть программу', ctaEventId: 6611,
    media: { eventId: 6611, mode: 'mosaic', assetSourceOrder: 0, mosaicColumns: 16, focusX: 56, focusY: 6, ocrSafe: true },
  },
  {
    id: 'media_review_swan_lake', label: 'Фото 05 · Лебединое озеро', family: 'signal', reviewOnly: true,
    cooldownDays: 0, cursor: 'underscore',
    fragments: [{ text: '«Лебединое озеро».', accent: true, eventId: 6565, breakAfter: true }, { text: 'Посмотрим?' }],
    ctaLabel: 'Открыть спектакль', ctaEventId: 6565,
    media: { eventId: 6565, mode: 'mosaic', assetSourceOrder: 1, focusX: 55, focusY: 50, ocrSafe: true },
  },
  {
    id: 'media_review_vertinsky', label: 'Фото 06 · Вертинский', family: 'signal', reviewOnly: true,
    cooldownDays: 0, cursor: 'underscore',
    fragments: [{ text: 'Вертинский.', accent: true, eventId: 4757, breakAfter: true }, { text: 'Встретимся в театре?' }],
    ctaLabel: 'Открыть спектакль', ctaEventId: 4757,
    media: { eventId: 4757, mode: 'mosaic', assetSourceOrder: 0, focusX: 38, focusY: 20, ocrSafe: true },
  },
  {
    id: 'media_review_literary_evening', label: 'Фото 07 · Литературный вечер', family: 'signal', reviewOnly: true,
    cooldownDays: 0, cursor: 'underscore',
    fragments: [{ text: 'Литературный вечер.', accent: true, eventId: 6153, breakAfter: true }, { text: 'Послушаем?' }],
    ctaLabel: 'Открыть программу', ctaEventId: 6153,
    media: { eventId: 6153, mode: 'mosaic', assetSourceOrder: 0, focusX: 52, focusY: 45, ocrSafe: true },
  },
  {
    id: 'media_review_hay_day', label: 'Фото 08 · День валяния в сене', family: 'signal', reviewOnly: true,
    cooldownDays: 0, cursor: 'underscore',
    fragments: [{ text: 'День валяния', breakAfter: true }, { text: 'в сене.', accent: true, eventId: 6365 }, { text: 'Серьёзно.' }],
    ctaLabel: 'Открыть необычное', ctaEventId: 6365,
    media: { eventId: 6365, mode: 'mosaic', assetSourceOrder: 0, focusX: 55, focusY: 50, ocrSafe: true },
  },
  {
    id: 'media_review_ship_quay', label: 'Фото 09 · Набережная кораблей', family: 'signal', reviewOnly: true,
    cooldownDays: 0, cursor: 'underscore',
    fragments: [{ text: 'Набережная кораблей.', accent: true, eventId: 6155, breakAfter: true }, { text: 'Пойдём к воде?' }],
    ctaLabel: 'Открыть событие', ctaEventId: 6155,
    media: { eventId: 6155, mode: 'mosaic', assetSourceOrder: 0, focusX: 59, focusY: 50, ocrSafe: true },
  },
  {
    id: 'media_review_admiral', label: 'Фото 10 · Адмирал маринистики', family: 'signal', reviewOnly: true,
    cooldownDays: 0, cursor: 'underscore',
    fragments: [{ text: 'Адмирал маринистики.', accent: true, eventId: 5725, breakAfter: true }, { text: 'Посмотрим?' }],
    ctaLabel: 'Открыть выставку', ctaEventId: 5725,
    media: { eventId: 5725, mode: 'mosaic', assetSourceOrder: 0, focusX: 50, focusY: 48, ocrSafe: true },
  },
  {
    id: 'media_review_flight', label: 'Фото 11 · Полёт над городом', family: 'signal', reviewOnly: true,
    cooldownDays: 0, cursor: 'underscore',
    fragments: [{ text: 'Полёт', breakAfter: true }, { text: 'над Калининградом.', accent: true, eventId: 6587 }],
    ctaLabel: 'Открыть показ', ctaEventId: 6587,
    media: { eventId: 6587, mode: 'mosaic', assetSourceOrder: 0, focusX: 58, focusY: 50, ocrSafe: true },
  },
  {
    id: 'media_review_craft', label: 'Фото 12 · Линии глины', family: 'signal', reviewOnly: true,
    cooldownDays: 0, cursor: 'underscore',
    fragments: [{ text: 'Линии глины.', accent: true, eventId: 6494, breakAfter: true }, { text: 'Попробуем?' }],
    ctaLabel: 'Открыть мастер-класс', ctaEventId: 6494,
    media: { eventId: 6494, mode: 'mosaic', assetSourceOrder: 5, focusX: 55, focusY: 50, ocrSafe: true },
  },
  {
    id: 'media_review_single_portrait', label: 'Фото 13 · Один вертикальный источник', family: 'signal', reviewOnly: true,
    cooldownDays: 0, cursor: 'underscore',
    fragments: [{ text: 'Древние воины.', accent: true, eventId: 698, breakAfter: true }, { text: 'Рассмотрим героя?' }],
    ctaLabel: 'Открыть выставку', ctaEventId: 698,
    media: { eventId: 698, mode: 'mosaic', assetSourceOrder: 0, layout: 'portrait-single', fit: 'contain', focusX: 50, focusY: 50, ocrSafe: true },
  },
  {
    id: 'media_review_portrait_collage', label: 'Фото 14 · Три вертикальных источника', family: 'signal', reviewOnly: true,
    cooldownDays: 0, cursor: 'underscore',
    fragments: [{ text: 'Живопись, которую', breakAfter: true }, { text: 'мы не потеряли.', accent: true, eventId: 5894 }],
    ctaLabel: 'Открыть выставку', ctaEventId: 5894,
    media: {
      eventId: 5894, mode: 'mosaic', assetSourceOrders: [6, 10, 11], layout: 'portrait-collage', fit: 'cover',
      focusX: 50, focusY: 50, minSourceWidth: 900, ocrSafe: true,
    },
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
