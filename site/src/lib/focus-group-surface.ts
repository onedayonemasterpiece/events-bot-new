export type FocusGroupPageFamily =
  | 'home'
  | 'today'
  | 'tomorrow'
  | 'calendar_date'
  | 'weekend'
  | 'popular'
  | 'search'
  | 'collections'
  | 'festivals'
  | 'clubs'
  | 'club_detail'
  | 'event_detail'
  | 'exhibitions'
  | 'unusual'
  | 'favorites'
  | 'for_me';

function normalizePath(value: string): string {
  const path = String(value || '/').split(/[?#]/u, 1)[0] || '/';
  const reviewStripped = path.replace(
    /^\/_review\/[A-Za-z0-9_-]+(?=\/|$)/u,
    '',
  );
  const previewStripped = reviewStripped.replace(
    /^\/preview-[A-Za-z0-9_-]+(?=\/|$)/u,
    '',
  );
  const normalized = `/${previewStripped.replace(/^\/+|\/+$/gu, '')}`;
  return normalized === '/' ? '/' : `${normalized}/`;
}

export function focusGroupPageFamily(pathname: string): FocusGroupPageFamily | null {
  const path = normalizePath(pathname);
  if (/^\/(?:fokus-gruppa|zakrytaya-afisha|lab|partners)(?:\/|$)/u.test(path)) return null;
  if (path === '/') return 'home';
  if (path === '/segodnya/') return 'today';
  if (path === '/zavtra/') return 'tomorrow';
  if (/^\/date-\d{4}-\d{2}-\d{2}\/$/u.test(path)) return 'calendar_date';
  if (/^\/vyhodnye(?:\/|$)/u.test(path)) return 'weekend';
  if (path === '/populyarnoe/') return 'popular';
  if (path === '/poisk/') return 'search';
  if (/^\/podborki(?:\/|$)/u.test(path)) return 'collections';
  if (path === '/festivali/') return 'festivals';
  if (path === '/kluby-po-interesam/') return 'clubs';
  if (/^\/kluby-po-interesam\/.+\/$/u.test(path)) return 'club_detail';
  if (/^\/sobytiya\/.+\/$/u.test(path)) return 'event_detail';
  if (path === '/vystavki/') return 'exhibitions';
  if (path === '/neobychnoe/') return 'unusual';
  if (path === '/izbrannoe/') return 'favorites';
  if (path === '/dlya-menya/') return 'for_me';
  return null;
}

export function focusGroupPageLabel(family: FocusGroupPageFamily): string {
  const labels: Record<FocusGroupPageFamily, string> = {
    home: 'главную',
    today: 'страницу «Сегодня»',
    tomorrow: 'страницу «Завтра»',
    calendar_date: 'страницу выбранного дня',
    weekend: 'страницу выходных',
    popular: 'страницу «Популярное»',
    search: 'поиск',
    collections: 'подборку',
    festivals: 'страницу фестивалей',
    clubs: 'страницу клубов',
    club_detail: 'страницу клуба',
    event_detail: 'страницу события',
    exhibitions: 'страницу выставок',
    unusual: 'страницу «Необычное»',
    favorites: 'избранное',
    for_me: 'страницу «Для меня»',
  };
  return labels[family];
}
