export type FestivalTimelineStatus = 'announced' | 'program-pending' | 'date-pending';
export type FestivalTimelineMediaMode = 'visual' | 'document' | 'unknown';

export interface FestivalTimelineItem {
  slug: string;
  title: string;
  dateLabel: string;
  monthKey: 'july' | 'august' | 'september' | 'october' | 'november' | 'december';
  place: string;
  category: string;
  description: string;
  status: FestivalTimelineStatus;
  statusLabel: string;
  sourceHref: string;
  sourceLabel: string;
  internalEventId?: number;
  image: string;
  imageWidth: number;
  imageHeight: number;
  mediaMode: FestivalTimelineMediaMode;
  objectPosition?: string;
}

export const festivalTimelineMonths = [
  { key: 'july', label: 'Июль', shortLabel: 'Июл', mood: 'Джаз открывает большой фестивальный сезон.' },
  { key: 'august', label: 'Август', shortLabel: 'Авг', mood: 'Главный месяц: море, кино, музыка и история.' },
  { key: 'september', label: 'Сентябрь', shortLabel: 'Сен', mood: 'Городские пикники, сцены и встречи у воды.' },
  { key: 'october', label: 'Октябрь', shortLabel: 'Окт', mood: 'Театр, книги и музыка переходят в залы.' },
  { key: 'november', label: 'Ноябрь', shortLabel: 'Ноя', mood: 'Культурные традиции и осенний джаз.' },
  { key: 'december', label: 'Декабрь', shortLabel: 'Дек', mood: 'Камерный финал года в Кафедральном соборе.' },
] as const;

/**
 * Festival rows are generated from core SQLite into
 * `festival-timeline.json`.  This module intentionally contains only the
 * presentation types/month labels; it must never become a page-data fallback.
 */
