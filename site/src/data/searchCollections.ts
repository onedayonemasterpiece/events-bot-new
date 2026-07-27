import { collapseOccurrenceCards } from '../lib/eventOccurrences';
import { getCurrentDate, getEvents } from '../lib/events';
import type { PreviewEvent } from '../lib/types';

export interface MaterializedSearchCollection {
  kind: 'materialized';
  slug: string;
  phrase: string;
  title: string;
  description: string;
  criteria: string;
}

export interface SearchQueryExample {
  kind: 'example';
  phrase: string;
  description: string;
}

export type SearchLearningItem = MaterializedSearchCollection | SearchQueryExample;

export const materializedSearchCollections: MaterializedSearchCollection[] = [
  {
    kind: 'materialized',
    slug: 'besplatnye-sobytiya',
    phrase: 'бесплатные события',
    title: 'Бесплатные события',
    description: 'Все актуальные события с подтверждённым бесплатным входом, включая продолжающиеся выставки.',
    criteria: 'Событие активно, ещё не закончилось, а в выгрузке афиши вход подтверждён как бесплатный.',
  },
  {
    kind: 'materialized',
    slug: 'dzhaz-na-vyhodnyh',
    phrase: 'джаз на ближайших выходных',
    title: 'Джаз на ближайших выходных',
    description: 'Концерты с джазом в названии, которые проходят в ближайшие субботу и воскресенье.',
    criteria: 'Дата события попадает в ближайшие субботу или воскресенье, а в названии явно указан джаз.',
  },
  {
    kind: 'materialized',
    slug: 'besplatno-s-detmi',
    phrase: 'куда сходить бесплатно с детьми',
    title: 'Бесплатные события с детьми',
    description: 'Будущие бесплатные события, отмеченные редакцией как семейные или детские.',
    criteria: 'Событие ещё актуально, вход отмечен бесплатным, а темы содержат FAMILY или KIDS_SCHOOL.',
  },
  {
    kind: 'materialized',
    slug: 'stendap-na-etoy-nedele',
    phrase: 'стендап на этой неделе',
    title: 'Стендап на этой неделе',
    description: 'Стендап-события на семь дней, начиная с даты обновления афиши.',
    criteria: 'Тема события — STANDUP, дата попадает в семидневное окно от даты обновления афиши.',
  },
];

export const searchQueryExamples: SearchQueryExample[] = [
  { kind: 'example', phrase: 'послушать хор', description: 'Пример короткого запроса по типу исполнения.' },
  { kind: 'example', phrase: 'концерт классической музыки вечером', description: 'Пример запроса с жанром и временем.' },
  { kind: 'example', phrase: 'куда сходить после работы', description: 'Пример запроса по жизненной ситуации.' },
  { kind: 'example', phrase: 'необычная экскурсия по городу', description: 'Пример запроса по формату и настроению.' },
  { kind: 'example', phrase: 'арт-вечеринка с музыкой', description: 'Пример запроса, объединяющего несколько интересов.' },
];

export const searchLearningItems: SearchLearningItem[] = [
  ...materializedSearchCollections,
  ...searchQueryExamples,
];

export function getMaterializedSearchCollectionReferenceDate(): string {
  const configured = String(import.meta.env.PUBLIC_SEARCH_COLLECTION_REFERENCE_DATE || '').trim();
  return /^\d{4}-\d{2}-\d{2}$/u.test(configured) ? configured : getCurrentDate();
}

function datePlusDays(date: string, days: number): string {
  const value = new Date(`${date}T00:00:00Z`);
  value.setUTCDate(value.getUTCDate() + days);
  return value.toISOString().slice(0, 10);
}

function nearestWeekend(date: string): { start: string; end: string } {
  const value = new Date(`${date}T00:00:00Z`);
  const day = value.getUTCDay();
  const daysUntilSaturday = day === 6 ? 0 : day === 0 ? -1 : 6 - day;
  const start = datePlusDays(date, daysUntilSaturday);
  return { start, end: datePlusDays(start, 1) };
}

function isPublicFutureEvent(event: PreviewEvent, currentDate: string): boolean {
  return event.lifecycle_status === 'active' && (event.end_date || event.start_date) >= currentDate;
}

export function getMaterializedSearchCollectionDateRange(
  slug: string,
): { start: string; end: string } | null {
  if (slug !== 'dzhaz-na-vyhodnyh') return null;
  return nearestWeekend(getMaterializedSearchCollectionReferenceDate());
}

export function getMaterializedSearchCollection(slug: string): MaterializedSearchCollection | undefined {
  return materializedSearchCollections.find((collection) => collection.slug === slug);
}

export function getMaterializedSearchCollectionEvents(slug: string): PreviewEvent[] {
  const currentDate = getMaterializedSearchCollectionReferenceDate();
  const events = getEvents().filter((event) => isPublicFutureEvent(event, currentDate));
  let matches: PreviewEvent[] = [];

  if (slug === 'dzhaz-na-vyhodnyh') {
    const weekend = nearestWeekend(currentDate);
    matches = events.filter((event) => event.start_date >= weekend.start
      && event.start_date <= weekend.end
      && /джаз/iu.test(event.title));
  } else if (slug === 'besplatnye-sobytiya') {
    matches = events.filter((event) => event.ticket.is_free);
  } else if (slug === 'besplatno-s-detmi') {
    matches = events.filter((event) => event.ticket.is_free
      && event.topics.some((topic) => topic === 'FAMILY' || topic === 'KIDS_SCHOOL'));
  } else if (slug === 'stendap-na-etoy-nedele') {
    const end = datePlusDays(currentDate, 6);
    matches = events.filter((event) => event.start_date >= currentDate
      && event.start_date <= end
      && event.topics.includes('STANDUP'));
  }

  const collapsed = collapseOccurrenceCards(matches, 'per-family');
  return slug === 'besplatnye-sobytiya' ? collapsed : collapsed.slice(0, 24);
}

export function getMaterializedSearchCollectionFallbackEvents(slug: string): PreviewEvent[] {
  if (slug !== 'dzhaz-na-vyhodnyh') return [];
  const currentDate = getMaterializedSearchCollectionReferenceDate();
  const weekend = nearestWeekend(currentDate);
  const matches = getEvents().filter((event) => isPublicFutureEvent(event, currentDate)
    && event.start_date > weekend.end
    && /джаз/iu.test(event.title));
  return collapseOccurrenceCards(matches, 'per-family').slice(0, 3);
}
