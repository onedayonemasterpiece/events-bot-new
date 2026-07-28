import type { PreviewEvent } from './types';

export type FocusStance = 'like' | 'neutral' | 'not-for-me';

export interface FocusInterest {
  id: string;
  label: string;
  group: 'Музыка' | 'Культура' | 'Город и досуг';
  description: string;
}

export interface FocusRecommendation {
  event: PreviewEvent;
  interestIds: string[];
  fallbackReason: string;
}

export const FOCUS_STORAGE_KEY = 'kenigevents.focus-personalization.prototype.v1';

export const FOCUS_INTERESTS: FocusInterest[] = [
  {
    id: 'jazz',
    label: 'Джаз',
    group: 'Музыка',
    description: 'Клубные сеты, импровизация и джазовые фестивали.',
  },
  {
    id: 'classical',
    label: 'Классика',
    group: 'Музыка',
    description: 'Камерная музыка, опера, вокал и академические программы.',
  },
  {
    id: 'symphonic',
    label: 'Симфоническая музыка',
    group: 'Музыка',
    description: 'Оркестры, большие концертные программы и дирижёрские вечера.',
  },
  {
    id: 'rock',
    label: 'Рок-концерты',
    group: 'Музыка',
    description: 'Живые группы, гитарная сцена и большие клубные концерты.',
  },
  {
    id: 'electronic',
    label: 'Электроника и новая сцена',
    group: 'Музыка',
    description: 'Электронная музыка, инди, экспериментальные выступления и диджей-сеты.',
  },
  {
    id: 'literary',
    label: 'Литературные вечера',
    group: 'Культура',
    description: 'Чтения, книжные клубы, встречи с авторами и разговоры о текстах.',
  },
  {
    id: 'theatre',
    label: 'Театр и перформанс',
    group: 'Культура',
    description: 'Спектакли, пластический театр, перформансы и сценические эксперименты.',
  },
  {
    id: 'exhibitions',
    label: 'Выставки и искусство',
    group: 'Культура',
    description: 'Музеи, галереи, современное искусство, фотография и дизайн.',
  },
  {
    id: 'talks',
    label: 'Лекции и дискуссии',
    group: 'Культура',
    description: 'Открытые лекции, наука, история, публичные разговоры и мастер-классы.',
  },
  {
    id: 'cinema',
    label: 'Кино и медиа',
    group: 'Культура',
    description: 'Показы, фестивальное кино, анимация и встречи с авторами.',
  },
  {
    id: 'walks',
    label: 'Прогулки',
    group: 'Город и досуг',
    description: 'Неспешные маршруты, природные прогулки и городские наблюдения.',
  },
  {
    id: 'excursions',
    label: 'Экскурсии и архитектура',
    group: 'Город и досуг',
    description: 'История места, районы, здания и поездки по области.',
  },
  {
    id: 'family',
    label: 'С детьми',
    group: 'Город и досуг',
    description: 'Семейные события, занятия и программы для разных возрастов.',
  },
  {
    id: 'food',
    label: 'Еда и локальные вкусы',
    group: 'Город и досуг',
    description: 'Маркеты, гастрономические встречи, дегустации и локальные продукты.',
  },
  {
    id: 'festivals',
    label: 'Фестивали и праздники',
    group: 'Город и досуг',
    description: 'Большие городские программы, ярмарки и события на весь день.',
  },
  {
    id: 'active',
    label: 'Активный досуг',
    group: 'Город и досуг',
    description: 'Спорт, танцы, веломаршруты и занятия с движением.',
  },
];

const PROJECTED_TOPIC_TO_INTEREST: Record<string, string> = {
  THEATRE: 'theatre',
  THEATRE_CLASSIC: 'theatre',
  THEATRE_MODERN: 'theatre',
  EXHIBITIONS: 'exhibitions',
  LECTURES: 'talks',
  SCIENCE_POP: 'talks',
  MOVIES: 'cinema',
  FAMILY: 'family',
  ACTIVE: 'active',
};

export function focusInterestIdsForEvent(event: PreviewEvent): string[] {
  // Topics are already projected by the event pipeline. This presentation
  // layer intentionally does not reinterpret titles or prose with keywords.
  return [...new Set(
    (event.topics || [])
      .map((topic) => PROJECTED_TOPIC_TO_INTEREST[String(topic).toUpperCase()])
      .filter(Boolean),
  )].slice(0, 3);
}

export function buildFocusRecommendations(events: PreviewEvent[]): FocusRecommendation[] {
  return events.map((event) => {
    const interestIds = focusInterestIdsForEvent(event);
    const interestLabels = interestIds
      .map((id) => FOCUS_INTERESTS.find((interest) => interest.id === id)?.label)
      .filter(Boolean);
    return {
      event,
      interestIds,
      fallbackReason: interestLabels.length
        ? `Темы из каталога: ${interestLabels.join(', ')}. До вашего выбора это только объяснимая тематическая подсказка.`
        : 'В каталоге нет достаточно точного совпадения с темами прототипа. Карточка оставлена как разнообразный вариант, а не как персональное предсказание.',
    };
  });
}
