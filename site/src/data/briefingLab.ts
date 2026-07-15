export type BriefingLabScenario = {
  id: string;
  label: string;
  headline: string;
  supportingText: string;
  ctaLabel: string;
  ctaHref: string;
};

export type BriefingLabEvent = {
  id: string;
  category: string;
  title: string;
  meta: string;
  admission: string;
  href: string;
};

// Research-only inventory: one deterministic, editorially fixed copy per case.
// There is no runtime/offline LLM generation or user-profile input.
export const briefingLabScenarios: readonly BriefingLabScenario[] = [
  { id: 'today_count', label: 'Сегодня', headline: 'Сегодня — события на любой план', supportingText: 'Выберите концерт, выставку или встречу.', ctaLabel: 'Смотреть сегодня', ctaHref: '/segodnya/' },
  { id: 'tomorrow_count', label: 'Завтра', headline: 'Завтра в городе есть из чего выбрать', supportingText: 'Соберите план заранее и сохраните интересное.', ctaLabel: 'Смотреть завтра', ctaHref: '/zavtra/' },
  { id: 'weekend_count', label: 'Выходные', headline: 'Идеи для ближайших выходных', supportingText: 'Прогулки, спектакли и музыка — в одной подборке.', ctaLabel: 'Открыть выходные', ctaHref: '/vyhodnye/' },
  { id: 'exhibitions_count', label: 'Выставки', headline: 'Выставки, которые можно увидеть сейчас', supportingText: 'Выберите музей, галерею или небольшое пространство.', ctaLabel: 'Смотреть выставки', ctaHref: '/vystavki/' },
  { id: 'free_count', label: 'Бесплатно', headline: 'Куда пойти без билета', supportingText: 'Собрали события со свободным входом.', ctaLabel: 'Смотреть сегодня', ctaHref: '/segodnya/#free' },
  { id: 'tonight_count', label: 'Вечером', headline: 'Планы на сегодняшний вечер', supportingText: 'Концерты, спектакли и встречи после рабочего дня.', ctaLabel: 'Смотреть сегодня', ctaHref: '/segodnya/#evening' },
  { id: 'newly_added_count', label: 'Новое', headline: 'В афише появились новые события', supportingText: 'Посмотрите свежие пополнения общего городского каталога.', ctaLabel: 'Открыть афишу', ctaHref: '/segodnya/#new' },
  { id: 'catalog_generic', label: 'Каталог', headline: 'События города — в одной афише', supportingText: 'Начните с даты или подходящего формата.', ctaLabel: 'Открыть афишу', ctaHref: '/segodnya/' },
] as const;

export const briefingLabFallback: BriefingLabScenario = {
  id: 'neutral_fallback',
  label: 'Нейтральный fallback',
  headline: 'Найдите событие для своего дня',
  supportingText: 'Афиша Калининграда и области — без лишнего шума.',
  ctaLabel: 'Смотреть афишу',
  ctaHref: '/segodnya/',
};

export const defaultBriefingLabScenario = briefingLabScenarios[0] ?? briefingLabFallback;

// Local lab fixtures deliberately do not depend on production event exports.
export const briefingLabEvents: readonly BriefingLabEvent[] = [
  { id: 'lab-concert-01', category: 'Концерт', title: 'Камерная музыка в старом зале', meta: 'Сегодня, 19:00 · Центр города', admission: 'от 600 ₽', href: '#lab-concert-01' },
  { id: 'lab-exhibition-02', category: 'Выставка', title: 'Город у моря: графика и свет', meta: 'Сегодня, до 20:00 · Галерея', admission: 'Свободный вход', href: '#lab-exhibition-02' },
  { id: 'lab-lecture-03', category: 'Встреча', title: 'Как читать архитектуру Калининграда', meta: 'Сегодня, 18:30 · Библиотека', admission: 'По регистрации', href: '#lab-lecture-03' },
] as const;

export const briefingLabCategories = ['Все события', 'Концерты', 'Выставки', 'Встречи'] as const;
