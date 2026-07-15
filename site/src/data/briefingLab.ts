export type BriefingLabScenario = {
  id: string;
  label: string;
  headline: string;
  supportingText: string;
  ctaLabel: string;
};

// Fixed editorial fixtures only: no profile, API, runtime LLM, or generated copy.
export const briefingLabScenarios = [
  { id: 'today_count', label: 'Сегодня', headline: 'Сегодня — события на любой план', supportingText: 'Выберите концерт, выставку или встречу.', ctaLabel: 'Смотреть сегодня' },
  { id: 'tomorrow_count', label: 'Завтра', headline: 'Завтра в городе есть из чего выбрать', supportingText: 'Соберите план заранее и сохраните интересное.', ctaLabel: 'Смотреть завтра' },
  { id: 'weekend_count', label: 'Выходные', headline: 'Идеи для ближайших выходных', supportingText: 'Прогулки, спектакли и музыка — в одной подборке.', ctaLabel: 'Открыть выходные' },
  { id: 'exhibitions_count', label: 'Выставки', headline: 'Выставки, которые можно увидеть сейчас', supportingText: 'Выберите музей, галерею или небольшое пространство.', ctaLabel: 'Смотреть выставки' },
  { id: 'free_count', label: 'Бесплатно', headline: 'Куда пойти без билета', supportingText: 'Собрали события со свободным входом.', ctaLabel: 'Смотреть сегодня' },
  { id: 'tonight_count', label: 'Вечером', headline: 'Планы на сегодняшний вечер', supportingText: 'Концерты, спектакли и встречи после рабочего дня.', ctaLabel: 'Смотреть сегодня' },
  { id: 'newly_added_count', label: 'Новое', headline: 'В афише появились новые события', supportingText: 'Посмотрите свежие пополнения общего городского каталога.', ctaLabel: 'Открыть афишу' },
  { id: 'catalog_generic', label: 'Каталог', headline: 'События города — в одной афише', supportingText: 'Начните с даты или подходящего формата.', ctaLabel: 'Открыть афишу' },
] as const satisfies readonly BriefingLabScenario[];

export const briefingLabFallback = {
  id: 'neutral_fallback', label: 'Нейтральный fallback', headline: 'Найдите событие для своего дня', supportingText: 'Афиша Калининграда и области — без лишнего шума.', ctaLabel: 'Смотреть афишу',
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
