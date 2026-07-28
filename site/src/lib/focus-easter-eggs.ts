export const FOCUS_EGG_PROGRAM_ID = 'focus-2026-01';
export const FOCUS_EGG_RULES_VERSION = 'focus-prize-pending-v1';
export const FOCUS_EGG_COLLECTION_VERSION = 'focus-eggs-v1';
export const FOCUS_EGG_PLACEMENT_VERSION = 'focus-eggs-placement-v1';
export const FOCUS_EGG_STORAGE_KEY = 'kenigevents:focus-eggs:prototype:v1';
export const FOCUS_EGG_STORAGE_MAX_BYTES = 4_096;
export const FOCUS_PARTICIPATION_MAX_POINTS = 40;
export const FOCUS_PARTICIPATION_MAX_CATEGORIES = 7;

export type FocusEggId =
  | 'FG-E01'
  | 'FG-E02'
  | 'FG-E03'
  | 'FG-E04'
  | 'FG-E05'
  | 'FG-E06'
  | 'FG-E07'
  | 'FG-E08'
  | 'FG-E09'
  | 'FG-E10'
  | 'FG-E11'
  | 'FG-E12';

export type FocusEggState = 'locked' | 'eligible' | 'found' | 'unavailable';

export interface FocusEggDefinition {
  id: FocusEggId;
  title: string;
  hint: string;
  family: string;
}

export interface FocusEggPrototypeState {
  version: 1;
  collectionVersion: typeof FOCUS_EGG_COLLECTION_VERSION;
  foundEggIds: FocusEggId[];
}

export interface FocusEggStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
}

export interface FocusEggPlacement {
  eggId: 'FG-E12';
  anchorId: 'focus-egg-FG-E12';
  placementBundleId: string;
  insertAfterRenderableItem: 3;
  state: 'eligible' | 'found';
}

export interface FocusCollectionProgress {
  found: number;
  eligible: number;
  coverage: number;
}

export interface FocusParticipationProgress {
  points: number;
  maxPoints: typeof FOCUS_PARTICIPATION_MAX_POINTS;
  categories: number;
  maxCategories: typeof FOCUS_PARTICIPATION_MAX_CATEGORIES;
}

export const FOCUS_EGG_DEFINITIONS: readonly FocusEggDefinition[] = [
  { id: 'FG-E01', title: 'Искатель', hint: 'Попробуйте поиск с настоящим запросом.', family: 'Поиск' },
  { id: 'FG-E02', title: 'Важные детали', hint: 'Изучите основные сведения о событии.', family: 'Событие' },
  { id: 'FG-E03', title: 'Сегодняшний маршрут', hint: 'Посмотрите достаточно событий на сегодня.', family: 'Афиша' },
  { id: 'FG-E04', title: 'План на выходные', hint: 'Составьте более длинный список идей.', family: 'Афиша' },
  { id: 'FG-E05', title: 'Большие программы', hint: 'Загляните в каталог фестивалей.', family: 'Фестивали' },
  { id: 'FG-E06', title: 'Внутри фестиваля', hint: 'Изучите программу выбранного фестиваля.', family: 'Фестивали' },
  { id: 'FG-E07', title: 'Мой вкус', hint: 'Настройте интересы и объяснимые рекомендации.', family: 'Для меня' },
  { id: 'FG-E08', title: 'Честная оценка', hint: 'Ответьте на общий вопрос NPS — любая оценка равноценна.', family: 'Обратная связь' },
  { id: 'FG-E09', title: 'Голос участника', hint: 'Оставьте непустой отзыв о странице.', family: 'Обратная связь' },
  { id: 'FG-E10', title: 'Это интересно', hint: 'Отметьте первое понравившееся событие.', family: 'Реакции' },
  { id: 'FG-E11', title: 'Не моё', hint: 'Дизлайк так же полезен, как лайк.', family: 'Реакции' },
  { id: 'FG-E12', title: 'Третий план', hint: 'Соберите минимум три разных события в календаре.', family: 'Календарь' },
] as const;

const KNOWN_EGG_IDS = new Set<FocusEggId>(FOCUS_EGG_DEFINITIONS.map(({ id }) => id));

export function createFocusEggPrototypeState(): FocusEggPrototypeState {
  return {
    version: 1,
    collectionVersion: FOCUS_EGG_COLLECTION_VERSION,
    foundEggIds: [],
  };
}

export function resolveFocusEggState(input: {
  found?: boolean;
  eligible?: boolean;
  unavailable?: boolean;
}): FocusEggState {
  if (input.found) return 'found';
  if (input.unavailable) return 'unavailable';
  if (input.eligible) return 'eligible';
  return 'locked';
}

export function markFocusEggFound(
  state: FocusEggPrototypeState,
  eggId: FocusEggId,
): FocusEggPrototypeState {
  if (state.foundEggIds.includes(eggId)) return state;
  return {
    ...state,
    foundEggIds: [...state.foundEggIds, eggId],
  };
}

export function getFocusEggCollectionProgress(
  states: readonly FocusEggState[],
): FocusCollectionProgress {
  const found = states.filter((state) => state === 'found').length;
  const eligible = states.filter((state) => state !== 'unavailable').length;
  return {
    found,
    eligible,
    coverage: eligible === 0 ? 0 : found / eligible,
  };
}

export function capFocusParticipation(input: {
  points: number;
  categories: number;
}): FocusParticipationProgress {
  const safePoints = Number.isFinite(input.points) ? Math.max(0, Math.trunc(input.points)) : 0;
  const safeCategories = Number.isFinite(input.categories)
    ? Math.max(0, Math.trunc(input.categories))
    : 0;
  return {
    points: Math.min(FOCUS_PARTICIPATION_MAX_POINTS, safePoints),
    maxPoints: FOCUS_PARTICIPATION_MAX_POINTS,
    categories: Math.min(FOCUS_PARTICIPATION_MAX_CATEGORIES, safeCategories),
    maxCategories: FOCUS_PARTICIPATION_MAX_CATEGORIES,
  };
}

/**
 * FG-E12 is tied to the third distinct, currently renderable canonical item.
 * The returned bundle and anchor never depend on reorder, viewport or retries.
 */
export function getFgE12Placement(
  renderableEventIds: readonly string[],
  foundEggIds: readonly FocusEggId[] = [],
): FocusEggPlacement | null {
  const distinctRenderableIds = new Set(
    renderableEventIds.map((eventId) => eventId.trim()).filter(Boolean),
  );
  if (distinctRenderableIds.size < 3) return null;

  return {
    eggId: 'FG-E12',
    anchorId: 'focus-egg-FG-E12',
    placementBundleId: `${FOCUS_EGG_PLACEMENT_VERSION}:FG-E12`,
    insertAfterRenderableItem: 3,
    state: foundEggIds.includes('FG-E12') ? 'found' : 'eligible',
  };
}

export function parseFocusEggPrototypeState(raw: string | null): FocusEggPrototypeState {
  if (!raw || new TextEncoder().encode(raw).byteLength > FOCUS_EGG_STORAGE_MAX_BYTES) {
    return createFocusEggPrototypeState();
  }

  try {
    const value = JSON.parse(raw) as Partial<FocusEggPrototypeState>;
    if (
      value.version !== 1
      || value.collectionVersion !== FOCUS_EGG_COLLECTION_VERSION
      || !Array.isArray(value.foundEggIds)
    ) {
      return createFocusEggPrototypeState();
    }

    const foundEggIds = [...new Set(value.foundEggIds)]
      .filter((eggId): eggId is FocusEggId => (
        typeof eggId === 'string' && KNOWN_EGG_IDS.has(eggId as FocusEggId)
      ));

    return {
      version: 1,
      collectionVersion: FOCUS_EGG_COLLECTION_VERSION,
      foundEggIds,
    };
  } catch {
    return createFocusEggPrototypeState();
  }
}

export function readFocusEggPrototypeState(storage: FocusEggStorage): FocusEggPrototypeState {
  try {
    return parseFocusEggPrototypeState(storage.getItem(FOCUS_EGG_STORAGE_KEY));
  } catch {
    return createFocusEggPrototypeState();
  }
}

export function storeFocusEggPrototypeState(
  storage: FocusEggStorage,
  state: FocusEggPrototypeState,
): boolean {
  try {
    const serialized = JSON.stringify(state);
    if (new TextEncoder().encode(serialized).byteLength > FOCUS_EGG_STORAGE_MAX_BYTES) {
      return false;
    }
    storage.setItem(FOCUS_EGG_STORAGE_KEY, serialized);
    return true;
  } catch {
    return false;
  }
}
