import type { FocusGroupPageFamily } from './focus-group-surface';

export const FOCUS_PAGE_RATING_TTL_MS = 24 * 60 * 60 * 1000;
const STORAGE_PREFIX = 'ke_focus_page_rating_v1';

export type FocusPageRatingType =
  | 'home'
  | 'calendar'
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

export interface FocusPageRating {
  score: number;
  savedAt: number;
}

interface RatingStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

export function focusPageRatingType(family: FocusGroupPageFamily): FocusPageRatingType {
  if (family === 'today' || family === 'tomorrow' || family === 'calendar_date') return 'calendar';
  return family;
}

export function focusPageRatingStorageKey(userId: string, family: FocusGroupPageFamily): string {
  return `${STORAGE_PREFIX}:${encodeURIComponent(userId)}:${focusPageRatingType(family)}`;
}

export function readFocusPageRating(
  storage: RatingStorage,
  userId: string,
  family: FocusGroupPageFamily,
  now = Date.now(),
): FocusPageRating | null {
  const key = focusPageRatingStorageKey(userId, family);
  let raw: string | null = null;
  try {
    raw = storage.getItem(key);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as Partial<FocusPageRating>;
    if (
      !Number.isInteger(parsed.score)
      || Number(parsed.score) < 0
      || Number(parsed.score) > 10
      || !Number.isFinite(parsed.savedAt)
      || now - Number(parsed.savedAt) >= FOCUS_PAGE_RATING_TTL_MS
      || Number(parsed.savedAt) > now + 60_000
    ) {
      storage.removeItem(key);
      return null;
    }
    return { score: Number(parsed.score), savedAt: Number(parsed.savedAt) };
  } catch {
    try { storage.removeItem(key); } catch { /* best effort */ }
    return null;
  }
}

export function writeFocusPageRating(
  storage: RatingStorage,
  userId: string,
  family: FocusGroupPageFamily,
  score: number,
  now = Date.now(),
): FocusPageRating {
  if (!Number.isInteger(score) || score < 0 || score > 10) {
    throw new RangeError('Focus page rating must be an integer from 0 to 10.');
  }
  const rating = { score, savedAt: now };
  storage.setItem(focusPageRatingStorageKey(userId, family), JSON.stringify(rating));
  return rating;
}
