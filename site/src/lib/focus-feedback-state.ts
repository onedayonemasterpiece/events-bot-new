export const FOCUS_FEEDBACK_SCORE_STORAGE_KEY = 'ke_fg_scores_v1';
export const FOCUS_FEEDBACK_SCORE_TTL_MS = 24 * 60 * 60 * 1000;
export const FOCUS_FEEDBACK_SCORE_MAX_BYTES = 2_048;
export const FOCUS_FEEDBACK_SCORE_MAX_FAMILIES = 24;

export interface FocusFeedbackScoreEntry {
  score: number;
  updatedAt: number;
}

interface StoredScoresV1 {
  v: 1;
  s: Record<string, { n: number; t: number }>;
}

export interface FocusFeedbackStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

const FAMILY_PATTERN = /^[a-z][a-z0-9_]{0,47}$/u;

function bytes(value: string): number {
  return new TextEncoder().encode(value).byteLength;
}

export function readFocusFeedbackScores(
  storage: FocusFeedbackStorage,
  now = Date.now(),
): Record<string, FocusFeedbackScoreEntry> {
  try {
    const raw = storage.getItem(FOCUS_FEEDBACK_SCORE_STORAGE_KEY);
    if (!raw) return {};
    if (bytes(raw) > FOCUS_FEEDBACK_SCORE_MAX_BYTES) {
      storage.removeItem(FOCUS_FEEDBACK_SCORE_STORAGE_KEY);
      return {};
    }
    const parsed = JSON.parse(raw) as Partial<StoredScoresV1>;
    if (parsed.v !== 1 || !parsed.s || typeof parsed.s !== 'object' || Array.isArray(parsed.s)) {
      storage.removeItem(FOCUS_FEEDBACK_SCORE_STORAGE_KEY);
      return {};
    }
    const valid = Object.entries(parsed.s)
      .filter(([family, entry]) => (
        FAMILY_PATTERN.test(family)
        && entry
        && Number.isInteger(entry.n)
        && entry.n >= 0
        && entry.n <= 10
        && Number.isFinite(entry.t)
        && entry.t <= now
        && now - entry.t < FOCUS_FEEDBACK_SCORE_TTL_MS
      ))
      .sort((left, right) => right[1].t - left[1].t)
      .slice(0, FOCUS_FEEDBACK_SCORE_MAX_FAMILIES);
    const result: Record<string, FocusFeedbackScoreEntry> = {};
    for (const [family, entry] of valid) result[family] = { score: entry.n, updatedAt: entry.t };
    if (valid.length !== Object.keys(parsed.s).length) writeFocusFeedbackScores(storage, result);
    return result;
  } catch {
    try { storage.removeItem(FOCUS_FEEDBACK_SCORE_STORAGE_KEY); } catch { /* best effort */ }
    return {};
  }
}

function writeFocusFeedbackScores(
  storage: FocusFeedbackStorage,
  scores: Record<string, FocusFeedbackScoreEntry>,
): void {
  const entries = Object.entries(scores)
    .filter(([family, entry]) => FAMILY_PATTERN.test(family) && entry.score >= 0 && entry.score <= 10)
    .sort((left, right) => right[1].updatedAt - left[1].updatedAt)
    .slice(0, FOCUS_FEEDBACK_SCORE_MAX_FAMILIES);
  while (entries.length) {
    const payload: StoredScoresV1 = {
      v: 1,
      s: Object.fromEntries(entries.map(([family, entry]) => [
        family,
        { n: Math.trunc(entry.score), t: Math.trunc(entry.updatedAt) },
      ])),
    };
    const raw = JSON.stringify(payload);
    if (bytes(raw) <= FOCUS_FEEDBACK_SCORE_MAX_BYTES) {
      storage.setItem(FOCUS_FEEDBACK_SCORE_STORAGE_KEY, raw);
      return;
    }
    entries.pop();
  }
  storage.removeItem(FOCUS_FEEDBACK_SCORE_STORAGE_KEY);
}

export function rememberFocusFeedbackScore(
  storage: FocusFeedbackStorage,
  family: string,
  score: number,
  now = Date.now(),
): FocusFeedbackScoreEntry | null {
  if (!FAMILY_PATTERN.test(family) || !Number.isInteger(score) || score < 0 || score > 10) return null;
  const scores = readFocusFeedbackScores(storage, now);
  const entry = { score, updatedAt: now };
  scores[family] = entry;
  writeFocusFeedbackScores(storage, scores);
  return entry;
}
