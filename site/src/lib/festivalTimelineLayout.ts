import type { FestivalTimelineItem } from '../data/festivalTimeline';

export interface FestivalTimelineRow {
  items: FestivalTimelineItem[];
  sourceIndices: number[];
  columnWeights: number[];
  cropFractions: number[];
  normalizedMediaHeight: number;
  widthFraction: number;
  isRemainder: boolean;
}

interface RowCandidate extends FestivalTimelineRow {
  score: number;
  cropScore: number;
  permutationScore: number;
}

interface LayoutCandidate {
  rows: FestivalTimelineRow[];
  score: number;
  cropScore: number;
  permutationScore: number;
}

const MAX_ROW_ITEMS = 4;
const NORMALIZED_GAP = 0.014;
const EPSILON = 0.000001;

/**
 * Timeline rows are editorial strips, not masonry columns. Their height is
 * deliberately compact; the image crop is the variable that makes a visual
 * row fit. Values are relative to the full cards plane.
 */
const FULL_ROW_HEIGHT: Record<number, number> = {
  1: 1 / (16 / 9),
  2: 0.28,
  3: 0.225,
  4: 0.255,
};

function cropFraction(naturalAspect: number, frameAspect: number): number {
  if (Math.abs(naturalAspect - frameAspect) < EPSILON) return 0;
  const visibleFraction = naturalAspect < frameAspect
    ? naturalAspect / frameAspect
    : frameAspect / naturalAspect;
  return Math.max(0, 1 - visibleFraction);
}

function visualWeight(item: FestivalTimelineItem): number {
  const natural = item.imageWidth / item.imageHeight;
  if (natural >= 1.55) return 1.04;
  if (natural <= 0.86) return 0.96;
  return 1;
}

function rowPermutationScore(indices: number[]): number {
  let score = 0;
  for (let index = 1; index < indices.length; index += 1) {
    score += Math.max(0, indices[index] - indices[index - 1] - 1);
  }
  return score;
}

function finalRowGeometry(rowSize: number, isOnlyRow: boolean): {
  widthFraction: number;
  normalizedHeight: number;
} {
  if (rowSize === 1) {
    const widthFraction = isOnlyRow ? 0.62 : 0.4;
    return {
      widthFraction,
      normalizedHeight: widthFraction / (isOnlyRow ? 3 : 2.3),
    };
  }
  if (rowSize === 2) {
    return { widthFraction: 0.62, normalizedHeight: 0.195 };
  }
  return { widthFraction: 1, normalizedHeight: FULL_ROW_HEIGHT[rowSize] };
}

function bestRow(
  items: FestivalTimelineItem[],
  sourceIndices: number[],
  isFinal: boolean,
  isOnlyRow: boolean,
  remainingCount: number,
): RowCandidate | undefined {
  const rowSize = items.length;
  const geometry = isFinal
    ? finalRowGeometry(rowSize, isOnlyRow)
    : { widthFraction: 1, normalizedHeight: FULL_ROW_HEIGHT[rowSize] };
  const gapWidth = NORMALIZED_GAP * Math.max(0, rowSize - 1);
  const availableWidth = geometry.widthFraction - gapWidth;
  if (availableWidth <= 0) return undefined;

  if (items.some((item) => item.mediaMode !== 'visual' && item.mediaMode !== 'document')) {
    return undefined;
  }
  const hasDocument = items.some((item) => item.mediaMode === 'document');
  let normalizedHeight = geometry.normalizedHeight;
  let aspects: number[];
  let widths: number[];
  if (hasDocument) {
    const visualTargetAspect = rowSize === 4
      ? 0.96
      : rowSize === 3
        ? 1.4
        : rowSize === 2
          ? 1.52
          : 16 / 9;
    aspects = items.map((item) => (
      item.mediaMode === 'document'
        ? item.imageWidth / item.imageHeight
        : visualTargetAspect * visualWeight(item)
    ));
    normalizedHeight = availableWidth / aspects.reduce((sum, value) => sum + value, 0);
    widths = aspects.map((aspect) => aspect * normalizedHeight);
  } else {
    const baseWeights = items.map(visualWeight);
    const weightSum = baseWeights.reduce((sum, value) => sum + value, 0);
    widths = baseWeights.map((weight) => availableWidth * weight / weightSum);
    aspects = widths.map((width) => width / normalizedHeight);
  }
  const crops = items.map((item, index) => cropFraction(
    item.imageWidth / item.imageHeight,
    aspects[index],
  ));

  const unsafeDocument = items.some((item, index) => (
    item.mediaMode === 'document'
    && (
      crops[index] > 0.2 + EPSILON
      || (
        item.imageWidth / item.imageHeight >= 4 / 5
        && crops[index] > EPSILON
      )
    )
  ));
  if (unsafeDocument) return undefined;

  const cropScore = crops.reduce((sum, value, index) => (
    sum + value * (items[index].mediaMode === 'document' ? 12 : 0.8)
  ), 0);
  const lowResolutionScore = items.reduce((sum, item, index) => {
    const expectedSlotWidth = widths[index] * 1180;
    if (item.imageWidth >= expectedSlotWidth * 0.82) return sum;
    return sum + Math.min(2, (expectedSlotWidth * 0.82 - item.imageWidth) / 280);
  }, 0);
  const denseCopyScore = rowSize === 4
    ? items.reduce((sum, item, index) => {
      const pressure = item.title.length / Math.max(0.15, widths[index]);
      return sum + Math.max(0, pressure - 150) / 420;
    }, 0)
    : 0;
  // A non-final one/two-up strip consumes too much height. It remains legal
  // for hard cases, but the whole-group optimiser strongly prefers 4+3,
  // 4+1, 3+3 and similar compact formations.
  const preferredRowSize = remainingCount >= 7
    ? 4
    : remainingCount === 6
      ? 3
      : remainingCount === 5 || remainingCount === 4
        ? 4
        : remainingCount;
  const formationScore = rowSize === preferredRowSize
    ? 0
    : (rowSize === 1 && !isFinal ? 4 : 1.15);
  const permutationScore = rowPermutationScore(sourceIndices);
  const score = normalizedHeight
    + cropScore * 0.06
    + lowResolutionScore
    + denseCopyScore
    + formationScore
    + permutationScore * 0.018;

  return {
    items,
    sourceIndices,
    columnWeights: aspects,
    cropFractions: crops,
    normalizedMediaHeight: normalizedHeight,
    widthFraction: geometry.widthFraction,
    isRemainder: isFinal && geometry.widthFraction < 1,
    score,
    cropScore,
    permutationScore,
  };
}

function firstUnusedIndex(mask: number, length: number): number {
  for (let index = 0; index < length; index += 1) {
    if ((mask & (1 << index)) === 0) return index;
  }
  return -1;
}

function rowIndexSets(mask: number, length: number): number[][] {
  const first = firstUnusedIndex(mask, length);
  if (first < 0) return [];
  const rest: number[] = [];
  for (let index = first + 1; index < length; index += 1) {
    if ((mask & (1 << index)) === 0) rest.push(index);
  }
  const result: number[][] = [];
  const choose = (cursor: number, selected: number[]) => {
    result.push([first, ...selected]);
    if (selected.length >= MAX_ROW_ITEMS - 1) return;
    for (let index = cursor; index < rest.length; index += 1) {
      selected.push(rest[index]);
      choose(index + 1, selected);
      selected.pop();
    }
  };
  choose(0, []);
  return result;
}

function isBetter(candidate: LayoutCandidate, current?: LayoutCandidate): boolean {
  if (!current) return true;
  if (candidate.score < current.score - EPSILON) return true;
  if (Math.abs(candidate.score - current.score) >= EPSILON) return false;
  if (candidate.rows.length !== current.rows.length) return candidate.rows.length < current.rows.length;
  if (Math.abs(candidate.cropScore - current.cropScore) >= EPSILON) {
    return candidate.cropScore < current.cropScore;
  }
  return candidate.permutationScore < current.permutationScore;
}

/**
 * Whole-month bitmask DP. It evaluates all legal 1–4-card formations and a
 * bounded set of order changes, then minimises the actual normalised strip
 * height plus crop, resolution and permutation costs. Every non-final row
 * fills 100%; a final one/two-card remainder may intentionally stay compact.
 */
export function packFestivalTimeline(items: FestivalTimelineItem[]): FestivalTimelineRow[] {
  if (items.length === 0) return [];
  if (items.length >= 31) throw new Error('Festival timeline bitmask supports at most 30 items per group');
  const fullMask = (1 << items.length) - 1;
  const memo = new Map<number, LayoutCandidate | undefined>();

  const solve = (mask: number): LayoutCandidate | undefined => {
    if (mask === fullMask) return { rows: [], score: 0, cropScore: 0, permutationScore: 0 };
    if (memo.has(mask)) return memo.get(mask);
    let best: LayoutCandidate | undefined;
    let usedCount = 0;
    for (let value = mask; value; value >>= 1) usedCount += value & 1;
    const remainingCount = items.length - usedCount;

    for (const indices of rowIndexSets(mask, items.length)) {
      const rowMask = indices.reduce((value, index) => value | (1 << index), 0);
      const nextMask = mask | rowMask;
      const isFinal = nextMask === fullMask;
      const row = bestRow(
        indices.map((index) => items[index]),
        indices,
        isFinal,
        mask === 0 && isFinal,
        remainingCount,
      );
      if (!row) continue;
      const tail = solve(nextMask);
      if (!tail) continue;
      const candidate: LayoutCandidate = {
        rows: [row, ...tail.rows],
        score: row.score + tail.score,
        cropScore: row.cropScore + tail.cropScore,
        permutationScore: row.permutationScore + tail.permutationScore,
      };
      if (isBetter(candidate, best)) best = candidate;
    }
    memo.set(mask, best);
    return best;
  };

  return solve(0)?.rows || [];
}
