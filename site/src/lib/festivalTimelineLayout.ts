import type { FestivalTimelineItem } from '../data/festivalTimeline';

export interface FestivalTimelineRow {
  items: FestivalTimelineItem[];
  columnWeights: number[];
  mediaAspect: number;
}

const MAX_ROW_ITEMS = 4;
const IDEAL_MEDIA_HEIGHT = 0.22;
const GAP = 0.016;

function displayAspect(item: FestivalTimelineItem): number {
  const natural = item.imageWidth / item.imageHeight;
  // Visual-only portrait photos use the compact 5:4 target from the card
  // contract. Documentary media stays natural and therefore never loses text.
  if (item.mediaMode === 'visual' && natural < 1.25) return 1.25;
  return Math.min(natural, 1.9);
}

function rowCost(items: FestivalTimelineItem[], isFinal: boolean): number {
  const aspects = items.map(displayAspect);
  const mediaHeight = (1 - GAP * Math.max(0, items.length - 1)) / aspects.reduce((sum, value) => sum + value, 0);
  const heightCost = Math.abs(mediaHeight - IDEAL_MEDIA_HEIGHT) * 12;
  const sparseFinalCost = isFinal && items.length === 1 ? 0.42 : 0;
  const densePortraitCost = items.length === 4 && aspects.filter((value) => value < 1).length > 1 ? 0.7 : 0;
  return heightCost + sparseFinalCost + densePortraitCost + 0.08;
}

/**
 * Packs consecutive cards into full-width rows. The dynamic-programming score
 * minimizes vertical expansion first and only then row count. Ordering is
 * stable, so calendar chronology is never traded for a prettier mosaic.
 */
export function packFestivalTimeline(items: FestivalTimelineItem[]): FestivalTimelineRow[] {
  const best: Array<{ cost: number; sizes: number[] } | undefined> = Array(items.length + 1);
  best[items.length] = { cost: 0, sizes: [] };

  for (let index = items.length - 1; index >= 0; index -= 1) {
    for (let size = 1; size <= Math.min(MAX_ROW_ITEMS, items.length - index); size += 1) {
      const tail = best[index + size];
      if (!tail) continue;
      const candidate = {
        cost: rowCost(items.slice(index, index + size), index + size === items.length) + tail.cost,
        sizes: [size, ...tail.sizes],
      };
      const current = best[index];
      if (!current
        || candidate.cost < current.cost - 0.0001
        || (Math.abs(candidate.cost - current.cost) < 0.0001 && candidate.sizes.length < current.sizes.length)) {
        best[index] = candidate;
      }
    }
  }

  const rows: FestivalTimelineRow[] = [];
  let offset = 0;
  for (const size of best[0]?.sizes || [items.length]) {
    const rowItems = items.slice(offset, offset + size);
    const weights = rowItems.map(displayAspect);
    rows.push({
      items: rowItems,
      columnWeights: weights,
      mediaAspect: weights.reduce((sum, value) => sum + value, 0) / Math.max(1, rowItems.length),
    });
    offset += size;
  }
  return rows;
}
