import type { PreviewEvent } from './types';

function exactChronologyTime(event: Pick<PreviewEvent, 'start_time' | 'display_time'>): string | null {
  const match = /(\d{1,2}):(\d{2})/u.exec(event.start_time || event.display_time || '');
  return match ? `${match[1].padStart(2, '0')}:${match[2]}` : null;
}

/** Stable chronological order for physical date rails and completed sections. */
export function chronologicalListingEvents(items: PreviewEvent[]): PreviewEvent[] {
  return [...items].sort((left, right) => {
    const leftTime = exactChronologyTime(left);
    const rightTime = exactChronologyTime(right);
    if (leftTime === rightTime) {
      return String(left.start_date || '').localeCompare(String(right.start_date || ''))
        || Number(left.id) - Number(right.id);
    }
    if (!leftTime) return 1;
    if (!rightTime) return -1;
    return leftTime.localeCompare(rightTime);
  });
}
