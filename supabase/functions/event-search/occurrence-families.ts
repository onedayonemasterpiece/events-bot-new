export type OccurrenceCandidate = Record<string, unknown>;

function candidateId(candidate: OccurrenceCandidate): number | null {
  const display = candidate?.display && typeof candidate.display === "object"
    ? candidate.display as OccurrenceCandidate
    : {};
  const value = Number(
    candidate?.event_id ?? candidate?.id ?? display.event_id ?? display.id,
  );
  return Number.isFinite(value) && value > 0 ? Math.trunc(value) : null;
}

export function occurrenceMemberIds(candidate: OccurrenceCandidate): number[] {
  const display = candidate?.display && typeof candidate.display === "object"
    ? candidate.display as OccurrenceCandidate
    : {};
  const raw = Array.isArray(display.occurrence_member_ids)
    ? display.occurrence_member_ids
    : Array.isArray(candidate.occurrence_member_ids)
      ? candidate.occurrence_member_ids
      : [];
  return Array.from(
    new Set(
      raw
        .map(Number)
        .filter((value) => Number.isFinite(value) && value > 0)
        .map(Math.trunc),
    ),
  ).sort((left, right) => left - right);
}

/**
 * The sync projection emits the same exact member list for every member of a
 * reciprocal explicit family. A malformed one-way/dangling payload must not
 * turn an unrelated event into a family representative, so self-membership is
 * mandatory and no title/type/venue fallback exists here.
 */
export function occurrenceFamilyKey(candidate: OccurrenceCandidate): string {
  const eventId = candidateId(candidate);
  const memberIds = occurrenceMemberIds(candidate);
  return eventId !== null && memberIds.length > 1 && memberIds.includes(eventId)
    ? `family:${memberIds.join(",")}`
    : `event:${eventId ?? ""}`;
}

/** Preserve input ranking: the first (highest-ranked) family member wins. */
export function collapseOccurrenceFamilies<T extends OccurrenceCandidate>(
  candidates: T[],
  seen = new Set<string>(),
): T[] {
  return candidates.filter((candidate) => {
    const key = occurrenceFamilyKey(candidate);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

/**
 * Offset is applied after family collapse. Calling this on the same complete
 * ranked pool makes pagination stable and prevents a sibling from reappearing
 * on a later page.
 */
export function paginateOccurrenceFamilies<T extends OccurrenceCandidate>(
  candidates: T[],
  offset: number,
  windowSize: number,
): {
  items: T[];
  retrievedCount: number;
  nextOffset: number;
  hasMore: boolean;
} {
  const families = collapseOccurrenceFamilies(candidates);
  const safeOffset = Math.max(0, Math.trunc(offset));
  const safeWindow = Math.max(1, Math.trunc(windowSize));
  const items = families.slice(safeOffset, safeOffset + safeWindow);
  const nextOffset = safeOffset + items.length;
  return {
    items,
    retrievedCount: items.length,
    nextOffset,
    hasMore: nextOffset < families.length,
  };
}
