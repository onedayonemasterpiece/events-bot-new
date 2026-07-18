function candidateOf(item) {
  return item?.candidate || item || {};
}

function eventIdOf(item) {
  const candidate = candidateOf(item);
  return String(item?.event_id ?? candidate.event_id ?? candidate.id ?? '').trim();
}

function categoryOf(item) {
  const candidate = candidateOf(item);
  return String(candidate.category || candidate.event_type || candidate.display?.event_type || 'unknown').trim().toLowerCase();
}

function venueOf(item) {
  const candidate = candidateOf(item);
  return String(candidate.venue_id || candidate.location_name || candidate.display?.venue_name || 'unknown').trim().toLowerCase();
}

export function isRejectedContinuationCandidate(item) {
  const candidate = candidateOf(item);
  return candidate.gemma_reject === true || candidate.verification_state === 'llm_rejected';
}

export function selectEventContinuation(options = {}) {
  const limit = Math.max(0, Number(options.limit ?? 6));
  const maxSameCategory = Math.max(1, Number(options.maxSameCategory ?? 3));
  const maxSameVenue = Math.max(1, Number(options.maxSameVenue ?? 2));
  const blocked = new Set([
    String(options.currentEventId ?? '').trim(),
    ...(options.excludedIds || []).map((value) => String(value).trim()),
    ...(options.recentServedIds || []).map((value) => String(value).trim()),
  ].filter(Boolean));
  const seen = new Set(blocked);
  const categoryCounts = new Map();
  const venueCounts = new Map();
  const result = [];

  const admit = (item) => {
    const id = eventIdOf(item);
    if (!id || seen.has(id) || isRejectedContinuationCandidate(item)) return false;
    const category = categoryOf(item);
    const venue = venueOf(item);
    if ((categoryCounts.get(category) || 0) >= maxSameCategory) return false;
    if ((venueCounts.get(venue) || 0) >= maxSameVenue) return false;
    seen.add(id);
    categoryCounts.set(category, (categoryCounts.get(category) || 0) + 1);
    venueCounts.set(venue, (venueCounts.get(venue) || 0) + 1);
    result.push(item);
    return true;
  };

  const profile = options.profileCandidates || [];
  const adjacent = options.adjacentCandidates || [];
  const boundedLaneSize = Math.max(limit * 4, limit);
  let profileIndex = 0;
  let adjacentIndex = 0;
  while (result.length < limit && (profileIndex < Math.min(profile.length, boundedLaneSize) || adjacentIndex < Math.min(adjacent.length, boundedLaneSize))) {
    if (profileIndex < Math.min(profile.length, boundedLaneSize)) admit(profile[profileIndex++]);
    if (result.length >= limit) break;
    if (adjacentIndex < Math.min(adjacent.length, boundedLaneSize)) admit(adjacent[adjacentIndex++]);
  }

  // Stable catalog order is the final backfill. Caps stay hard: returning fewer
  // than six is safer than repeating a category/venue wall or a rejected pair.
  for (const item of (options.genericCandidates || []).slice(0, Math.max(limit * 12, limit))) {
    if (result.length >= limit) break;
    admit(item);
  }
  return result.slice(0, limit).map((item, rank) => ({ ...item, rank }));
}
