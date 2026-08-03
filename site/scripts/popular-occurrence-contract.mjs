function normalizedLinkedIds(event) {
  const ownId = String(event?.id ?? '');
  return [...new Set((event?.other_date_ids || [])
    .map((id) => String(id))
    .filter((id) => id && id !== ownId))];
}

/**
 * Validate repeated-date collapse against the ranked cards that actually exist.
 *
 * The production catalog is mutable, so a healthy Popular selection may contain
 * zero occurrence families. A fixture-specific literal such as `ещё 1 показ`
 * must therefore never be required unconditionally.
 */
export function assertPopularOccurrenceCollapse({ desktopIds, temporalLabels, events }) {
  const eventById = new Map(events.map((event) => [String(event.id), event]));
  const selectedIds = new Set(desktopIds.map(String));

  desktopIds.forEach((rawId, index) => {
    const id = String(rawId);
    const linkedIds = normalizedLinkedIds(eventById.get(id));
    for (const linkedId of linkedIds) {
      if (selectedIds.has(linkedId)) {
        throw new Error(`Popular desktop renders linked occurrence ${linkedId} as a second card for family ${id}`);
      }
    }
    if (linkedIds.length === 0) return;

    const label = String(temporalLabels[index] || '');
    const repeatSummary = new RegExp(`ещё ${linkedIds.length} показ(?:а|ов)?$`, 'u');
    if (!repeatSummary.test(label)) {
      throw new Error(`Popular desktop family ${id} must summarize ${linkedIds.length} linked occurrence(s)`);
    }
  });
}
