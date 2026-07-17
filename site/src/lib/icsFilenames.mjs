function conciseAsciiSlug(value, maxLength) {
  const parts = String(value || '')
    .toLocaleLowerCase('en-US')
    .replace(/[^a-z0-9]+/gu, '-')
    .replace(/^-|-$/gu, '')
    .split('-')
    .filter(Boolean);
  const selected = [];
  let truncated = false;
  for (const part of parts) {
    const next = [...selected, part].join('-');
    if (next.length > maxLength) {
      truncated = true;
      break;
    }
    selected.push(part);
  }
  if (truncated && selected.length > 1 && selected.at(-1).length <= 2) selected.pop();
  return selected.join('-') || 'calendar';
}

export function eventIcsDownloadFilename(event) {
  const semanticSource = String(event.slug || '').replace(new RegExp(`-${event.id}$`, 'u'), '');
  const date = String(event.start_date || '').replace(/[^0-9]/gu, '') || 'date';
  return `event-${conciseAsciiSlug(semanticSource, 44)}-${date}-e${event.id}.ics`;
}

export function transportIcsDownloadFilename(event, trip) {
  return `${conciseAsciiSlug(trip, 64)}-e${event.id}.ics`;
}
