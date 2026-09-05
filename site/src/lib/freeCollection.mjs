/** Russian event-count label for the one free-events listing. */
export function freeEventCountLabel(value) {
  const count = Math.max(0, Math.floor(Number(value) || 0));
  const mod100 = count % 100;
  const mod10 = count % 10;
  const noun = mod100 >= 11 && mod100 <= 14
    ? 'событий'
    : mod10 === 1
      ? 'событие'
      : mod10 >= 2 && mod10 <= 4
        ? 'события'
        : 'событий';
  return `${count} ${noun}`;
}

export function freeCollectionCountMessage(loaded, total) {
  const loadedCount = Math.max(0, Math.floor(Number(loaded) || 0));
  const totalCount = Math.max(loadedCount, Math.floor(Number(total) || 0));
  return loadedCount === totalCount
    ? freeEventCountLabel(totalCount)
    : `Показано ${freeEventCountLabel(loadedCount)} из ${freeEventCountLabel(totalCount)}`;
}
