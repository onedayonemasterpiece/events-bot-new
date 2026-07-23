/**
 * Return catalog cards in the order a person sees them: rows top-to-bottom,
 * then cards left-to-right. CSS grid placement must not make DOM adjacency the
 * keyboard contract.
 */
export function visualClubCardRows(cards, {
  rowTolerance = 16,
  rectFor = (card) => card.getBoundingClientRect(),
} = {}) {
  const measured = Array.from(cards || [], (card) => {
    const rect = rectFor(card);
    return {
      card,
      rect,
      centerX: rect.left + rect.width / 2,
    };
  }).filter(({ rect }) => Number.isFinite(rect?.top)
    && Number.isFinite(rect?.left)
    && Number.isFinite(rect?.width));

  measured.sort((left, right) => left.rect.top - right.rect.top || left.rect.left - right.rect.left);
  const rows = [];
  for (const entry of measured) {
    const row = rows.at(-1);
    if (!row || Math.abs(row.top - entry.rect.top) > rowTolerance) {
      rows.push({ top: entry.rect.top, cards: [entry] });
    } else {
      row.cards.push(entry);
    }
  }
  rows.forEach((row) => row.cards.sort((left, right) => left.rect.left - right.rect.left));
  return rows;
}

export function adjacentVisualClubCard(cards, current, code, options = {}) {
  const rows = visualClubCardRows(cards, options);
  const ordered = rows.flatMap((row) => row.cards.map(({ card }) => card));
  const index = ordered.indexOf(current);
  if (index < 0) return null;
  if (code === 'Home') return ordered[0] || null;
  if (code === 'End') return ordered.at(-1) || null;
  if (code === 'ArrowLeft') return ordered[index - 1] || null;
  if (code === 'ArrowRight') return ordered[index + 1] || null;
  if (code !== 'ArrowUp' && code !== 'ArrowDown') return null;

  const rowIndex = rows.findIndex((row) => row.cards.some(({ card }) => card === current));
  const destinationRow = rows[rowIndex + (code === 'ArrowUp' ? -1 : 1)];
  if (!destinationRow) return null;
  const currentEntry = rows[rowIndex].cards.find(({ card }) => card === current);
  return destinationRow.cards.reduce((nearest, candidate) => (
    !nearest
      || Math.abs(candidate.centerX - currentEntry.centerX) < Math.abs(nearest.centerX - currentEntry.centerX)
      ? candidate
      : nearest
  ), null)?.card || null;
}

export function initClubCatalogNavigation(options = {}) {
  const doc = options.document || document;
  const win = options.window || window;
  const root = options.root || doc.querySelector('[data-club-catalog]');
  if (!(root instanceof win.HTMLElement)) return { destroy() {} };

  const controller = new win.AbortController();
  const { signal } = controller;
  const desktopKeyboard = win.matchMedia('(min-width: 1024px)');
  const status = root.querySelector('[data-club-keyboard-status]');
  const cards = () => Array.from(root.querySelectorAll('[data-club-card]')).filter((card) => {
    if (!(card instanceof win.HTMLElement) || card.hidden) return false;
    const rect = card.getBoundingClientRect();
    const style = win.getComputedStyle(card);
    return rect.width > 0 && rect.height > 0 && style.display !== 'none' && style.visibility !== 'hidden';
  });

  const setStatus = (message) => {
    if (status instanceof win.HTMLElement) status.textContent = message;
  };
  const focusCard = (card) => {
    if (!(card instanceof win.HTMLElement)) return;
    card.focus({ preventScroll: true });
    card.scrollIntoView({ block: 'nearest', inline: 'nearest' });
    setStatus(`Выбран клуб ${card.dataset.clubName || ''}.`);
  };
  const showCoverFallback = (image) => {
    const card = image.closest('[data-club-card]');
    if (!(card instanceof win.HTMLElement)) return;
    card.dataset.coverState = 'fallback';
    image.hidden = true;
  };

  root.querySelectorAll('[data-club-cover]').forEach((image) => {
    image.addEventListener('error', () => showCoverFallback(image), { signal });
    if (image.complete && image.naturalWidth === 0) showCoverFallback(image);
  });

  root.addEventListener('keydown', (event) => {
    if (!desktopKeyboard.matches
      || event.defaultPrevented
      || event.isComposing
      || event.altKey
      || event.ctrlKey
      || event.metaKey
      || event.shiftKey) return;
    const target = event.target;
    const card = target instanceof win.Element ? target.closest('[data-club-card]') : null;
    if (!(card instanceof win.HTMLElement) || !root.contains(card)) return;

    // Nested links retain native Enter/Space behavior. Escape returns to the
    // card surface; no card shortcut can activate a different action.
    if (target !== card) {
      if (event.code === 'Escape') {
        event.preventDefault();
        focusCard(card);
      }
      return;
    }

    if (event.code === 'Enter' && !event.repeat) {
      const primary = card.querySelector('[data-club-primary-action]');
      if (primary instanceof win.HTMLAnchorElement) {
        event.preventDefault();
        primary.click();
      }
      return;
    }

    if (!['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown', 'Home', 'End'].includes(event.code)) return;
    const destination = adjacentVisualClubCard(cards(), card, event.code);
    if (!destination || destination === card) return;
    event.preventDefault();
    focusCard(destination);
  }, { capture: true, signal });

  return {
    destroy() {
      controller.abort();
    },
  };
}
