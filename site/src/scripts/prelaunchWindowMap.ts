const WINDOW_COUNT = 10;

function initializePrelaunchWindowMap(): void {
  const root = document.querySelector<HTMLElement>('[data-prelaunch-page]');
  const mosaic = root?.querySelector<HTMLElement>('[data-prelaunch-mosaic]');
  if (!root || !mosaic || root.dataset.windowMapReady === 'true') return;
  root.dataset.windowMapReady = 'true';

  const tiles = Array.from(root.querySelectorAll<HTMLElement>('[data-prelaunch-tile]'));
  let frame = 0;

  function calibrate(): void {
    const artwork = root.querySelector<HTMLElement>('.prelaunch__artwork');
    const artworkRect = artwork?.getBoundingClientRect();
    if (!artworkRect || artworkRect.width <= 0 || artworkRect.height <= 0) return;

    const targets = [
      {
        x: artworkRect.left + artworkRect.width * .49,
        y: artworkRect.top + artworkRect.height * .49,
      },
      {
        x: artworkRect.left + artworkRect.width * .51,
        y: artworkRect.top + artworkRect.height * .66,
      },
    ];

    const candidates = tiles.map((tile, index) => {
      const rect = tile.getBoundingClientRect();
      const centerX = rect.left + rect.width / 2;
      const centerY = rect.top + rect.height / 2;
      const score = Math.min(...targets.map((target) => Math.hypot(
        (centerX - target.x) / Math.max(1, rect.width * 1.08),
        (centerY - target.y) / Math.max(1, rect.height),
      )));
      const intersectsArtwork = !(
        rect.right < artworkRect.left
        || rect.left > artworkRect.right
        || rect.bottom < artworkRect.top
        || rect.top > artworkRect.bottom
      );
      return { tile, index, score, intersectsArtwork };
    });

    for (const tile of tiles) tile.dataset.window = 'false';
    candidates
      .filter(({ intersectsArtwork }) => intersectsArtwork)
      .sort((left, right) => left.score - right.score || left.index - right.index)
      .slice(0, WINDOW_COUNT)
      .forEach(({ tile }) => {
        tile.dataset.window = 'true';
      });

    root.dataset.windowMapCount = String(
      tiles.filter((tile) => tile.dataset.window === 'true').length,
    );
  }

  function schedule(): void {
    if (frame) cancelAnimationFrame(frame);
    frame = requestAnimationFrame(() => {
      frame = 0;
      calibrate();
    });
  }

  window.addEventListener('prelaunch-artwork-ready', schedule, { passive: true });
  window.addEventListener('resize', schedule, { passive: true });

  const observer = new MutationObserver(schedule);
  for (const tile of tiles) {
    observer.observe(tile, {
      attributes: true,
      attributeFilter: ['data-state'],
    });
  }

  if ('ResizeObserver' in window) {
    const resizeObserver = new ResizeObserver(schedule);
    resizeObserver.observe(mosaic);
  }

  schedule();
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initializePrelaunchWindowMap, { once: true });
} else {
  initializePrelaunchWindowMap();
}
