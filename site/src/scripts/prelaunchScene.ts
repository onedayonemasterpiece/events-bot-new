const MAX_TILES = 112;
const MIN_PHASE_INTERVAL_MS = 3_200;
const MAX_PHASE_INTERVAL_MS = 5_600;

type PaneDepth = 'sealed' | 'dim' | 'clear';
type PaneLight = 'ambient' | 'soft' | 'warm' | 'hot';

type PaneBox = {
  x: number;
  y: number;
  width: number;
  height: number;
};

function roundedRectPath({ x, y, width, height }: PaneBox, radius: number): string {
  const r = Math.max(0, Math.min(radius, width / 2, height / 2));
  return [
    `M${x + r},${y}`,
    `H${x + width - r}`,
    `A${r},${r} 0 0 1 ${x + width},${y + r}`,
    `V${y + height - r}`,
    `A${r},${r} 0 0 1 ${x + width - r},${y + height}`,
    `H${x + r}`,
    `A${r},${r} 0 0 1 ${x},${y + height - r}`,
    `V${y + r}`,
    `A${r},${r} 0 0 1 ${x + r},${y}`,
    'Z',
  ].join('');
}

function randomBetween(minimum: number, maximum: number): number {
  return minimum + Math.random() * (maximum - minimum);
}

function initializePrelaunchScene(): void {
  const root = document.querySelector<HTMLElement>('[data-prelaunch-page]');
  if (!root || root.dataset.sceneBound === 'true') return;
  root.dataset.sceneBound = 'true';

  const mosaic = root.querySelector<HTMLElement>('[data-prelaunch-mosaic]');
  const seamSvg = root.querySelector<SVGSVGElement>('[data-prelaunch-seams]');
  const seamPath = root.querySelector<SVGPathElement>('[data-prelaunch-seam-path]');
  const artwork = root.querySelector<HTMLElement>('[data-prelaunch-artwork]');
  const artworkImage = root.querySelector<HTMLImageElement>('[data-prelaunch-artwork-image]');
  const panes = Array.from(root.querySelectorAll<HTMLElement>('[data-prelaunch-tile]')).slice(0, MAX_TILES);
  if (!mosaic || !seamSvg || !seamPath || !artwork || !artworkImage || panes.length === 0) return;

  const motionQuery = window.matchMedia('(prefers-reduced-motion: reduce)');
  let resizeFrame = 0;
  let phaseTimer = 0;

  function applyLayout(): void {
    const rootRect = root.getBoundingClientRect();
    const width = rootRect.width;
    const height = rootRect.height;
    const vmin = Math.min(width, height);
    const isMobile = width <= 599;
    const isCompactMobile = isMobile && height <= 680;
    const isLandscapePhone = width >= 600 && height <= 500;
    const isWide = width >= 1200 && height <= 1000 && width / height >= 4 / 3;

    let artworkSize = Math.min(vmin * .84, 1008);
    let artworkCenterX = width * .51;
    let artworkCenterY = height * .51;

    if (isWide) {
      artworkSize = Math.min(vmin * .84, 780);
      artworkCenterX = width * .64;
      artworkCenterY = height * .62;
    }
    if (isMobile) {
      artworkSize = Math.min(vmin * .72, 286);
      artworkCenterX = width * .54;
      artworkCenterY = height * .57;
    }
    if (isCompactMobile) {
      artworkSize = Math.min(vmin * .68, 226);
      artworkCenterY = height * .54;
    }
    if (isLandscapePhone) {
      artworkSize = Math.min(vmin * .68, 260);
      artworkCenterX = width * .50;
      artworkCenterY = height * .60;
    }

    const tileSize = isMobile
      ? Math.min(width * .155, 62)
      : isLandscapePhone
        ? Math.min(height * .145, 58)
        : artworkSize / 5.95;
    const tileGap = Math.max(isMobile ? 4 : 5, Math.min(vmin * .0068, isMobile ? 6 : 9));
    const tileRadius = Math.max(9, Math.min(vmin * .0112, 15));
    const pitch = tileSize + tileGap;
    const left = -tileSize * .05;
    const top = -tileSize * .45;
    const columns = Math.ceil((width - left + tileGap) / pitch);
    let rows = Math.ceil((height - top + tileGap) / pitch);
    if (columns * rows > panes.length) rows = Math.floor(panes.length / columns);

    root.style.setProperty('--prelaunch-artwork-size', `${artworkSize}px`);
    root.style.setProperty('--prelaunch-artwork-left', `${artworkCenterX}px`);
    root.style.setProperty('--prelaunch-artwork-top', `${artworkCenterY}px`);
    root.style.setProperty('--prelaunch-tile-size', `${tileSize}px`);
    root.style.setProperty('--prelaunch-tile-gap', `${tileGap}px`);
    root.style.setProperty('--prelaunch-tile-radius', `${tileRadius}px`);

    mosaic.style.left = `${left}px`;
    mosaic.style.top = `${top}px`;
    mosaic.style.gridTemplateColumns = `repeat(${columns}, var(--prelaunch-tile-size))`;

    const artworkBox = {
      left: artworkCenterX - artworkSize / 2,
      top: artworkCenterY - artworkSize / 2,
      width: artworkSize,
      height: artworkSize,
    };
    const sourceX = width * 1.04;
    const sourceY = -height * .06;
    const visibleBoxes: PaneBox[] = [];
    const clearCandidates: HTMLElement[] = [];
    let visibleCount = 0;
    let clearCount = 0;

    for (const [index, pane] of panes.entries()) {
      const visible = index < columns * rows;
      pane.hidden = !visible;
      if (!visible) continue;

      const row = Math.floor(index / columns);
      const column = index % columns;
      const x = left + column * pitch;
      const y = top + row * pitch;
      const centerX = x + tileSize / 2;
      const centerY = y + tileSize / 2;
      const artworkX = (centerX - artworkBox.left) / artworkBox.width;
      const artworkY = (centerY - artworkBox.top) / artworkBox.height;
      const insideArtwork = artworkX > -.04 && artworkX < 1.04 && artworkY > -.04 && artworkY < 1.04;
      const wordmarkBand = artworkX > .04 && artworkX < .96 && artworkY > .40 && artworkY < .90;
      const coherentCore = artworkX > .20 && artworkX < .82 && artworkY > .43 && artworkY < .90;
      const lowerLeather = artworkX > .01 && artworkX < .99 && artworkY > .30 && artworkY < .97;

      let depth: PaneDepth = 'sealed';
      if (coherentCore) {
        const pattern = (row * 2 + column * 3) % 6;
        depth = pattern < 3 ? 'clear' : pattern < 5 ? 'dim' : 'sealed';
        if (depth !== 'clear') clearCandidates.push(pane);
      } else if (wordmarkBand) {
        depth = (row + column) % 3 === 0 ? 'dim' : 'sealed';
      } else if (lowerLeather) {
        depth = (row * 3 + column) % 5 < 2 ? 'dim' : 'sealed';
      } else if (!insideArtwork && (row * 5 + column) % 8 === 0) {
        depth = 'dim';
      }

      const sourceDistance = Math.hypot(
        (sourceX - centerX) / tileSize,
        (sourceY - centerY) / tileSize,
      );
      let light: PaneLight = 'ambient';
      if (sourceDistance < 2.7) light = 'hot';
      else if (sourceDistance < 4.7) light = 'warm';
      else if (sourceDistance < 6.8) light = 'soft';

      pane.dataset.depth = depth;
      pane.dataset.light = light;
      pane.dataset.row = String(row);
      pane.dataset.column = String(column);
      pane.dataset.accent = light === 'hot' && (row + column) % 3 === 0 ? 'true' : 'false';
      pane.dataset.locked = depth === 'clear' ? 'true' : 'false';
      if (!pane.dataset.phase) pane.dataset.phase = index % 3 === 0 ? 'open' : 'closed';

      visibleBoxes.push({ x, y, width: tileSize, height: tileSize });
      visibleCount += 1;
      if (depth === 'clear') clearCount += 1;
    }

    if (isLandscapePhone && clearCount < 3) {
      for (const pane of clearCandidates) {
        if (clearCount >= 3) break;
        if (pane.dataset.depth === 'clear') continue;
        pane.dataset.depth = 'clear';
        pane.dataset.locked = 'true';
        clearCount += 1;
      }
    }

    seamSvg.setAttribute('viewBox', `0 0 ${width} ${height}`);
    let path = `M0,0H${width}V${height}H0Z`;
    for (const box of visibleBoxes) path += roundedRectPath(box, tileRadius);
    seamPath.setAttribute('d', path);

    root.dataset.sceneReady = 'true';
    root.dataset.gridColumns = String(columns);
    root.dataset.gridRows = String(rows);
    root.dataset.visibleTileCount = String(visibleCount);
    root.dataset.clearTileCount = String(clearCount);
    root.dataset.seamModel = 'inverse-svg-rounded-holes';
    root.dataset.artworkModel = 'source-asset-rounded-crop';
  }

  function scheduleLayout(): void {
    if (resizeFrame) cancelAnimationFrame(resizeFrame);
    resizeFrame = requestAnimationFrame(() => {
      resizeFrame = 0;
      applyLayout();
    });
  }

  function stopPhaseAnimation(): void {
    if (!phaseTimer) return;
    window.clearTimeout(phaseTimer);
    phaseTimer = 0;
  }

  function schedulePhaseAnimation(): void {
    stopPhaseAnimation();
    if (motionQuery.matches || document.hidden) return;
    phaseTimer = window.setTimeout(() => {
      const candidates = panes.filter((pane) => !pane.hidden && pane.dataset.locked !== 'true');
      const changeCount = Math.min(candidates.length, Math.floor(randomBetween(2, 4)));
      for (let index = 0; index < changeCount; index += 1) {
        const candidateIndex = Math.floor(Math.random() * candidates.length);
        const pane = candidates.splice(candidateIndex, 1)[0];
        if (!pane) continue;
        pane.dataset.phase = pane.dataset.phase === 'open' ? 'closed' : 'open';
        pane.style.setProperty('--delay', '0s');
      }
      schedulePhaseAnimation();
    }, randomBetween(MIN_PHASE_INTERVAL_MS, MAX_PHASE_INTERVAL_MS));
  }

  artworkImage.addEventListener('load', scheduleLayout, { once: true });
  window.addEventListener('resize', scheduleLayout, { passive: true });
  document.addEventListener('visibilitychange', schedulePhaseAnimation);
  motionQuery.addEventListener?.('change', schedulePhaseAnimation);

  if ('ResizeObserver' in window) {
    const observer = new ResizeObserver(scheduleLayout);
    observer.observe(root);
  }

  applyLayout();
  schedulePhaseAnimation();
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initializePrelaunchScene, { once: true });
} else {
  initializePrelaunchScene();
}
