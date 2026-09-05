/** Preview-only presentation; owns no filters, brand menu, profile or network. */
export const REVIEW_VERSION = 'floating-islands.owner-review.v1.2';
export const REVIEW_PREFIX = /^\/preview-islands-[a-z0-9][a-z0-9-]*$/;

export function reviewEnabled(basePath, production = false) {
  return !production && typeof basePath === 'string'
    && REVIEW_PREFIX.test(basePath.replace(/\/$/, ''));
}

export function citySummary(all, cities) {
  if (all) return 'Все города';
  const selected = cities.filter(city => city.checked);
  if (!selected.length) return 'Города не выбраны';
  if (selected.length === 1) return selected[0].label;
  return `Города · ${selected.length}`;
}

export function equivalentFreeScope(pathname, basePath) {
  if (!reviewEnabled(basePath)) return false;
  const base = basePath.replace(/\/$/, '');
  return pathname === `${base}/podborki/besplatnye-sobytiya/`;
}

export function panelPlacement(anchor, viewport, occupiedBottom = 0) {
  const values = [anchor.left, anchor.bottom, viewport.left, viewport.top,
    viewport.width, viewport.height, occupiedBottom];
  if (values.some(value => !Number.isFinite(value)) || viewport.width <= 0 || viewport.height <= 0) {
    throw new TypeError('Finite positive viewport required');
  }
  const gap = 12;
  const width = Math.max(0, Math.min(420, viewport.width - gap * 2));
  const left = Math.max(viewport.left + gap,
    Math.min(anchor.left, viewport.left + viewport.width - gap - width));
  const top = Math.max(viewport.top + gap, anchor.bottom + 8);
  const height = Math.max(0, viewport.top + viewport.height - Math.max(0, occupiedBottom) - gap - top);
  return { left, top, width, maxHeight: height, inline: height < 144 || width < 240 };
}

/** Install once per document; destroy restores only the nodes/styles we own. */
export function initFloatingIslandsReview(doc = document, win = window) {
  const seed = doc.querySelector('[data-fi-review-seed]');
  if (!seed) return null;
  if (seed.__fiReview) return seed.__fiReview;
  const base = seed.dataset.fiBase || '';
  if (!reviewEnabled(base) || new URLSearchParams(win.location.search).get('islands') === 'off') return null;
  const controller = new win.AbortController();
  const options = { signal: controller.signal };
  const cleanups = [];
  const tasks = [];
  let frame = 0;
  const schedule = () => {
    if (controller.signal.aborted) return;
    if (!frame) frame = win.requestAnimationFrame(() => { frame = 0; tasks.forEach(fn => fn()); });
  };
  const root = doc.documentElement;
  root.dataset.fiReview = REVIEW_VERSION;
  cleanups.push(() => delete root.dataset.fiReview);

  // Only the page-context action is adapted. Never select or mutate brand/menu.
  const free = doc.querySelector('[data-free-collection-surface]');
  const context = doc.querySelector('[data-floating-page-context]');
  const action = context?.querySelector('button');
  const source = free?.querySelector('[data-free-collection-medallion="large"] img');
  if (free && context && action && source && equivalentFreeScope(win.location.pathname, base)
      && !win.location.search) {
    const image = doc.createElement('img');
    image.src = source.getAttribute('src');
    image.alt = ''; image.width = 64; image.height = 64;
    image.dataset.fiFreeMark = '';
    action.append(image);
    context.dataset.fiFreeContext = '';
    free.dataset.fiFreeIdentity = 'medallion-only';
    cleanups.push(() => {
      image.remove(); delete context.dataset.fiFreeContext; delete free.dataset.fiFreeIdentity;
    });
    // The existing context owner still chooses visibility, accessible name and
    // return-to-heading action. H1 and canonical hero medallion remain intact.
  }

  const template = seed.querySelector('template[data-fi-city-template]');
  doc.querySelectorAll('[data-listing-controls]').forEach((controls, index) => {
    const fieldset = controls.querySelector('[data-listing-city-filter]');
    if (!fieldset || !template || fieldset.querySelectorAll('[data-listing-city-input]').length < 2) return;
    const fragment = template.content.cloneNode(true);
    const toggle = fragment.querySelector('[data-fi-city-toggle]');
    const panel = fragment.querySelector('[data-fi-city-panel]');
    const close = fragment.querySelector('[data-fi-city-close]');
    const slot = fragment.querySelector('[data-fi-city-options]');
    const summary = toggle.querySelector('[data-fi-city-summary]');
    panel.id = `fi-cities-${index}`;
    toggle.setAttribute('aria-controls', panel.id);
    const marker = doc.createComment('Original city fieldset position');
    fieldset.before(marker);
    controls.insertBefore(fragment, fieldset);
    slot.append(fieldset); // The same checkboxes, labels, listeners and values.
    controls.dataset.fiCityRoot = '';
    const rail = controls.closest('[data-listing-discovery-rail]');
    if (rail) rail.dataset.fiCityRail = '';
    let compact = false;
    let opened = false;
    let natural = 0;
    let forceInline = false;
    const supportsPopover = typeof panel.showPopover === 'function';
    const hide = (restore = false) => {
      if (!opened) return;
      if (supportsPopover && panel.hasAttribute('popover') && panel.matches(':popover-open')) panel.hidePopover();
      opened = false;
      toggle.setAttribute('aria-expanded', 'false');
      if (compact && !panel.hasAttribute('popover')) panel.hidden = true;
      if (restore) toggle.focus({ preventScroll: true });
      schedule();
    };
    const updateSummary = () => {
      const all = controls.querySelector('[data-listing-city-all]');
      const cities = [...fieldset.querySelectorAll('[data-listing-city-input]')].map(input => ({
        checked: input.checked,
        label: input.closest('label')?.querySelector('span')?.textContent?.trim() || input.value,
      }));
      const label = citySummary(!all || all.checked, cities);
      if (summary.textContent !== label) summary.textContent = label;
      toggle.setAttribute('aria-label', `Выбор городов. ${label}`);
    };
    const position = () => {
      const vv = win.visualViewport;
      const view = { left: vv?.offsetLeft || 0, top: vv?.offsetTop || 0,
        width: vv?.width || win.innerWidth, height: vv?.height || win.innerHeight };
      const snapshot = win.KenigEventsShellOccupiedSpace?.();
      const occupied = Math.max(0, ...(snapshot?.rects || [])
        .filter(rect => ['navigation', 'date', 'event-cta', 'notification'].includes(rect.role))
        .filter(rect => rect.y >= view.top && rect.y < view.top + view.height)
        .map(rect => view.top + view.height - rect.y));
      const placement = panelPlacement(toggle.getBoundingClientRect(), view, occupied);
      if (placement.inline) return false;
      for (const [property, value] of Object.entries({ left:placement.left, top:placement.top,
        width:placement.width, maxHeight:placement.maxHeight })) panel.style[property] = `${value}px`;
      return true;
    };
    const update = () => {
      updateSummary();
      if (opened) {
        if (panel.hasAttribute('popover') && !position()) {
          panel.hidePopover(); panel.removeAttribute('popover'); panel.removeAttribute('style');
          panel.hidden = false; opened = true; forceInline = true;
          toggle.popoverTargetElement = null;
          toggle.setAttribute('aria-expanded', 'true');
        }
        return;
      }
      if (controls.contains(doc.activeElement)) return;
      if (!compact) {
        const labels = [...fieldset.querySelectorAll('label')];
        const gap = Number.parseFloat(win.getComputedStyle(fieldset).gap) || 8;
        natural = labels.reduce((sum, label) => sum + label.getBoundingClientRect().width, 0)
          + Math.max(0, labels.length - 1) * gap;
      }
      const available = rail?.getBoundingClientRect().width || controls.parentElement.getBoundingClientRect().width;
      const shouldCompact = !forceInline && (win.innerWidth < 980 || natural > available - (compact ? 24 : 0));
      if (shouldCompact === compact && controls.dataset.fiCityMode) return;
      compact = shouldCompact;
      controls.dataset.fiCityMode = compact ? 'compact' : 'inline';
      toggle.hidden = !compact;
      close.hidden = !compact;
      panel.hidden = compact && !supportsPopover;
      if (compact && supportsPopover) {
        panel.setAttribute('popover', 'auto');
        toggle.popoverTargetElement = panel;
      } else {
        panel.removeAttribute('popover'); panel.hidden = false;
        panel.removeAttribute('style'); toggle.popoverTargetElement = null;
      }
    };
    toggle.addEventListener('click', event => {
      event.preventDefault(); // One disclosure owner; native checkbox clicks untouched.
      if (opened) { hide(true); return; }
      if (supportsPopover && panel.hasAttribute('popover') && !position()) {
        forceInline = true;
        panel.removeAttribute('popover'); panel.removeAttribute('style');
        controls.dataset.fiCityMode = 'compact';
      }
      panel.hidden = false;
      if (supportsPopover && panel.hasAttribute('popover')) panel.showPopover();
      opened = true; toggle.setAttribute('aria-expanded', 'true');
    }, options);
    close.addEventListener('click', () => hide(true), options);
    panel.addEventListener('toggle', event => {
      if (!panel.hasAttribute('popover')) return;
      opened = event.newState === 'open';
      toggle.setAttribute('aria-expanded', String(opened));
      if (!opened) schedule();
    }, options);
    controls.addEventListener('change', () => win.queueMicrotask(updateSummary), options);
    controls.addEventListener('focusout', schedule, options);
    controls.addEventListener('keydown', event => {
      if (event.key === 'Escape' && opened) { event.stopPropagation(); event.preventDefault(); hide(true); }
    }, options);
    doc.addEventListener('pointerdown', event => {
      if (opened && !controls.contains(event.target)) hide(false);
    }, options);
    win.addEventListener('kenigevents:lower-surface-state', event => {
      if (event.detail?.modalOpen) hide(false); // The overlay owner restores/manages focus.
    }, options);
    win.addEventListener('storage', schedule, options);
    tasks.push(update);
    update();
    cleanups.push(() => {
      hide(false); marker.replaceWith(fieldset); panel.remove(); toggle.remove();
      delete controls.dataset.fiCityRoot; delete controls.dataset.fiCityMode;
      if (rail) delete rail.dataset.fiCityRail;
    });
  });

  win.addEventListener('resize', schedule, { ...options, passive:true });
  win.visualViewport?.addEventListener('resize', schedule, { ...options, passive:true });
  doc.fonts?.ready.then(() => { if (!controller.signal.aborted) schedule(); });
  const api = { destroy() {
    controller.abort(); if (frame) win.cancelAnimationFrame(frame); frame = 0;
    cleanups.reverse().forEach(fn => fn()); delete seed.__fiReview;
  }};
  seed.__fiReview = api;
  // Let the existing shell remeasure the resized dock; do not write its offsets.
  win.dispatchEvent(new win.Event('resize'));
  win.addEventListener('pagehide', event => { if (!event.persisted) api.destroy(); }, options);
  win.addEventListener('pageshow', schedule, options);
  return api;
}
