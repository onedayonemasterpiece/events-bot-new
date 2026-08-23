/** Reviewed V7 desktop event keyboard router.
 * Source contract: d0027a53. DOM actions delegate to the existing controls.
 */
const GALLERY_HANDOFF_STORAGE_KEY = 'ke_keyboard_gallery_handoff_v1';
const GALLERY_HANDOFF_TTL_MS = 30_000;

export function keyboardGalleryDestination(href, currentHref) {
  try {
    const current = new URL(currentHref);
    const destination = new URL(href, current);
    if (destination.origin !== current.origin || !/\/sobytiya\/[^/]+\/?$/u.test(destination.pathname)) return '';
    return `${destination.pathname}${destination.search}`;
  } catch {
    return '';
  }
}

function rectIntersection(rect, viewportWidth, viewportHeight) {
  if (!rect || !(rect.width > 0) || !(rect.height > 0)) return { width:0, height:0 };
  return {
    width:Math.max(0, Math.min(rect.right, viewportWidth) - Math.max(rect.left, 0)),
    height:Math.max(0, Math.min(rect.bottom, viewportHeight) - Math.max(rect.top, 0)),
  };
}

/** Pure ownership policy used by the router and focused regression tests. */
export function footerViewportShortcutOwnership({ footerRect, targetRect = null, targetKind, viewportWidth, viewportHeight }) {
  if (targetKind === 'footer') return true;
  if (!['body', 'managed-card', 'event-surface'].includes(targetKind)) return false;
  const footerIntersection = rectIntersection(footerRect, viewportWidth, viewportHeight);
  const footerVisibleEnough = footerIntersection.width >= Math.min(120, footerRect?.width || 0)
    && footerIntersection.height >= Math.min(72, (footerRect?.height || 0) * 0.35);
  if (!footerVisibleEnough) return false;
  if (targetKind === 'body') return true;
  const targetIntersection = rectIntersection(targetRect, viewportWidth, viewportHeight);
  return targetIntersection.width === 0 || targetIntersection.height === 0;
}

/**
 * Builds the keyboard order from the rendered card geometry rather than DOM
 * order. The recommendation optimizer deliberately moves cards with CSS grid
 * coordinates, so DOM adjacency is not a reliable description of what a
 * person sees.
 */
export function visualCardRows(cards, { rowTolerance = 16, rectFor = (card) => card.getBoundingClientRect() } = {}) {
  const measured = Array.from(cards || [], (card) => {
    const rect = rectFor(card);
    return { card, rect, centerX:rect.left + rect.width / 2 };
  }).filter(({ rect }) => Number.isFinite(rect?.top) && Number.isFinite(rect?.left));
  measured.sort((left, right) => left.rect.top - right.rect.top || left.rect.left - right.rect.left);
  const rows = [];
  for (const entry of measured) {
    const row = rows.at(-1);
    if (!row || Math.abs(row.top - entry.rect.top) > rowTolerance) {
      rows.push({ top:entry.rect.top, cards:[entry] });
    } else {
      row.cards.push(entry);
    }
  }
  rows.forEach((row) => row.cards.sort((left, right) => left.rect.left - right.rect.left));
  return rows;
}

export function initKeyboardEventNavigation(options = {}) {
  const doc = options.document || document;
  const win = options.window || window;
  const abortController = new win.AbortController();
  const { signal } = abortController;
  const observers = new Set();
  const timeouts = new Set();
  const frames = new Set();
  const attributeSnapshots = new Map();
  let destroyed = false;
  let activated = false;
  const observe = (callback) => {
    const observer = new win.MutationObserver(callback);
    observers.add(observer);
    return observer;
  };
  const listen = (target, type, handler, init = {}) => {
    target?.addEventListener(type, handler, typeof init === 'boolean' ? { capture: init, signal } : { ...init, signal });
  };
  const scheduleTimeout = (callback, delay) => {
    const id = win.setTimeout(() => { timeouts.delete(id); callback(); }, delay);
    timeouts.add(id);
    return id;
  };
  const scheduleFrame = (callback) => {
    const id = win.requestAnimationFrame((time) => { frames.delete(id); callback(time); });
    frames.add(id);
    return id;
  };
  const rememberAttribute = (element, name) => {
    if (!(element instanceof win.Element)) return;
    let snapshot = attributeSnapshots.get(element);
    if (!snapshot) {
      snapshot = new Map();
      attributeSnapshots.set(element, snapshot);
    }
    if (!snapshot.has(name)) {
      snapshot.set(name, element.hasAttribute(name) ? element.getAttribute(name) : null);
    }
  };
  const setManagedAttribute = (element, name, value = '') => {
    if (!(element instanceof win.Element)) return;
    rememberAttribute(element, name);
    element.setAttribute(name, String(value));
  };
  const restoreManagedAttributes = () => {
    attributeSnapshots.forEach((snapshot, element) => {
      if (!(element instanceof win.Element)) return;
      snapshot.forEach((value, name) => {
        if (value === null) element.removeAttribute(name);
        else element.setAttribute(name, value);
      });
    });
    attributeSnapshots.clear();
  };
  const meaningfulCodes = new Set(['ArrowLeft','ArrowRight','ArrowUp','ArrowDown','Enter','KeyL','KeyK','KeyS','KeyC','KeyP','Home','End']);
  const activateForIntent = (event) => {
    if (activated) return true;
    if (!meaningfulCodes.has(event.code) || event.isComposing || event.altKey || event.ctrlKey || event.metaKey || event.shiftKey) return false;
    const target = event.target;
    const owned = target === doc.body || target === doc.documentElement
      || (target instanceof win.Element && (
        root?.contains(target)
        || Boolean(target.closest('[data-personal-feed-section][data-listing-context="event-detail"],[data-hero-gallery],[data-personalization-consent],[data-service-share-root][data-service-share-surface="footer"]'))
      ));
    if (!owned) return false;
    activated = true;
    return true;
  };
  const root = doc.querySelector('[data-desktop-clean-event]');
  const status = doc.querySelector('[data-keyboard-prototype-status]');
  const startButton = doc.querySelector('[data-keyboard-prototype-start]');

  const isVisible = (element) => {
    if (!(element instanceof win.HTMLElement) || element.hidden) return false;
    const rect = element.getBoundingClientRect();
    const style = win.getComputedStyle(element);
    return rect.width > 0 && rect.height > 0 && style.display !== 'none' && style.visibility !== 'hidden';
  };

  const surface = root instanceof win.HTMLElement
    ? Array.from(root.querySelectorAll('[data-desktop-action-panel]')).find(isVisible)
    : null;
  const desktopKeyboard = win.matchMedia('(min-width:1024px)');

  if (root instanceof win.HTMLElement && surface instanceof win.HTMLElement && desktopKeyboard.matches) {
    setManagedAttribute(root, 'data-closed-hero-keyboard-scope', '');
    setManagedAttribute(surface, 'tabindex', '0');
    setManagedAttribute(surface, 'data-keyboard-event-surface', '');
    setManagedAttribute(surface, 'aria-describedby', 'keyboard-prototype-instructions');
    setManagedAttribute(surface, 'aria-keyshortcuts', 'ArrowLeft ArrowRight ArrowUp ArrowDown Enter L K S C P');

    const primary = surface.querySelector('.desktop-prototype__primary-action:not(.is-disabled)');
    const currentLike = surface.querySelector('[data-feedback-action="like"]');
    if (currentLike instanceof win.HTMLElement) {
      // The shared feedback renderer reads identity from its scope. Keep this
      // one-page prototype self-contained while making consent replay visible.
      setManagedAttribute(surface, 'data-event-id', currentLike.dataset.eventId || '');
      setManagedAttribute(surface, 'data-event-title', currentLike.dataset.eventTitle || '');
    }
    const shortcutFactsKey = 'ke_keyboard_shortcut_daily_v2';
    const shortcutFactVersion = 2;
    const shortcutRetentionDays = 35;
    const shortcutMasteryDays = 3;
    const shortcutMasteryWindowDays = 14;
    const shortcutActionAllowlist = new Set([
      'primary_cta', 'hero_previous', 'hero_next', 'gallery_open', 'gallery_close_down', 'gallery_recommendation_enter',
      'gallery_recommendation_space', 'related_jump', 'related_boundary_focus', 'related_reentry',
      'return_event_top', 'card_open', 'card_previous', 'card_next', 'card_row_up', 'card_row_down',
      'like_toggle', 'calendar_add', 'copy_event', 'copy_description', 'copy_event_poster', 'service_copy_poster', 'service_copy_text',
    ]);
    const kaliningradDay = (date = new Date()) => {
      const parts = Object.fromEntries(new Intl.DateTimeFormat('en-GB', {
        timeZone: 'Europe/Kaliningrad', year: 'numeric', month: '2-digit', day: '2-digit',
      }).formatToParts(date).filter((part) => part.type !== 'literal').map((part) => [part.type, part.value]));
      return `${parts.year}-${parts.month}-${parts.day}`;
    };
    const dayNumber = (value) => Math.floor(Date.parse(`${value}T00:00:00Z`) / 86400000);
    const readShortcutFacts = () => {
      try {
        const value = JSON.parse(localStorage.getItem(shortcutFactsKey) || 'null');
        const currentDay = dayNumber(kaliningradDay());
        const days = {};
        const reportedDays = {};
        if (value?.v === shortcutFactVersion && value.days && typeof value.days === 'object') {
          Object.entries(value.days).forEach(([day, actions]) => {
            const age = currentDay - dayNumber(day);
            if (age >= 0 && age < shortcutRetentionDays && Array.isArray(actions)) {
              const accepted = [...new Set(actions.filter((action) => shortcutActionAllowlist.has(action)))];
              if (accepted.length) days[day] = accepted;
            }
          });
          Object.entries(value.reported_days || {}).forEach(([day, actions]) => {
            const age = currentDay - dayNumber(day);
            if (age >= 0 && age < shortcutRetentionDays && Array.isArray(actions)) {
              const accepted = [...new Set(actions.filter((action) => shortcutActionAllowlist.has(action)))];
              if (accepted.length) reportedDays[day] = accepted;
            }
          });
        }
        return { v: shortcutFactVersion, days, reported_days: reportedDays };
      } catch {
        return { v: shortcutFactVersion, days: {}, reported_days: {} };
      }
    };
    const shortcutIsMastered = (action) => {
      const today = dayNumber(kaliningradDay());
      return Object.entries(readShortcutFacts().days).filter(([day, actions]) => {
        const age = today - dayNumber(day);
        return age >= 0 && age < shortcutMasteryWindowDays && actions.includes(action);
      }).length >= shortcutMasteryDays;
    };
    const updateShortcutHintVisibility = () => {
      const badges = doc.querySelectorAll('[data-keyboard-shortcut-badge]');
      badges.forEach((badge) => {
        const action = badge instanceof win.HTMLElement ? badge.dataset.shortcutAction : '';
        badge.hidden = Boolean(action && shortcutIsMastered(action));
      });
      const coreActions = ['primary_cta', 'calendar_add', 'copy_event', 'like_toggle'];
      setManagedAttribute(surface, 'data-keyboard-shortcut-hints', coreActions.every(shortcutIsMastered) ? 'hidden' : 'visible');
    };
    const recordShortcutUse = (action) => {
      if (!shortcutActionAllowlist.has(action)) return false;
      const facts = readShortcutFacts();
      const day = kaliningradDay();
      const actions = new Set(facts.days[day] || []);
      const newUse = !actions.has(action);
      if (newUse) {
        actions.add(action);
        facts.days[day] = [...actions].sort();
      }
      let consented = false;
      try {
        const profile = JSON.parse(localStorage.getItem('ke_personalization_profile') || 'null');
        consented = profile?.consent_ok === true && /^[0-9a-f]{8}-[0-9a-f-]{27}$/iu.test(String(profile?.anon_id || ''));
      } catch {}
      const reported = new Set(facts.reported_days[day] || []);
      const shouldReport = consented && !reported.has(action);
      if (shouldReport) {
        reported.add(action);
        facts.reported_days[day] = [...reported].sort();
      }
      if (!newUse && !shouldReport) return false;
      try {
        localStorage.setItem(shortcutFactsKey, JSON.stringify(facts));
      } catch {}
      if (newUse) updateShortcutHintVisibility();
      if (shouldReport) {
        win.dispatchEvent(new win.CustomEvent('kenigevents:shortcut-daily-fact', {
          detail: { schema_version: 'keyboard-shortcut-daily-v1', action_code: action },
        }));
      }
      return newUse || shouldReport;
    };
    const shortcutTargets = [
      [primary, 'Enter', 'Основное действие — клавиша Enter', 'primary_cta'],
      [surface.querySelector('[data-calendar-action]:not(.desktop-prototype__primary-action)'), 'K', 'Добавить в календарь — клавиша K', 'calendar_add'],
      [surface.querySelector('[data-native-share]'), 'S', 'Скопировать название и ссылку события — клавиша S', 'copy_event'],
      [surface.querySelector('[data-feedback-action="like"]'), 'L', 'Поставить лайк — клавиша L', 'like_toggle'],
    ];
    shortcutTargets.forEach(([target, key, title, action]) => {
      if (!(target instanceof win.HTMLElement) || target.querySelector('[data-keyboard-shortcut-badge]')) return;
      setManagedAttribute(target, 'data-keyboard-shortcut-target', '');
      setManagedAttribute(target, 'aria-keyshortcuts', key);
      setManagedAttribute(target, 'title', title);
      const badge = doc.createElement('span');
      badge.className = 'keyboard-shortcut-badge';
      badge.dataset.keyboardShortcutBadge = '';
      badge.dataset.shortcutAction = action;
      badge.setAttribute('aria-hidden', 'true');
      badge.textContent = key;
      target.append(badge);
    });
    updateShortcutHintVisibility();

    let footerShare = null;
    let footerImageAction = null;
    let footerTextAction = null;
    let footerStatusObserver = null;
    const enhanceFooterShare = () => {
      footerShare = doc.querySelector('[data-service-share-root][data-service-share-surface="footer"]');
      footerImageAction = footerShare?.querySelector('[data-service-share-button][data-service-share-intent="image"]') || null;
      footerTextAction = footerShare?.querySelector('[data-service-share-button][data-service-share-intent="text"]') || null;
      [
        [footerImageAction, 'P', 'Скопировать карточку сервиса — клавиша P'],
        [footerTextAction, 'S', 'Скопировать текст и ссылку сервиса — клавиша S'],
      ].forEach(([target, key, title]) => {
        if (!(target instanceof win.HTMLElement)) return;
        setManagedAttribute(target, 'title', title);
        setManagedAttribute(target, 'aria-keyshortcuts', key);
        const label = target.querySelector('span:last-of-type');
        if (!(label instanceof win.HTMLElement) || target.querySelector('[data-service-shortcut-badge]')) return;
        const keycap = doc.createElement('kbd');
        keycap.className = 'service-share-inline-shortcut';
        keycap.dataset.serviceShortcutBadge = '';
        keycap.setAttribute('aria-hidden', 'true');
        keycap.textContent = key;
        label.after(keycap);
      });
      const footerStatus = footerShare?.querySelector('[data-service-share-status]');
      if (footerStatus instanceof win.HTMLElement && !footerStatusObserver) {
        footerStatusObserver = observe(() => {
          const message = footerStatus.textContent?.trim();
          if (!message) return;
          const failed = /не удалось|ошиб/iu.test(message);
          showActionToast(message, failed ? 'error' : 'success');
          const pending = footerShare instanceof win.HTMLElement ? footerShare.dataset.keyboardShortcutPending : '';
          if (pending && !failed) recordShortcutUse(pending);
          if (footerShare instanceof win.HTMLElement) delete footerShare.dataset.keyboardShortcutPending;
        });
        footerStatusObserver.observe(footerStatus, { childList:true, characterData:true, subtree:true });
      }
    };
    if (doc.readyState === 'loading') listen(doc, 'DOMContentLoaded', enhanceFooterShare, { once:true });
    else enhanceFooterShare();

    const graphRoot = root.closest('#main') || doc;
    const relatedSection = root.querySelector('[data-related-start]');
    const continuationSection = graphRoot.querySelector('[data-personal-feed-section][data-listing-context="event-detail"]');
    // The personalization renderer may replace the slot element itself, so
    // never retain it as the identity of the continuation zone.
    const continuationSlot = () => continuationSection?.querySelector('[data-personal-feed-slot]');
    if (continuationSection instanceof win.HTMLElement) {
      setManagedAttribute(continuationSection, 'tabindex', '-1');
      setManagedAttribute(continuationSection, 'aria-describedby', 'keyboard-prototype-instructions');
    }
    const zoneCards = (zone) => {
      const scope = zone === 'related' ? relatedSection : continuationSlot();
      return scope instanceof win.HTMLElement ? Array.from(scope.querySelectorAll('[data-event-card]')).filter(isVisible) : [];
    };
    const cards = () => [...zoneCards('related'), ...zoneCards('continuation')];
    const rowsFor = (allCards) => visualCardRows(allCards);
    const visualZoneCards = (zone) => rowsFor(zoneCards(zone)).flatMap((row) => row.cards.map(({ card }) => card));
    const visualCards = () => [...visualZoneCards('related'), ...visualZoneCards('continuation')];
    const zoneForCard = (card) => relatedSection?.contains(card) ? 'related' : continuationSlot()?.contains(card) ? 'continuation' : '';
    const managedCardFor = (target) => {
      const card = target instanceof win.Element ? target.closest('[data-event-card]') : null;
      return card instanceof win.HTMLElement && (relatedSection?.contains(card) || continuationSlot()?.contains(card)) ? card : null;
    };
    const gallerySlides = Array.from(root.querySelectorAll('[data-hero-gallery-slide][data-gallery-slide-kind="image"]'));
    const heroImage = root.querySelector('[data-clean-hero-image]');
    const heroOpener = heroImage?.closest('[data-hero-gallery-open]');
    const heroStatus = root.querySelector('[data-closed-hero-status]');
    const description = root.querySelector('.desktop-clean-description');
    const descriptionText = description?.querySelector('.desktop-clean-description__text');
    const currentShare = surface.querySelector('[data-native-share]');
    const currentEventTitle = String(currentShare?.dataset.shareEventTitle || currentShare?.dataset.shareTitle || root.querySelector('h1')?.textContent || '').trim();
    const currentEventUrl = String(currentShare?.dataset.shareUrl || '').trim();
    const canonicalPosterUrl = heroImage instanceof win.HTMLImageElement ? String(heroImage.currentSrc || heroImage.src || '').trim() : '';

    const normalizeManagedCardLinks = (card) => {
      if (!(card instanceof win.HTMLElement) || zoneForCard(card) !== 'continuation') return;
      const legacyBase = String(doc.body.dataset.siteBasePath || '').replace(/\/$/u, '');
      // Root builds already contain canonical relative links. Reassigning the
      // DOM `href` property would serialize them as absolute URLs while
      // `data-card-href` stayed relative, creating needless divergence between
      // otherwise identical canonical cards. Only legacy prefixed candidates
      // need the migration below.
      if (!legacyBase) return;
      const normalize = (value) => {
        if (!value) return value;
        try {
          const url = new URL(value, win.location.href);
          const legacyEvents = `${legacyBase}/sobytiya/`;
          if (url.origin === win.location.origin && url.pathname.startsWith(legacyEvents)) {
            url.pathname = url.pathname.slice(legacyBase.length);
          }
          return url.href;
        } catch { return value; }
      };
      card.dataset.cardHref = normalize(card.dataset.cardHref || '');
      card.querySelectorAll('a[href]').forEach((anchor) => { anchor.href = normalize(anchor.href); });
      card.querySelectorAll('[data-native-share]').forEach((button) => {
        if (button instanceof win.HTMLElement) button.dataset.shareUrl = normalize(button.dataset.shareUrl || '');
      });
    };

    const enhanceManagedCard = (card) => {
      if (!(card instanceof win.HTMLElement)) return;
      normalizeManagedCardLinks(card);
      const calendar = card.querySelector('[data-calendar-action]:not([hidden])');
      if (!(calendar instanceof win.HTMLElement)) return;
      setManagedAttribute(calendar, 'aria-keyshortcuts', 'K');
      setManagedAttribute(calendar, 'title', 'Добавить выбранное событие в календарь — клавиша K');
      if (calendar.querySelector('[data-related-calendar-shortcut]')) return;
      const keycap = doc.createElement('kbd');
      keycap.className = 'related-calendar-shortcut';
      keycap.dataset.relatedCalendarShortcut = '';
      keycap.dataset.keyboardShortcutBadge = '';
      keycap.dataset.shortcutAction = 'calendar_add';
      keycap.setAttribute('aria-hidden', 'true');
      keycap.textContent = 'K';
      calendar.append(keycap);
    };
    const enhanceManagedCards = () => {
      cards().forEach(enhanceManagedCard);
      updateShortcutHintVisibility();
    };
    enhanceManagedCards();
    if (relatedSection instanceof win.HTMLElement) {
      observe(enhanceManagedCards).observe(relatedSection, { childList: true, subtree: true });
    }
    if (continuationSection instanceof win.HTMLElement) {
      observe(enhanceManagedCards).observe(continuationSection, { childList: true, subtree: true });
    }

    let descriptionCopyButton = null;
    let posterCopyButton = null;
    if (description instanceof win.HTMLElement && descriptionText instanceof win.HTMLElement) {
      const actions = doc.createElement('div');
      actions.className = 'keyboard-description-actions';
      actions.dataset.eventContentCopyActions = '';
      actions.setAttribute('aria-label', 'Скопировать материалы события');
      actions.innerHTML = `
        <button type="button" data-copy-event-description aria-keyshortcuts="C" title="Скопировать полное описание — клавиша C">
          <span>Скопировать описание</span><kbd aria-hidden="true">C</kbd>
        </button>
        <button type="button" data-copy-event-poster aria-keyshortcuts="P" title="Скопировать афишу события — клавиша P">
          <span>Скопировать афишу</span><kbd aria-hidden="true">P</kbd>
        </button>`;
      description.append(actions);
      descriptionCopyButton = actions.querySelector('[data-copy-event-description]');
      posterCopyButton = actions.querySelector('[data-copy-event-poster]');
    }

    let statusTimer;
    let gallerySpaceArm = null;
    let downGesture = null;
    let downGestureTimer = null;
    let suppressGalleryUntilArrowUpRelease = false;
    let suppressPageDownUntilArrowDownRelease = false;
    const pressedArrows = new Set();
    let logicalOwner = { kind: 'surface', node: surface, eventId: '', zone: '', index: 0 };
    let bodyRecoveryArmed = false;
    // A freshly loaded event page deliberately keeps BODY focus.  The first
    // physical hero arrow is still a meaningful keyboard intent, but this
    // implicit entry is revoked as soon as another page surface (header,
    // footer, editor, dialog, browser blur) claims the interaction context.
    let coldBodyHeroEntryArmed = true;
    let overlayReturn = null;
    let overlaySequence = 0;
    let pendingContinuationEntry = false;
    let relatedRestoreFrame = 0;
    let continuationRestoreFrame = 0;
    let posterPngPromise = null;
    const clearGalleryHandoff = () => {
      try { win.sessionStorage.removeItem(GALLERY_HANDOFF_STORAGE_KEY); } catch {}
    };
    const armGalleryHandoff = (anchor) => {
      if (!(anchor instanceof win.HTMLAnchorElement)) return false;
      const destination = keyboardGalleryDestination(anchor.href, win.location.href);
      if (!destination) return false;
      try {
        win.sessionStorage.setItem(GALLERY_HANDOFF_STORAGE_KEY, JSON.stringify({
          version:1,
          destination,
          expires_at:Date.now() + GALLERY_HANDOFF_TTL_MS,
        }));
        return true;
      } catch {
        return false;
      }
    };
    const readGalleryHandoff = () => {
      try {
        const value = JSON.parse(win.sessionStorage.getItem(GALLERY_HANDOFF_STORAGE_KEY) || 'null');
        clearGalleryHandoff();
        const current = keyboardGalleryDestination(win.location.href, win.location.href);
        return value?.version === 1
          && value.destination === current
          && Number(value.expires_at || 0) >= Date.now()
          ? Number(value.expires_at)
          : 0;
      } catch {
        clearGalleryHandoff();
        return 0;
      }
    };
    let galleryDestinationHandoffExpiresAt = readGalleryHandoff();
    const setStatus = (message) => {
      if (!(status instanceof win.HTMLElement)) return;
      win.clearTimeout(statusTimer);
      status.textContent = '';
      statusTimer = scheduleTimeout(() => { status.textContent = message; }, 20);
    };

    function showActionToast(message, state = 'success') {
      const detail = { message, type: state, dedupeKey: 'keyboard-action', announce: false };
      if (win.KenigEventsToast?.show) win.KenigEventsToast.show(detail);
      else win.dispatchEvent(new win.CustomEvent('kenigevents:toast', { detail }));
    }

    const fallbackCopyText = (value) => {
      const previousFocus = doc.activeElement;
      const field = doc.createElement('textarea');
      field.value = value;
      field.setAttribute('readonly', '');
      field.style.position = 'fixed';
      field.style.opacity = '0';
      doc.body.appendChild(field);
      field.select();
      const copied = doc.execCommand('copy');
      field.remove();
      if (previousFocus instanceof win.HTMLElement && doc.contains(previousFocus)) previousFocus.focus({ preventScroll:true });
      if (!copied) throw new Error('clipboard unavailable');
    };

    const copyText = async (value) => {
      if (win.navigator.clipboard?.writeText) {
        try {
          await win.navigator.clipboard.writeText(value);
          return;
        } catch {}
      }
      fallbackCopyText(value);
    };

    const descriptionPayload = () => {
      if (!(description instanceof win.HTMLElement) || !(descriptionText instanceof win.HTMLElement) || !currentEventTitle || !currentEventUrl) return '';
      const lead = String(description.querySelector('.desktop-clean-description__lead')?.textContent || '').trim();
      const body = String(descriptionText.innerText || descriptionText.textContent || '').trim();
      const parts = [currentEventTitle];
      if (lead && lead !== currentEventTitle) parts.push(lead);
      if (body && !parts.includes(body)) parts.push(body);
      parts.push(currentEventUrl);
      return parts.join('\n\n');
    };

    const copyDescription = async ({ keyboard = false } = {}) => {
      if (!(descriptionCopyButton instanceof win.HTMLButtonElement) || descriptionCopyButton.disabled) return;
      const payload = descriptionPayload();
      if (!payload) {
        showActionToast('Не удалось подготовить описание', 'error');
        setStatus('Не удалось подготовить описание события для копирования.');
        return;
      }
      descriptionCopyButton.disabled = true;
      descriptionCopyButton.setAttribute('aria-busy', 'true');
      try {
        await copyText(payload);
        descriptionCopyButton.dataset.copyState = 'success';
        showActionToast('Описание и ссылка на событие скопированы');
        setStatus('Полное описание и ссылка на событие скопированы.');
        if (keyboard) recordShortcutUse('copy_description');
      } catch {
        descriptionCopyButton.dataset.copyState = 'error';
        showActionToast('Не удалось скопировать описание', 'error');
        setStatus('Не удалось скопировать описание события.');
      } finally {
        descriptionCopyButton.disabled = false;
        descriptionCopyButton.setAttribute('aria-busy', 'false');
        scheduleTimeout(() => { delete descriptionCopyButton.dataset.copyState; }, 2600);
      }
    };

    const preparePosterPng = async () => {
      if (!canonicalPosterUrl) throw new Error('poster unavailable');
      const response = await win.fetch(canonicalPosterUrl, { mode: 'cors', credentials: 'omit' });
      if (!response.ok) throw new Error('poster fetch failed');
      const source = await response.blob();
      if (source.type === 'image/png') return source;
      const bitmap = await win.createImageBitmap(source);
      try {
        const canvas = doc.createElement('canvas');
        canvas.width = bitmap.width;
        canvas.height = bitmap.height;
        const context = canvas.getContext('2d');
        if (!context) throw new Error('canvas unavailable');
        context.drawImage(bitmap, 0, 0);
        return await new Promise((resolve, reject) => canvas.toBlob((blob) => blob ? resolve(blob) : reject(new Error('PNG conversion failed')), 'image/png'));
      } finally {
        bitmap.close?.();
      }
    };
    if (canonicalPosterUrl) {
      const startPosterPreparation = () => {
        if (posterPngPromise) return;
        const preparation = preparePosterPng();
        posterPngPromise = preparation;
        preparation.catch(() => {
          if (posterPngPromise === preparation) posterPngPromise = null;
        });
      };
      if ('requestIdleCallback' in win) win.requestIdleCallback(startPosterPreparation, { timeout: 1500 });
      else scheduleTimeout(startPosterPreparation, 250);
    }

    const copyPoster = async ({ keyboard = false } = {}) => {
      if (!(posterCopyButton instanceof win.HTMLButtonElement) || posterCopyButton.disabled) return;
      const clipboardItem = win.ClipboardItem;
      if (!win.navigator.clipboard?.write || typeof clipboardItem !== 'function' || (clipboardItem.supports && !clipboardItem.supports('image/png'))) {
        showActionToast('Браузер не поддерживает копирование афиши', 'error');
        setStatus('Копирование изображения недоступно в этом браузере.');
        return;
      }
      posterCopyButton.disabled = true;
      posterCopyButton.setAttribute('aria-busy', 'true');
      try {
        posterPngPromise ||= preparePosterPng();
        await win.navigator.clipboard.write([new clipboardItem({ 'image/png': posterPngPromise })]);
        posterCopyButton.dataset.copyState = 'success';
        showActionToast('Афиша скопирована в буфер');
        setStatus('Афиша события скопирована в буфер обмена.');
        if (keyboard) recordShortcutUse('copy_event_poster');
      } catch {
        posterCopyButton.dataset.copyState = 'error';
        showActionToast('Не удалось скопировать афишу', 'error');
        setStatus('Не удалось скопировать афишу события.');
      } finally {
        posterCopyButton.disabled = false;
        posterCopyButton.setAttribute('aria-busy', 'false');
        scheduleTimeout(() => { delete posterCopyButton.dataset.copyState; }, 2600);
      }
    };

    listen(descriptionCopyButton, 'click', () => { void copyDescription(); });
    listen(posterCopyButton, 'click', () => { void copyPoster(); });

    const copyEventLink = async (button) => {
      if (!(button instanceof win.HTMLElement) || button.dataset.keyboardCopyPending === 'true') return;
      const title = String(button.dataset.shareEventTitle || button.dataset.shareTitle || '').trim();
      const url = String(button.dataset.shareUrl || '').trim();
      if (!title || !url) {
        button.dataset.keyboardCopyState = 'error';
        showActionToast('Не удалось определить ссылку события', 'error');
        setStatus('Не удалось определить ссылку выбранного события.');
        scheduleTimeout(() => { delete button.dataset.keyboardCopyState; }, 2600);
        return;
      }
      button.dataset.keyboardCopyPending = 'true';
      button.setAttribute('aria-busy', 'true');
      try {
        await copyText(`${title}\n${url}`);
        button.dataset.keyboardCopyState = 'success';
        showActionToast('Название и ссылка на событие скопированы');
        setStatus('Название и ссылка на событие скопированы.');
        if (button.dataset.keyboardShortcutPending === 'copy_event') recordShortcutUse('copy_event');
      } catch {
        button.dataset.keyboardCopyState = 'error';
        showActionToast('Не удалось скопировать ссылку', 'error');
        setStatus('Не удалось скопировать ссылку на событие.');
      } finally {
        delete button.dataset.keyboardCopyPending;
        delete button.dataset.keyboardShortcutPending;
        button.setAttribute('aria-busy', 'false');
        scheduleTimeout(() => { delete button.dataset.keyboardCopyState; }, 2600);
      }
    };

    // On desktop the event share control is deliberately a deterministic copy
    // action. It never enters Web Share or image generation in this prototype.
    listen(doc, 'click', (event) => {
      if (!activated) return;
      const button = event.target instanceof win.Element ? event.target.closest('[data-native-share]') : null;
      const managedCard = managedCardFor(button);
      if (!(button instanceof win.HTMLElement) || (!root.contains(button) && !(managedCard instanceof win.HTMLElement))) return;
      event.preventDefault();
      event.stopImmediatePropagation();
      void copyEventLink(button);
    }, true);

    const snapshotOwner = (node = doc.activeElement) => {
      if (node === surface || (node instanceof win.Node && surface.contains(node))) {
        return { kind: 'surface', node: surface, eventId: '', zone: '', index: 0 };
      }
      const card = managedCardFor(node);
      if (card instanceof win.HTMLElement) {
        const zone = zoneForCard(card);
        return {
          kind: 'card', node: card, eventId: card.dataset.eventId || '', zone,
          index: Math.max(0, visualZoneCards(zone).indexOf(card)),
        };
      }
      return logicalOwner;
    };

    const captureLogicalOwner = (node = doc.activeElement) => {
      logicalOwner = snapshotOwner(node);
      return logicalOwner;
    };

    const resolveLogicalOwner = (snapshot = logicalOwner) => {
      if (snapshot?.node instanceof win.HTMLElement && doc.contains(snapshot.node)) return snapshot.node;
      if (snapshot?.kind === 'card' && snapshot.eventId) {
        const sameEvent = cards().find((card) => card.dataset.eventId === snapshot.eventId);
        if (sameEvent) return sameEvent;
      }
      if (snapshot?.kind === 'card' && snapshot.zone) {
        const candidates = visualZoneCards(snapshot.zone);
        if (candidates.length) return candidates[Math.min(snapshot.index || 0, candidates.length - 1)];
      }
      return surface;
    };

    const restoreLogicalOwner = (snapshot, token, allowedFocus = []) => {
      // Closing an overlay starts a new keyboard gesture context. Never carry
      // held/repeat or double-Down state from the obscured page into it.
      pressedArrows.clear();
      suppressGalleryUntilArrowUpRelease = false;
      resetDownGesture();
      scheduleFrame(() => {
        if (token !== overlaySequence || activeHeroGallery() || doc.querySelector('[data-personalization-consent].is-visible')) return;
        const active = doc.activeElement;
        const mayRestore = active === doc.body || active === doc.documentElement
          || allowedFocus.some((element) => element instanceof win.HTMLElement && (active === element || element.contains(active)))
          || (snapshot?.node instanceof win.HTMLElement && (active === snapshot.node || snapshot.node.contains(active)));
        if (!mayRestore) return;
        const target = resolveLogicalOwner(snapshot);
        if (target === surface) surface.focus({ preventScroll: true });
        else if (target instanceof win.HTMLElement) target.focus({ preventScroll: true });
        captureLogicalOwner(target);
        resetDownGesture();
      });
    };

    const focusSurface = () => {
      surface.focus({ preventScroll: true });
      captureLogicalOwner(surface);
      surface.scrollIntoView({ block: 'center', inline: 'nearest', behavior: 'instant' });
    };

    const focusSurfaceTop = () => {
      surface.focus({ preventScroll: true });
      captureLogicalOwner(surface);
      win.scrollTo({ top: 0, left: 0, behavior: 'instant' });
      scheduleFrame(() => win.scrollTo({ top: 0, left: 0, behavior: 'instant' }));
    };

    const focusCard = (card) => {
      if (!(card instanceof win.HTMLElement)) return;
      // Personal continuation cards are injected asynchronously. Mutation
      // observers run after the current task, while ArrowDown can move focus
      // during that same task. Enhance the destination synchronously so the
      // newly focused owner exposes its one reserved K hint immediately.
      enhanceManagedCard(card);
      updateShortcutHintVisibility();
      card.focus({ preventScroll: true });
      captureLogicalOwner(card);
      card.scrollIntoView({ block: 'center', inline: 'nearest', behavior: 'instant' });
    };

    const focusFirstCard = () => {
      const first = visualZoneCards('related')[0];
      if (first) focusCard(first);
    };

    const enterContinuation = () => {
      const first = visualZoneCards('continuation')[0];
      if (first) {
        pendingContinuationEntry = false;
        focusCard(first);
        return true;
      }
      if (!(continuationSection instanceof win.HTMLElement)) return false;
      pendingContinuationEntry = true;
      continuationSection.focus({ preventScroll: true });
      continuationSection.scrollIntoView({ block: 'start', inline: 'nearest', behavior: 'instant' });
      setStatus('Подбираем ещё события. Первая карточка получит фокус автоматически.');
      return true;
    };

    if (continuationSection instanceof win.HTMLElement) {
      observe(() => {
        enhanceManagedCards();
        if (pendingContinuationEntry && visualZoneCards('continuation')[0]) {
          pendingContinuationEntry = false;
          focusCard(visualZoneCards('continuation')[0]);
          recordShortcutUse('card_row_down');
          return;
        }
        const snapshot = logicalOwner;
        if (snapshot.kind !== 'card' || snapshot.zone !== 'continuation' || doc.contains(snapshot.node)) return;
        win.cancelAnimationFrame(continuationRestoreFrame);
        // Personal-feed renderers can replace a batch more than once. Restore
        // after all MutationObservers for this frame, resolving by event id
        // against the final DOM rather than focusing an intermediate clone.
        continuationRestoreFrame = scheduleFrame(() => {
          continuationRestoreFrame = 0;
          const current = logicalOwner.kind === 'card' && logicalOwner.zone === 'continuation' ? logicalOwner : snapshot;
          const active = doc.activeElement;
          if (active !== doc.body && active !== doc.documentElement
            && !(continuationSection instanceof win.HTMLElement && continuationSection.contains(active))) return;
          const target = resolveLogicalOwner(current);
          if (target instanceof win.HTMLElement) focusCard(target);
        });
      }).observe(continuationSection, { childList: true, subtree: true });
    }

    if (relatedSection instanceof win.HTMLElement) {
      observe(() => {
        const snapshot = logicalOwner;
        if (snapshot.kind !== 'card' || snapshot.zone !== 'related') return;
        win.cancelAnimationFrame(relatedRestoreFrame);
        // The shared discovery controller may reorder canonical nodes with
        // append(). Browsers then move focus to BODY even while the exact card
        // remains connected, so restore its logical ownership after the batch.
        relatedRestoreFrame = scheduleFrame(() => {
          relatedRestoreFrame = 0;
          const active = doc.activeElement;
          if (active !== doc.body && active !== doc.documentElement) return;
          const target = resolveLogicalOwner(snapshot);
          if (target instanceof win.HTMLElement) focusCard(target);
        });
      }).observe(relatedSection, { childList: true, subtree: true });
    }

    const relatedBoundaryIsVisible = () => {
      if (!(relatedSection instanceof win.HTMLElement)) return false;
      const rect = relatedSection.getBoundingClientRect();
      return rect.top >= 0 && rect.top <= win.innerHeight - 24;
    };

    const resetDownGesture = () => {
      win.clearTimeout(downGestureTimer);
      downGestureTimer = null;
      downGesture = null;
    };

    const handleSurfaceArrowDown = (event) => {
      if (relatedBoundaryIsVisible()) {
        event.preventDefault();
        resetDownGesture();
        focusFirstCard();
        recordShortcutUse('related_boundary_focus');
        return;
      }
      if (downGesture?.released && event.timeStamp - downGesture.at <= 430) {
        event.preventDefault();
        resetDownGesture();
        focusFirstCard();
        recordShortcutUse('related_jump');
        return;
      }
      resetDownGesture();
      downGesture = { at: event.timeStamp, released: false };
      downGestureTimer = scheduleTimeout(resetDownGesture, 430);
      // Keep the platform meaning (one ordinary scroll step), but perform the
      // step explicitly. Chromium can otherwise drop the first default scroll
      // while a fullscreen gallery is finishing its close transition.
      event.preventDefault();
      win.scrollBy({ top: Math.max(40, Math.min(72, win.innerHeight * 0.075)), left: 0, behavior: 'instant' });
    };

    const nearestCardInRow = (current, row) => {
      if (!row?.cards?.length) return null;
      const rect = current.getBoundingClientRect();
      const centerX = rect.left + rect.width / 2;
      return row.cards.reduce((best, entry) => Math.abs(entry.centerX - centerX) < Math.abs(best.centerX - centerX) ? entry : best).card;
    };

    const verticalNeighbor = (current, direction) => {
      const zone = zoneForCard(current);
      const rows = rowsFor(zoneCards(zone));
      const rowIndex = rows.findIndex((row) => row.cards.some((entry) => entry.card === current));
      if (rowIndex < 0) return null;
      const targetRowIndex = rowIndex + direction;
      if (targetRowIndex < 0) {
        if (zone === 'related') return surface;
        const relatedRows = rowsFor(zoneCards('related'));
        return nearestCardInRow(current, relatedRows.at(-1));
      }
      const targetRow = rows[targetRowIndex];
      if (targetRow) return nearestCardInRow(current, targetRow);
      if (zone === 'related' && direction > 0) {
        const continuationRows = rowsFor(zoneCards('continuation'));
        return nearestCardInRow(current, continuationRows[0]) || continuationSection;
      }
      return null;
    };

    const actionFor = (scope, code) => {
      if (!(scope instanceof win.HTMLElement)) return null;
      if (code === 'KeyL') return scope.querySelector('[data-feedback-action="like"]');
      if (code === 'KeyK') return scope.querySelector('[data-calendar-action]');
      if (code === 'KeyS') return scope.querySelector('[data-native-share]');
      return null;
    };

    const actionLabel = (code) => ({ KeyL: 'Лайк', KeyK: 'Календарь', KeyS: 'Поделиться' })[code] || 'Действие';
    let pendingConsentOwner = null;
    const captureVisibleConsent = () => {
      if (!pendingConsentOwner) return false;
      const consent = doc.querySelector('[data-personalization-consent].is-visible');
      if (!(consent instanceof win.HTMLElement)) return false;
      const pending = pendingConsentOwner;
      pendingConsentOwner = null;
      const token = ++overlaySequence;
      overlayReturn = { token, type: 'consent', owner: pending.owner, overlay: consent, opener: pending.opener };
      observeConsentOverlay(consent);
      pressedArrows.clear();
      resetDownGesture();
      scheduleFrame(() => {
        if (token !== overlaySequence || !consent.classList.contains('is-visible')) return;
        const accept = consent.querySelector('[data-personalization-consent-accept]');
        if (accept instanceof win.HTMLElement) accept.focus({ preventScroll: true });
        else consent.focus({ preventScroll: true });
      });
      return true;
    };
    const runAction = (scope, event) => {
      const action = actionFor(scope, event.code);
      event.preventDefault();
      if (!(action instanceof win.HTMLElement)) {
        setStatus(`${actionLabel(event.code)}: действие недоступно для выбранного события.`);
        return;
      }
      const owner = snapshotOwner(scope);
      logicalOwner = owner;
      const shortcutAction = event.code === 'KeyL' ? 'like_toggle' : event.code === 'KeyK' ? 'calendar_add' : 'copy_event';
      action.dataset.keyboardShortcutPending = shortcutAction;
      // The production feedback controller may await card/feed state before it
      // creates the consent dialog. Arm ownership before delegating to the real
      // control, then capture either synchronously or from the existing body
      // mutation observer; do not implement a parallel consent state.
      if (shortcutAction === 'like_toggle') pendingConsentOwner = { owner, opener: action };
      action.click();
      captureVisibleConsent();
      if (shortcutAction === 'calendar_add' && action.dataset.calendarState === 'added') {
        recordShortcutUse(shortcutAction);
        delete action.dataset.keyboardShortcutPending;
      }
      setStatus(`${actionLabel(event.code)}: команда передана для выбранного события.`);
    };

    const actionCompletionObserver = observe((mutations) => {
      mutations.forEach((mutation) => {
        const action = mutation.target;
        if (!(action instanceof win.HTMLElement)) return;
        const pending = action.dataset.keyboardShortcutPending;
        if (pending === 'calendar_add' && action.dataset.calendarState === 'added') {
          recordShortcutUse(pending);
          delete action.dataset.keyboardShortcutPending;
        } else if (pending === 'like_toggle' && mutation.attributeName === 'aria-pressed') {
          if (pendingConsentOwner?.opener === action) pendingConsentOwner = null;
          recordShortcutUse(pending);
          delete action.dataset.keyboardShortcutPending;
        }
      });
    });
    const observedActions = new WeakSet();
    const observeManagedActions = () => {
      [surface, ...cards()].forEach((scope) => {
        scope.querySelectorAll('[data-calendar-action], [data-feedback-action="like"]').forEach((action) => {
          if (observedActions.has(action)) return;
          observedActions.add(action);
          actionCompletionObserver.observe(action, { attributes: true, attributeFilter: ['data-calendar-state', 'aria-pressed'] });
        });
      });
    };
    observeManagedActions();
    if (continuationSection instanceof win.HTMLElement) {
      observe(observeManagedActions).observe(continuationSection, { childList: true, subtree: true });
    }

    const selectHero = (direction) => {
      if (!(heroImage instanceof win.HTMLImageElement) || gallerySlides.length < 2) return false;
      const current = Number(root.dataset.activeHeroGalleryIndex || '0');
      const next = (current + direction + gallerySlides.length) % gallerySlides.length;
      const slide = gallerySlides[next];
      const image = slide?.querySelector('.hero-gallery__image[data-gallery-src]');
      const src = image?.dataset.gallerySrc;
      if (!(image instanceof win.HTMLImageElement) || !src) return false;
      root.dataset.activeHeroGalleryIndex = String(next);
      heroImage.src = src;
      heroImage.alt = image.alt || heroImage.alt;
      const width = Number(image.getAttribute('width') || 0);
      const height = Number(image.getAttribute('height') || 0);
      if (width) heroImage.width = width;
      if (height) heroImage.height = height;
      heroImage.style.objectPosition = image.dataset.objectPosition || '50% 50%';
      if (heroOpener instanceof win.HTMLElement) heroOpener.dataset.heroGalleryIndex = String(next);
      if (heroStatus instanceof win.HTMLElement) heroStatus.textContent = `Фото ${next + 1} из ${gallerySlides.length}`;
      return true;
    };

    const dialogIsOpen = () => [
      '[data-hero-gallery].is-open:not([hidden])',
      '[data-efficient-viewer]:not([hidden])',
      '[role="dialog"]:not([hidden]):not([data-hero-gallery])',
    ].some((selector) => Array.from(doc.querySelectorAll(selector)).some(isVisible));

    const activeHeroGallery = () => Array.from(doc.querySelectorAll('[data-hero-gallery]:not([hidden])'))
      .find((gallery) => gallery instanceof win.HTMLElement && gallery.classList.contains('is-open') && isVisible(gallery));

    doc.querySelectorAll('[data-hero-gallery]').forEach((gallery) => {
      let wasOpen = gallery instanceof win.HTMLElement && gallery.classList.contains('is-open') && !gallery.hidden;
      observe(() => {
        const open = gallery instanceof win.HTMLElement && gallery.classList.contains('is-open') && !gallery.hidden;
        if (open === wasOpen) return;
        wasOpen = open;
        gallery.dispatchEvent(new win.CustomEvent(open ? 'kenigevents:hero-gallery-opened' : 'kenigevents:hero-gallery-closed', { bubbles: true }));
      }).observe(gallery, { attributes: true, attributeFilter: ['class', 'hidden'] });
    });

    listen(doc, 'kenigevents:hero-gallery-closed', (event) => {
      if (!overlayReturn || overlayReturn.type !== 'gallery') return;
      const session = overlayReturn;
      overlayReturn = null;
      restoreLogicalOwner(session.owner, session.token, [event.target, session.opener]);
    });

    const observedConsentOverlays = new WeakSet();
    const observeConsentOverlay = (consentOverlay) => {
      if (!(consentOverlay instanceof win.HTMLElement) || observedConsentOverlays.has(consentOverlay)) return;
      observedConsentOverlays.add(consentOverlay);
      let consentWasVisible = consentOverlay.classList.contains('is-visible');
      observe(() => {
        const visible = consentOverlay.classList.contains('is-visible');
        if (visible === consentWasVisible) return;
        consentWasVisible = visible;
        if (visible || !overlayReturn || overlayReturn.type !== 'consent') return;
        const session = overlayReturn;
        overlayReturn = null;
        restoreLogicalOwner(session.owner, session.token, [consentOverlay, session.opener]);
      }).observe(consentOverlay, { attributes: true, attributeFilter: ['class', 'hidden'] });
    };
    observeConsentOverlay(doc.querySelector('[data-personalization-consent]'));
    observe((mutations) => {
      mutations.forEach((mutation) => mutation.addedNodes.forEach((node) => {
        if (!(node instanceof win.HTMLElement)) return;
        if (node.matches('[data-personalization-consent]')) observeConsentOverlay(node);
        node.querySelectorAll?.('[data-personalization-consent]').forEach(observeConsentOverlay);
      }));
      captureVisibleConsent();
    }).observe(doc.body, { childList: true, subtree: true });

    const activeGalleryRecommendation = (gallery) => {
      if (!(gallery instanceof win.HTMLElement)) return null;
      const slides = Array.from(gallery.querySelectorAll('[data-hero-gallery-slide]'));
      const slide = slides[Number(gallery.dataset.activeIndex || '0')];
      if (!(slide instanceof win.HTMLElement) || slide.dataset.gallerySlideKind !== 'cta') return null;
      return slide.querySelector('a[href]');
    };

    const footerOwnsShortcut = (target, code) => {
      if (!(footerShare instanceof win.HTMLElement) || !['KeyP', 'KeyS'].includes(code)) return false;
      const managedCard = managedCardFor(target);
      const targetKind = footerShare.contains(target)
        ? 'footer'
        : target === doc.body || target === doc.documentElement
          ? 'body'
          : managedCard instanceof win.HTMLElement
            ? 'managed-card'
            : target === surface || surface.contains(target)
              ? 'event-surface'
              : 'other';
      return footerViewportShortcutOwnership({
        footerRect:footerShare.getBoundingClientRect(),
        targetRect:managedCard?.getBoundingClientRect()
          || (targetKind === 'event-surface' ? surface.getBoundingClientRect() : null),
        targetKind,
        viewportWidth:Math.max(0, Number(win.innerWidth || doc.documentElement.clientWidth || 0)),
        viewportHeight:Math.max(0, Number(win.innerHeight || doc.documentElement.clientHeight || 0)),
      });
    };

    const isEditing = (target) => target instanceof win.HTMLElement && (
      target.isContentEditable || ['INPUT', 'TEXTAREA', 'SELECT'].includes(target.tagName)
    );

    // The shared card renderer also handles Enter on document bubbling. Own
    // managed-card Enter in capture so normalized continuation links and one
    // activation contract win before that generic handler can navigate.
    listen(doc, 'keydown', (event) => {
      if (!desktopKeyboard.matches || event.code !== 'Enter' || event.repeat || event.isComposing
        || event.altKey || event.ctrlKey || event.metaKey || event.shiftKey || isEditing(event.target)
        || dialogIsOpen()) return;
      if (!activateForIntent(event)) return;
      const card = managedCardFor(event.target);
      if (!(card instanceof win.HTMLElement) || event.target !== card) return;
      const link = card.querySelector('[data-card-title][href], [data-card-media-link][href]');
      if (!(link instanceof win.HTMLAnchorElement)) return;
      event.preventDefault();
      event.stopImmediatePropagation();
      recordShortcutUse('card_open');
      link.click();
    }, true);

    listen(doc, 'keydown', (event) => {
      if (!desktopKeyboard.matches || event.defaultPrevented || event.isComposing || event.altKey || event.ctrlKey || event.metaKey || event.shiftKey || isEditing(event.target)) return;
      if (!activateForIntent(event)) return;
      const target = event.target;
      if (!(target instanceof win.HTMLElement)) return;
      if (event.code !== 'ArrowDown') resetDownGesture();

      const gallery = activeHeroGallery();
      if (gallery instanceof win.HTMLElement) {
        const recommendation = activeGalleryRecommendation(gallery);
        if (event.code !== 'Space') gallerySpaceArm = null;
        if (event.code === 'ArrowDown') {
          event.preventDefault();
          event.stopImmediatePropagation();
          if (!event.repeat) {
            const close = gallery.querySelector('[data-hero-gallery-close]');
            if (close instanceof win.HTMLElement) {
              if (!overlayReturn || overlayReturn.type !== 'gallery') {
                const token = ++overlaySequence;
                overlayReturn = { token, type: 'gallery', owner: logicalOwner, opener: heroOpener };
              }
              suppressPageDownUntilArrowDownRelease = true;
              pressedArrows.clear();
              resetDownGesture();
              close.click();
              recordShortcutUse('gallery_close_down');
            }
          }
        } else if (recommendation instanceof win.HTMLAnchorElement && event.code === 'Enter' && !event.repeat) {
          if (target === gallery) {
            event.preventDefault();
            recordShortcutUse('gallery_recommendation_enter');
            armGalleryHandoff(recommendation);
            recommendation.click();
          }
        } else if (recommendation instanceof win.HTMLAnchorElement && event.code === 'Space' && !event.repeat) {
          if (target === gallery || target === recommendation) {
            event.preventDefault();
            gallerySpaceArm = { gallery, recommendation, target };
          }
        }
        return;
      }

      const consent = doc.querySelector('[data-personalization-consent].is-visible');
      if (consent instanceof win.HTMLElement) {
        if (event.code === 'Escape') {
          event.preventDefault();
          consent.querySelector('[data-personalization-consent-dismiss]')?.click();
        } else if (event.code === 'Enter') {
          event.preventDefault();
          consent.querySelector('[data-personalization-consent-accept]')?.click();
        }
        return;
      }
      if (dialogIsOpen()) return;

      if (event.code === 'ArrowDown' && suppressPageDownUntilArrowDownRelease) {
        event.preventDefault();
        return;
      }

      if (['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown'].includes(event.code)) {
        if (pressedArrows.has(event.code) || event.repeat) {
          event.preventDefault();
          return;
        }
        pressedArrows.add(event.code);
      }

      const bodyTarget = target === doc.body || target === doc.documentElement;
      const galleryHandoffArmed = galleryDestinationHandoffExpiresAt >= Date.now();
      if ((event.code === 'ArrowLeft' || event.code === 'ArrowRight')
        && bodyTarget
        && (coldBodyHeroEntryArmed || bodyRecoveryArmed || galleryHandoffArmed)) {
        coldBodyHeroEntryArmed = false;
        galleryDestinationHandoffExpiresAt = 0;
        if (selectHero(event.code === 'ArrowRight' ? 1 : -1)) {
          event.preventDefault();
          surface.focus({ preventScroll:true });
          captureLogicalOwner(surface);
          bodyRecoveryArmed = true;
          recordShortcutUse(event.code === 'ArrowRight' ? 'hero_next' : 'hero_previous');
        } else if (galleryHandoffArmed || bodyRecoveryArmed) {
          // A single-image destination has no slide to advance, but a proven
          // event owner may still re-enter its action surface without scroll.
          event.preventDefault();
          surface.focus({ preventScroll:true });
          captureLogicalOwner(surface);
          bodyRecoveryArmed = true;
        }
        return;
      }

      if (event.code === 'ArrowDown' && (target === doc.body || target === doc.documentElement)) {
        event.preventDefault();
        focusFirstCard();
        recordShortcutUse('related_reentry');
        return;
      }

      // Footer service shortcuts win over event shortcuts whenever the footer
      // owns the current visual/focus scope.
      if (footerOwnsShortcut(target, event.code)) {
        const serviceAction = event.code === 'KeyP' ? footerImageAction : event.code === 'KeyS' ? footerTextAction : null;
        if (serviceAction instanceof win.HTMLElement && !event.repeat) {
          event.preventDefault();
          if (footerShare instanceof win.HTMLElement) footerShare.dataset.keyboardShortcutPending = event.code === 'KeyP' ? 'service_copy_poster' : 'service_copy_text';
          serviceAction.click();
        }
        return;
      }

      // C/P are physical-key shortcuts. KeyboardEvent.code intentionally makes
      // them layout independent (KeyC/KeyP also when `key` is Cyrillic). On a
      // freshly opened event the browser focus naturally belongs to BODY, so
      // re-enter the event action surface without scrolling before copying.
      if (['KeyC', 'KeyP'].includes(event.code)
        && !event.repeat
        && bodyTarget
        && (coldBodyHeroEntryArmed || bodyRecoveryArmed || galleryHandoffArmed)) {
        event.preventDefault();
        surface.focus({ preventScroll:true });
        captureLogicalOwner(surface);
        coldBodyHeroEntryArmed = false;
        galleryDestinationHandoffExpiresAt = 0;
        bodyRecoveryArmed = true;
        if (event.code === 'KeyC') void copyDescription({ keyboard:true });
        else void copyPoster({ keyboard:true });
        return;
      }

      if (['KeyL', 'KeyK', 'KeyS', 'Enter'].includes(event.code)
        && !event.repeat && bodyRecoveryArmed && bodyTarget) {
        const scope = resolveLogicalOwner();
        if (scope instanceof win.HTMLElement && isVisible(scope)) {
          scope.focus({ preventScroll: true });
          captureLogicalOwner(scope);
          if (event.code === 'Enter') {
            event.preventDefault();
            const card = managedCardFor(scope);
            const link = card?.querySelector('[data-card-title][href], [data-card-media-link][href]');
            if (link instanceof win.HTMLAnchorElement) {
              recordShortcutUse('card_open');
              link.click();
            } else if (scope === surface && primary instanceof win.HTMLElement) {
              recordShortcutUse('primary_cta');
              primary.click();
            } else {
              setStatus('Основное действие недоступно для выбранного события.');
            }
          } else {
            runAction(scope, event);
          }
        }
        return;
      }

      if (target === surface || surface.contains(target)) {
        if (event.code === 'ArrowLeft' || event.code === 'ArrowRight') {
          if (selectHero(event.code === 'ArrowRight' ? 1 : -1)) {
            event.preventDefault();
            recordShortcutUse(event.code === 'ArrowRight' ? 'hero_next' : 'hero_previous');
          }
          return;
        }
        if (event.code === 'ArrowDown') {
          handleSurfaceArrowDown(event);
          return;
        }
        if (event.code === 'ArrowUp') {
          if (suppressGalleryUntilArrowUpRelease || event.repeat) {
            event.preventDefault();
          } else if (heroOpener instanceof win.HTMLElement) {
            event.preventDefault();
            const token = ++overlaySequence;
            overlayReturn = { token, type: 'gallery', owner: snapshotOwner(surface), opener: heroOpener };
            resetDownGesture();
            heroOpener.click();
            recordShortcutUse('gallery_open');
          }
          return;
        }
        if (event.code === 'KeyC' && !event.repeat) {
          event.preventDefault();
          void copyDescription({ keyboard: true });
          return;
        }
        if (event.code === 'KeyP' && !event.repeat) {
          event.preventDefault();
          void copyPoster({ keyboard: true });
          return;
        }
        if (event.code === 'Enter' && target === surface) {
          event.preventDefault();
          if (primary instanceof win.HTMLElement) {
            recordShortcutUse('primary_cta');
            primary.click();
          } else {
            setStatus('Основное действие недоступно для выбранного события.');
          }
          return;
        }
        if (['KeyL', 'KeyK', 'KeyS'].includes(event.code) && !event.repeat) runAction(surface, event);
        return;
      }

      if (event.code === 'KeyC' && !event.repeat && descriptionCopyButton instanceof win.HTMLElement && target.closest('[data-event-content-copy-actions]')) {
        event.preventDefault();
        void copyDescription({ keyboard: true });
        return;
      }
      if (event.code === 'KeyP' && !event.repeat && posterCopyButton instanceof win.HTMLElement && target.closest('[data-event-content-copy-actions]')) {
        event.preventDefault();
        void copyPoster({ keyboard: true });
        return;
      }

      if (target === continuationSection) {
        if (event.code === 'ArrowDown') {
          event.preventDefault();
          enterContinuation();
        } else if (event.code === 'ArrowUp') {
          const lastRelated = visualZoneCards('related').at(-1);
          if (lastRelated) {
            event.preventDefault();
            pendingContinuationEntry = false;
            focusCard(lastRelated);
          }
        }
        return;
      }

      const current = managedCardFor(target);
      if (!(current instanceof win.HTMLElement)) return;
      const allCards = visualCards();
      const index = allCards.indexOf(current);

      if (event.code === 'Escape' && target !== current) {
        event.preventDefault();
        focusCard(current);
        return;
      }
      if (['KeyL', 'KeyK', 'KeyS'].includes(event.code) && !event.repeat) {
        runAction(current, event);
        return;
      }
      if (event.code === 'Home' && allCards[0]) {
        event.preventDefault();
        focusCard(allCards[0]);
        return;
      }
      if (event.code === 'End' && allCards.at(-1)) {
        event.preventDefault();
        focusCard(allCards.at(-1));
        return;
      }
      if (event.code === 'ArrowLeft' || event.code === 'ArrowRight') {
        const neighbor = allCards[index + (event.code === 'ArrowLeft' ? -1 : 1)];
        if (neighbor) {
          event.preventDefault();
          focusCard(neighbor);
          recordShortcutUse(event.code === 'ArrowLeft' ? 'card_previous' : 'card_next');
        }
        return;
      }
      if (event.code === 'ArrowUp' || event.code === 'ArrowDown') {
        const neighbor = verticalNeighbor(current, event.code === 'ArrowUp' ? -1 : 1);
        if (neighbor) {
          event.preventDefault();
          if (neighbor === surface) {
            suppressGalleryUntilArrowUpRelease = true;
            focusSurfaceTop();
            recordShortcutUse('return_event_top');
          } else if (neighbor === continuationSection) {
            enterContinuation();
          } else {
            focusCard(neighbor);
            recordShortcutUse(event.code === 'ArrowUp' ? 'card_row_up' : 'card_row_down');
          }
        }
      }
    }, true);

    listen(doc, 'keyup', (event) => {
      if (!desktopKeyboard.matches) return;
      if (['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown'].includes(event.code)) pressedArrows.delete(event.code);
      if (event.code === 'ArrowDown') suppressPageDownUntilArrowDownRelease = false;
      if (event.defaultPrevented || event.isComposing || event.altKey || event.ctrlKey || event.metaKey || event.shiftKey) return;
      if (event.code === 'ArrowDown') {
        if (downGesture) downGesture.released = true;
        return;
      }
      if (event.code === 'ArrowUp') {
        suppressGalleryUntilArrowUpRelease = false;
        return;
      }
      if (event.code !== 'Space' || !gallerySpaceArm) return;
      const arm = gallerySpaceArm;
      gallerySpaceArm = null;
      const gallery = activeHeroGallery();
      const recommendation = activeGalleryRecommendation(gallery);
      if (gallery !== arm.gallery || recommendation !== arm.recommendation || event.target !== arm.target || doc.activeElement !== arm.target) return;
      event.preventDefault();
      recordShortcutUse('gallery_recommendation_space');
      armGalleryHandoff(recommendation);
      recommendation.click();
    });

    listen(doc, 'focusin', () => {
      if (gallerySpaceArm && doc.activeElement !== gallerySpaceArm.target) gallerySpaceArm = null;
      if (downGesture && doc.activeElement !== surface) resetDownGesture();
      const active = doc.activeElement;
      if (galleryDestinationHandoffExpiresAt && active !== doc.body && active !== doc.documentElement) {
        galleryDestinationHandoffExpiresAt = 0;
      }
      if (active === surface || surface.contains(active) || managedCardFor(active)) {
        coldBodyHeroEntryArmed = false;
        captureLogicalOwner(active);
        bodyRecoveryArmed = true;
      } else if (active !== doc.body && active !== doc.documentElement) {
        coldBodyHeroEntryArmed = false;
        if (!doc.querySelector('[data-personalization-consent].is-visible')) pendingConsentOwner = null;
        bodyRecoveryArmed = false;
      }
    });
    listen(doc, 'pointerdown', (event) => {
      galleryDestinationHandoffExpiresAt = 0;
      resetDownGesture();
      pressedArrows.clear();
      suppressPageDownUntilArrowDownRelease = false;
      coldBodyHeroEntryArmed = false;
      const pointerTarget = event.target;
      const managedPointerCard = managedCardFor(pointerTarget);
      const directSurfacePointer = pointerTarget === surface
        || (pointerTarget instanceof win.Node && surface.contains(pointerTarget));
      const rootRect = root.getBoundingClientRect();
      const bodyPointInsideEvent = (pointerTarget === doc.body || pointerTarget === doc.documentElement)
        && event.clientX >= rootRect.left && event.clientX <= rootRect.right
        && event.clientY >= rootRect.top && event.clientY <= rootRect.bottom;
      const inertCurrentEventPointer = pointerTarget instanceof win.Element
        && (root.contains(pointerTarget) || bodyPointInsideEvent)
        && !pointerTarget.closest('a,button,input,textarea,select,[contenteditable="true"],[role="dialog"],[data-personalization-consent],[data-service-share-root]');
      const pointerOwner = directSurfacePointer
        ? surface
        : managedPointerCard instanceof win.HTMLElement
          ? managedPointerCard
          : inertCurrentEventPointer
            ? surface
            : null;
      if (pointerOwner instanceof win.HTMLElement) {
        captureLogicalOwner(pointerOwner);
        bodyRecoveryArmed = true;
      } else {
        pendingConsentOwner = null;
        bodyRecoveryArmed = false;
      }
    }, { passive: true });
    listen(win, 'blur', () => {
      resetDownGesture();
      pressedArrows.clear();
      suppressPageDownUntilArrowDownRelease = false;
      coldBodyHeroEntryArmed = false;
      bodyRecoveryArmed = false;
      pendingConsentOwner = null;
    });
    listen(doc, 'visibilitychange', () => {
      if (!doc.hidden) return;
      resetDownGesture();
      pressedArrows.clear();
      suppressPageDownUntilArrowDownRelease = false;
      coldBodyHeroEntryArmed = false;
      bodyRecoveryArmed = false;
      pendingConsentOwner = null;
    });

    listen(startButton, 'click', () => { activated = true; focusFirstCard(); });

  }

  const destroy = () => {
    if (destroyed) return;
    destroyed = true;
    abortController.abort();
    observers.forEach((observer) => observer.disconnect());
    observers.clear();
    timeouts.forEach((id) => win.clearTimeout(id));
    timeouts.clear();
    frames.forEach((id) => win.cancelAnimationFrame(id));
    frames.clear();
    doc.querySelectorAll('[data-keyboard-shortcut-badge],[data-related-calendar-shortcut],[data-service-shortcut-badge],[data-event-content-copy-actions]').forEach((node) => node.remove());
    doc.querySelectorAll('[data-keyboard-shortcut-pending]').forEach((node) => node.removeAttribute('data-keyboard-shortcut-pending'));
    restoreManagedAttributes();
  };
  return { destroy, get active() { return activated; } };
}
