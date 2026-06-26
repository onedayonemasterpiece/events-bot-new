(function attachKenigEventsPersonalization(global) {
  'use strict';

  const DEFAULT_STORAGE_KEY = 'ke_personalization_profile';
  const DEFAULT_SURFACE = 'event_detail_related';
  const DEFAULT_LAYOUT_MODE = 'module';
  const STATIC_ALGORITHM = 'static_related_v1';
  const LOCAL_ALGORITHM = 'local_related_rerank_v1';
  const HIDDEN_SCORE = -9999;
  const DEFAULT_VISIBLE_LIMIT = 6;

  const WEIGHTS = Object.freeze({
    baseSimilarity: 0.45,
    profileSimilarity: 0.20,
    cityMatch: 0.10,
    dateTime: 0.08,
    price: 0.05,
    freshnessPopularity: 0.05,
    diversity: 0.04,
    exploration: 0.03,
    negativeInterest: 0.9,
    fatigue: 0.35,
    soldOut: 0.25,
  });

  function safeJsonParse(raw, fallback) {
    if (!raw) return fallback;
    try {
      return JSON.parse(raw);
    } catch (_) {
      return fallback;
    }
  }

  function viewportClass(width) {
    if (width < 768) return 'mobile';
    if (width < 1024) return 'tablet';
    return 'desktop';
  }

  function presentationVariant(width) {
    const klass = viewportClass(width);
    return klass === 'desktop' ? 'desktop_related' : klass === 'tablet' ? 'tablet_related' : 'mobile_related';
  }

  function asArray(value) {
    return Array.isArray(value) ? value : [];
  }

  function asNumber(value, fallback) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : fallback;
  }

  function eventId(value) {
    return String(value && value.event_id != null ? value.event_id : value && value.id != null ? value.id : '');
  }

  function eventTags(event) {
    const tags = [];
    for (const key of ['normalized_tags', 'tags', 'audience_tags', 'mood_tags', 'format_tags']) {
      for (const tag of asArray(event && event[key])) tags.push(String(tag));
    }
    return Array.from(new Set(tags));
  }

  function candidateEvent(candidate) {
    return (candidate && candidate.event) || candidate || {};
  }

  function profileHasConsent(profile) {
    return Boolean(profile && profile.consent_ok === true);
  }

  function profileCompatible(profile, options) {
    if (!profileHasConsent(profile)) return false;
    if (options && options.profileVersion && profile.profile_version && profile.profile_version !== options.profileVersion) return false;
    if (options && options.taxonomyVersion && profile.taxonomy_version && profile.taxonomy_version !== options.taxonomyVersion) return false;
    if (options && options.featureSchemaVersion && profile.feature_schema_version && profile.feature_schema_version !== options.featureSchemaVersion) return false;
    return true;
  }

  function tagAffinity(tags, map) {
    let score = 0;
    const source = map || {};
    for (const tag of tags) score += asNumber(source[tag], 0);
    return score;
  }

  function hasAny(tags, map) {
    const source = map || {};
    return tags.some((tag) => asNumber(source[tag], 0) > 0);
  }

  function sameCityScore(event, currentEvent, profile) {
    const eventCity = String(event.city || '').trim();
    const currentCity = String(currentEvent && currentEvent.city || '').trim();
    if (eventCity && currentCity && eventCity === currentCity) return 1;
    const cityAffinity = profile && profile.city_affinity || {};
    return Math.min(1, Math.max(0, asNumber(cityAffinity[eventCity], 0)));
  }

  function dateTimeScore(event, currentEvent) {
    const when = String(event.time_bucket || event.time || '').toLowerCase();
    const currentWhen = String(currentEvent && (currentEvent.time_bucket || currentEvent.time) || '').toLowerCase();
    if (when && currentWhen && when === currentWhen) return 1;
    if (when.includes('evening') || /18|19|20|21/.test(when)) return 0.5;
    return 0;
  }

  function priceScore(event, currentEvent, profile) {
    const positive = profile && profile.positive_tags || {};
    if (event.is_free && asNumber(positive.free, 0) > 0) return 1;
    if (currentEvent && Boolean(event.is_free) === Boolean(currentEvent.is_free)) return 0.5;
    return 0;
  }

  function freshnessPopularityScore(event) {
    return Math.min(1, Math.max(0, asNumber(event.popularity_score, 0))) || Math.min(1, Math.max(0, asNumber(event.freshness_score, 0)));
  }

  function explicitHide(profile, event) {
    const hidden = asArray(profile && profile.hidden_event_ids).map(String);
    return hidden.includes(eventId(event));
  }

  function shouldHardExclude(candidate, context) {
    const event = candidateEvent(candidate);
    if (!eventId(event)) return true;
    if (context && context.currentEvent && eventId(event) === eventId(context.currentEvent)) return true;
    if (event.status === 'cancelled' || event.cancelled === true) return true;
    if (candidate.is_other_date === true || event.is_other_date === true) return true;
    return false;
  }

  function scoreRelatedCandidate(candidate, profile, context) {
    const event = candidateEvent(candidate);
    if (shouldHardExclude(candidate, context)) return HIDDEN_SCORE;
    if (profileHasConsent(profile) && explicitHide(profile, event)) return HIDDEN_SCORE;

    const tags = eventTags(event);
    const baseSimilarity = Math.max(0, Math.min(1, asNumber(candidate.base_similarity, asNumber(event.base_similarity, 0))));
    let score = WEIGHTS.baseSimilarity * baseSimilarity;

    if (profileCompatible(profile, context || {})) {
      const positive = profile.positive_tags || {};
      const negative = profile.negative_interest_tags || {};
      score += WEIGHTS.profileSimilarity * Math.min(1, Math.max(0, tagAffinity(tags, positive)));
      score += WEIGHTS.cityMatch * sameCityScore(event, context && context.currentEvent, profile);
      score += WEIGHTS.dateTime * dateTimeScore(event, context && context.currentEvent);
      score += WEIGHTS.price * priceScore(event, context && context.currentEvent, profile);
      score += WEIGHTS.freshnessPopularity * freshnessPopularityScore(event);
      score += WEIGHTS.exploration * Math.max(0, Math.min(1, asNumber(candidate.exploration_bonus, 0)));
      score -= WEIGHTS.negativeInterest * Math.min(1, Math.max(0, tagAffinity(tags, negative)));
      if (hasAny(tags, negative)) score -= 0.15;
      const recent = asArray(profile.recent_event_ids).map(String);
      if (recent.includes(eventId(event))) score -= WEIGHTS.fatigue;
    }

    if (event.status === 'sold_out' || event.ticket_status === 'sold_out') score -= WEIGHTS.soldOut;
    return Number(score.toFixed(6));
  }

  function applyDiversity(ranked, limit) {
    const seenTypes = new Map();
    const out = [];
    const delayed = [];
    for (const item of ranked) {
      const type = String(candidateEvent(item.candidate).event_type || candidateEvent(item.candidate).type || 'unknown');
      const count = seenTypes.get(type) || 0;
      if (count >= 3 && out.length < limit) {
        delayed.push(item);
        continue;
      }
      seenTypes.set(type, count + 1);
      out.push(item);
    }
    return out.concat(delayed);
  }

  function rankRelated(candidates, profile, context) {
    const ranked = asArray(candidates)
      .filter((candidate) => !shouldHardExclude(candidate, context))
      .map((candidate, originalIndex) => ({
        candidate,
        originalIndex,
        score: scoreRelatedCandidate(candidate, profile, context),
      }))
      .filter((item) => item.score > HIDDEN_SCORE / 2)
      .sort((left, right) => {
        if (right.score !== left.score) return right.score - left.score;
        const byBase = asNumber(right.candidate.base_similarity, 0) - asNumber(left.candidate.base_similarity, 0);
        if (byBase !== 0) return byBase;
        return left.originalIndex - right.originalIndex;
      });
    return applyDiversity(ranked, asNumber(context && context.visibleLimit, DEFAULT_VISIBLE_LIMIT));
  }

  function readProfile(storage, storageKey) {
    if (!storage) return null;
    return safeJsonParse(storage.getItem(storageKey), null);
  }

  function writeProfile(storage, storageKey, profile) {
    if (!storage) return;
    storage.setItem(storageKey, JSON.stringify(profile));
  }

  function compactShownItem(item, rank) {
    const event = candidateEvent(item.candidate);
    return {
      event_id: asNumber(event.event_id != null ? event.event_id : event.id, eventId(event)),
      rank,
      base_similarity: asNumber(item.candidate.base_similarity, null),
      personal_score: item.score,
      reason_codes: asArray(item.candidate.reason_codes),
    };
  }

  function randomId(prefix) {
    const rnd = Math.random().toString(36).slice(2, 10);
    const now = Date.now().toString(36);
    return `${prefix}_${now}_${rnd}`;
  }

  function createServedListSummary(args) {
    const shown = asArray(args.items).map(compactShownItem);
    return {
      served_list_id: args.servedListId || randomId('served'),
      anon_id: args.anonId || null,
      session_id: args.sessionId || null,
      surface: DEFAULT_SURFACE,
      layout_mode: DEFAULT_LAYOUT_MODE,
      viewport_class: args.viewportClass,
      presentation: args.presentation,
      current_event_id: asNumber(args.currentEvent && (args.currentEvent.event_id != null ? args.currentEvent.event_id : args.currentEvent.id), eventId(args.currentEvent)),
      algorithm_id: args.algorithmId,
      shown,
    };
  }

  function createTelemetryEvent(kind, event, context) {
    return {
      event_kind: kind,
      event_id: event && (event.event_id != null ? event.event_id : event.id) || null,
      event_slug: event && event.slug || null,
      viewport_class: context.viewportClass,
      layout_mode: DEFAULT_LAYOUT_MODE,
      presentation: context.presentation,
      surface: DEFAULT_SURFACE,
      position: context.position == null ? null : context.position,
      page_cursor: context.pageCursor || null,
      algorithm_id: context.algorithmId,
      served_list_id: context.servedListId || null,
      consent_version: context.consentVersion || 'draft-v1',
    };
  }

  function defaultCardRenderer(doc, item, index, controllerContext) {
    const event = candidateEvent(item.candidate);
    const card = doc.createElement('article');
    card.className = 'related-card';
    card.dataset.eventId = eventId(event);
    card.dataset.eventType = event.event_type || event.type || '';
    card.dataset.rank = String(index);

    const title = doc.createElement('h3');
    title.textContent = event.title || 'Событие';
    const meta = doc.createElement('p');
    meta.className = 'related-meta';
    meta.textContent = [event.date_label || event.date, event.city, event.venue_name].filter(Boolean).join(' · ');
    const reasons = doc.createElement('p');
    reasons.className = 'reason-codes';
    reasons.textContent = asArray(item.candidate.reason_codes).slice(0, 3).join(' · ');

    const open = doc.createElement('button');
    open.type = 'button';
    open.className = 'open-related';
    open.textContent = 'Подробнее';
    open.addEventListener('click', () => controllerContext.emit('related_card_click', event, index));

    const ticket = doc.createElement('button');
    ticket.type = 'button';
    ticket.className = 'ticket-click';
    ticket.textContent = 'Билеты';
    ticket.addEventListener('click', () => controllerContext.emit('ticket_click', event, index));

    const hide = doc.createElement('button');
    hide.type = 'button';
    hide.className = 'hide-related';
    hide.textContent = 'Не интересно';
    hide.addEventListener('click', () => controllerContext.hideEvent(event, index));

    card.append(title, meta, reasons, open, ticket, hide);
    return card;
  }

  function createController(options) {
    const opts = options || {};
    const doc = opts.document || global.document;
    const win = opts.window || global.window || global;
    if (!doc) throw new Error('document is required');
    const storage = opts.storage || win.localStorage;
    const storageKey = opts.storageKey || DEFAULT_STORAGE_KEY;
    const currentEvent = opts.currentEvent || {};
    const candidates = asArray(opts.relatedStatic || opts.candidates);
    const container = typeof opts.container === 'string' ? doc.querySelector(opts.container) : opts.container;
    const status = typeof opts.status === 'string' ? doc.querySelector(opts.status) : opts.status;
    const consentButton = typeof opts.consentButton === 'string' ? doc.querySelector(opts.consentButton) : opts.consentButton;
    const resetButton = typeof opts.resetButton === 'string' ? doc.querySelector(opts.resetButton) : opts.resetButton;
    const moreButton = typeof opts.moreButton === 'string' ? doc.querySelector(opts.moreButton) : opts.moreButton;
    const telemetrySink = typeof opts.telemetrySink === 'function' ? opts.telemetrySink : function noop() {};
    const cardRenderer = typeof opts.cardRenderer === 'function' ? opts.cardRenderer : (item, index, ctx) => defaultCardRenderer(doc, item, index, ctx);
    const visibleLimit = asNumber(opts.visibleLimit, DEFAULT_VISIBLE_LIMIT);
    let expanded = false;
    let lastServedListId = null;

    function contextBase(position, algorithmId) {
      const width = asNumber(win.innerWidth, 1024);
      return {
        currentEvent,
        profileVersion: opts.profileVersion || 'anon-profile-v1',
        taxonomyVersion: opts.taxonomyVersion || 'event-taxonomy-v1',
        featureSchemaVersion: opts.featureSchemaVersion || 'event-features-v1',
        visibleLimit,
        viewportClass: viewportClass(width),
        presentation: presentationVariant(width),
        position,
        algorithmId,
        pageCursor: opts.pageCursor || null,
        consentVersion: opts.consentVersion || 'draft-v1',
        servedListId: lastServedListId,
      };
    }

    function emit(kind, event, position) {
      const profile = readProfile(storage, storageKey);
      if (!profileHasConsent(profile)) return;
      const algorithmId = profileCompatible(profile, contextBase(null, LOCAL_ALGORITHM)) ? LOCAL_ALGORITHM : STATIC_ALGORITHM;
      telemetrySink(createTelemetryEvent(kind, event, contextBase(position, algorithmId)));
    }

    function updateProfile(mutator) {
      const profile = readProfile(storage, storageKey) || {};
      const next = mutator(profile);
      writeProfile(storage, storageKey, next);
      return next;
    }

    function hideEvent(event, position) {
      updateProfile((profile) => {
        const hidden = asArray(profile.hidden_event_ids).map(String);
        return Object.assign({}, profile, {
          consent_ok: profile.consent_ok === true,
          hidden_event_ids: Array.from(new Set(hidden.concat([eventId(event)]))),
        });
      });
      emit('hide_event', event, position);
      render();
    }

    function rankedItems() {
      const profile = readProfile(storage, storageKey);
      const compatible = profileCompatible(profile, contextBase(null, LOCAL_ALGORITHM));
      const algorithmId = compatible ? LOCAL_ALGORITHM : STATIC_ALGORITHM;
      return { profile, algorithmId, items: rankRelated(candidates, compatible ? profile : null, contextBase(null, algorithmId)) };
    }

    function render() {
      if (!container) return;
      const width = asNumber(win.innerWidth, 1024);
      const { profile, algorithmId, items } = rankedItems();
      const viewport = viewportClass(width);
      const presentation = presentationVariant(width);
      const visibleItems = expanded ? items : items.slice(0, visibleLimit);
      lastServedListId = randomId('served');

      container.dataset.surface = DEFAULT_SURFACE;
      container.dataset.layoutMode = DEFAULT_LAYOUT_MODE;
      container.dataset.presentation = presentation;
      container.dataset.viewportClass = viewport;
      container.dataset.algorithmId = algorithmId;
      container.className = `related-block ${presentation}`;

      if (status) {
        if (!profileHasConsent(profile)) status.textContent = 'static related fallback';
        else if (!profileCompatible(profile, contextBase(null, LOCAL_ALGORITHM))) status.textContent = 'static fallback: incompatible profile';
        else if (opts.backendAvailable === false) status.textContent = 'local rerank: backend unavailable';
        else status.textContent = 'local personalized related';
      }

      container.replaceChildren();
      for (const [index, item] of visibleItems.entries()) {
        const ctx = { emit, hideEvent };
        container.appendChild(cardRenderer(item, index, ctx));
      }

      if (moreButton) {
        moreButton.hidden = expanded || items.length <= visibleLimit;
        moreButton.textContent = 'Показать ещё';
      }

      if (profileHasConsent(profile)) {
        const summary = createServedListSummary({
          items: visibleItems,
          currentEvent,
          algorithmId,
          viewportClass: viewport,
          presentation,
          anonId: profile.anon_id || null,
          sessionId: profile.session_id || null,
          servedListId: lastServedListId,
        });
        telemetrySink({ event_kind: 'personalization_served_list_summary', summary });
        if (opts.backendAvailable === false) telemetrySink(createTelemetryEvent('recommendation_fallback_used', null, contextBase(null, algorithmId)));
      }
    }

    function acceptConsent(seedProfile) {
      const existing = readProfile(storage, storageKey) || {};
      const profile = Object.assign({
        anon_id: randomId('anon'),
        session_id: randomId('session'),
        profile_version: opts.profileVersion || 'anon-profile-v1',
        taxonomy_version: opts.taxonomyVersion || 'event-taxonomy-v1',
        feature_schema_version: opts.featureSchemaVersion || 'event-features-v1',
        positive_tags: {},
        negative_interest_tags: {},
        city_affinity: {},
        recent_event_ids: [],
        hidden_event_ids: [],
      }, existing, seedProfile || opts.defaultProfile || {}, { consent_ok: true });
      writeProfile(storage, storageKey, profile);
      render();
    }

    function resetPersonalization() {
      if (storage) storage.removeItem(storageKey);
      expanded = false;
      render();
    }

    if (consentButton) consentButton.addEventListener('click', () => acceptConsent(win.__seedProfile || opts.defaultProfile));
    if (resetButton) resetButton.addEventListener('click', resetPersonalization);
    if (moreButton) moreButton.addEventListener('click', () => { expanded = true; render(); });
    if (win.addEventListener) win.addEventListener('resize', render);

    return {
      render,
      acceptConsent,
      resetPersonalization,
      readProfile: () => readProfile(storage, storageKey),
      rankRelated: () => rankedItems().items,
      emit,
      hideEvent,
      createServedListSummary,
    };
  }

  global.KenigEventsPersonalization = {
    DEFAULT_STORAGE_KEY,
    viewportClass,
    presentationVariant,
    eventTags,
    scoreRelatedCandidate,
    rankRelated,
    createServedListSummary,
    createTelemetryEvent,
    createController,
  };
})(typeof window !== 'undefined' ? window : globalThis);
