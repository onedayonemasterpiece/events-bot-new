const FRAME_CONTRACT = 'v1';
const FRAME_STYLE_OWNER = 'media-frame.css';
const GALLERY_SIZES = '(max-width: 820px) 100vw, min(1000px, calc(100vw - 120px))';

const parseArray = (value) => {
  try {
    const parsed = JSON.parse(String(value || '[]'));
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
};

const finitePositive = (value) => {
  const number = Number(value);
  return Number.isFinite(number) && number > 0 ? number : undefined;
};

const safePosition = (value) => {
  const raw = String(value || '').trim();
  return /^[a-z0-9.%\s-]+$/iu.test(raw) ? raw : '50% 50%';
};

const normalizedTextMode = (value) => (
  value === 'visual_only' || value === 'ocr_text' ? value : 'unknown'
);

const normalizedRole = (value) => String(value || 'unknown_document');
const normalizedUrl = (value) => {
  const raw = String(value || '').trim();
  if (!raw) return '';
  try { return new URL(raw, document.baseURI).href; } catch { return raw; }
};

const semanticDecision = (asset, surface) => {
  const textMode = normalizedTextMode(asset?.textMode);
  const role = normalizedRole(asset?.mediaRole);
  const safeDeckCover = surface === 'exhibitions-deck'
    && asset?.treatment === 'photo-cover'
    && asset?.safeCrop === true
    && textMode === 'visual_only'
    && role === 'event_photo';
  const safeMedallionCover = surface === 'exhibitions-medallion'
    && asset?.treatment === 'photo-cover'
    && asset?.safeCrop === true
    && textMode === 'visual_only';
  const safeCover = safeDeckCover || safeMedallionCover;
  const kind = textMode === 'visual_only' ? 'visual' : textMode === 'ocr_text' ? 'document' : 'unknown';
  return {
    role,
    textMode,
    kind,
    fit: safeCover ? 'cover' : 'contain',
    cropPermission: safeCover ? 'allowed' : 'forbidden',
    cropReason: safeCover
      ? String(asset?.cropReason || (surface === 'exhibitions-medallion' ? 'institutional_identity_square' : 'surface-wide-safe-photo'))
      : surface === 'exhibitions-gallery'
        ? 'gallery_full_asset_contain'
        : String(asset?.cropReason || (textMode === 'ocr_text' ? 'document_contain' : 'unknown_contain')),
    objectPosition: safeCover ? safePosition(asset?.objectPosition) : '50% 50%',
  };
};

const publishFrame = (frame, surface, asset, options = {}) => {
  if (!(frame instanceof HTMLElement)) return;
  const decision = semanticDecision(asset, surface);
  const sourceRatio = finitePositive(asset?.sourceRatio)
    || (finitePositive(asset?.width) && finitePositive(asset?.height) ? Number(asset.width) / Number(asset.height) : undefined);
  const frameRatio = surface === 'exhibitions-deck'
    ? finitePositive(asset?.presentationRatio) || sourceRatio
    : sourceRatio;

  frame.setAttribute('data-media-frame', '');
  frame.dataset.mediaFrameContract = FRAME_CONTRACT;
  frame.dataset.mediaFrameStyleOwner = FRAME_STYLE_OWNER;
  frame.dataset.mediaFrameSurface = surface;
  frame.dataset.mediaFrameRole = decision.role;
  frame.dataset.mediaFrameKind = decision.kind;
  frame.dataset.mediaFrameFit = decision.fit;
  frame.dataset.mediaFrameCropPermission = decision.cropPermission;
  frame.dataset.mediaFrameObjectPosition = decision.objectPosition;
  frame.dataset.mediaFrameFocalPosition = decision.objectPosition;
  frame.dataset.mediaFrameCropReason = decision.cropReason;
  frame.dataset.mediaFrameClip = 'frame';
  frame.dataset.mediaFrameRadius = 'surface';
  frame.dataset.mediaFrameFill = 'true';
  frame.dataset.mediaFrameInteractionOwner = options.interactionOwner || 'caller';
  frame.dataset.mediaFrameLoading = options.loading || 'interactive';
  if (frameRatio) frame.dataset.mediaFrameRatio = frameRatio.toFixed(5);
  else delete frame.dataset.mediaFrameRatio;
  if (sourceRatio) frame.dataset.mediaFrameSourceRatio = sourceRatio.toFixed(5);
  else delete frame.dataset.mediaFrameSourceRatio;
  if (finitePositive(asset?.width)) frame.dataset.mediaFrameSourceWidth = String(Number(asset.width));
  else delete frame.dataset.mediaFrameSourceWidth;
  if (finitePositive(asset?.height)) frame.dataset.mediaFrameSourceHeight = String(Number(asset.height));
  else delete frame.dataset.mediaFrameSourceHeight;
  frame.style.setProperty('--media-frame-object-position', decision.objectPosition);
  if (frameRatio) frame.style.setProperty('--media-frame-ratio', String(frameRatio));
  else frame.style.removeProperty('--media-frame-ratio');
  if (sourceRatio) frame.style.setProperty('--media-frame-source-ratio', String(sourceRatio));
  else frame.style.removeProperty('--media-frame-source-ratio');
};

const publishResourceState = (frame, state, fallbackReason) => {
  if (!(frame instanceof HTMLElement)) return;
  const normalized = state === 'loading' ? 'pending'
    : state === 'error' ? 'broken'
      : state === 'depth' ? 'fallback'
        : state === 'loaded' ? 'loaded'
          : 'idle';
  frame.dataset.mediaFrameResourceState = normalized;
  if (normalized === 'broken' || normalized === 'fallback') {
    frame.dataset.mediaFrameKind = 'fallback';
    frame.dataset.mediaFrameFit = 'contain';
    frame.dataset.mediaFrameCropPermission = 'forbidden';
    frame.dataset.mediaFrameCropReason = fallbackReason || (normalized === 'broken' ? 'resource_load_error' : 'depth_fallback');
    frame.dataset.mediaFrameObjectPosition = '50% 50%';
    frame.dataset.mediaFrameFocalPosition = '50% 50%';
    frame.style.setProperty('--media-frame-object-position', '50% 50%');
    frame.setAttribute('data-media-frame-fallback', '');
  } else {
    frame.removeAttribute('data-media-frame-fallback');
  }
};

const clearFailedImageResource = (image) => {
  if (!(image instanceof HTMLImageElement)) return;
  image.closest('picture')?.querySelectorAll('source').forEach((source) => {
    source.removeAttribute('srcset');
    source.removeAttribute('sizes');
  });
  image.removeAttribute('srcset');
  image.removeAttribute('sizes');
  image.removeAttribute('src');
};

const bindImageResourceLifecycle = (frame, image, stateTarget = frame, isCurrent = () => true) => {
  if (!(frame instanceof HTMLElement) || !(image instanceof HTMLImageElement)) return;
  if (image.dataset.mediaFrameResourceLifecycleBound === 'true') return;
  image.dataset.mediaFrameResourceLifecycleBound = 'true';

  const settle = (state) => {
    if (!isCurrent()) return;
    const loaded = state === 'loaded' && image.naturalWidth > 0 && image.naturalHeight > 0;
    if (loaded && frame.dataset.mediaFrameResourceState === 'broken') return;
    if (stateTarget instanceof HTMLElement) stateTarget.dataset.imageState = loaded ? 'loaded' : 'error';
    publishResourceState(frame, loaded ? 'loaded' : 'error', 'resource_load_error');
    if (!loaded) clearFailedImageResource(image);
  };

  image.addEventListener('load', () => settle('loaded'));
  image.addEventListener('error', () => settle('error'));
};

const deckManifestFor = (deck) => parseArray(deck?.dataset?.deckManifest);

const syncDeckFrame = (frame) => {
  if (!(frame instanceof HTMLElement)) return;
  const deck = frame.closest('[data-deck]');
  const manifest = deck instanceof HTMLElement ? deckManifestFor(deck) : [];
  const mediaIndex = Number(frame.dataset.mediaIndex);
  const asset = Number.isInteger(mediaIndex) && mediaIndex >= 0 ? manifest[mediaIndex] : null;
  publishFrame(frame, 'exhibitions-deck', asset || {}, {
    loading: frame.querySelector('[data-deck-image]')?.getAttribute('loading') || 'lazy',
    interactionOwner: 'caller',
  });
  const image = frame.querySelector('[data-deck-image]');
  bindImageResourceLifecycle(frame, image, frame, () => frame.dataset.deckVisual === 'media');
  const visual = frame.dataset.deckVisual;
  const state = visual === 'depth-tail' ? 'depth' : frame.dataset.imageState || 'idle';
  if (state === 'error') clearFailedImageResource(image);
  publishResourceState(frame, state, visual === 'depth-tail' ? 'deck_depth_tail' : undefined);
};

const bindDeckFrames = (root) => {
  root.querySelectorAll('[data-deck-frame]').forEach(syncDeckFrame);
  const observer = new MutationObserver((records) => {
    const frames = new Set();
    records.forEach((record) => {
      const frame = record.target instanceof Element ? record.target.closest('[data-deck-frame]') : null;
      if (frame instanceof HTMLElement) frames.add(frame);
    });
    frames.forEach(syncDeckFrame);
  });
  observer.observe(root, {
    subtree: true,
    attributes: true,
    attributeFilter: ['data-media-index', 'data-image-state', 'data-deck-visual', 'hidden'],
  });
};

const bindMedallions = (root) => {
  root.querySelectorAll('[data-exhibition-medallion]').forEach((seal) => {
    if (!(seal instanceof HTMLElement) || seal.dataset.mediaFrameBridgeBound === 'true') return;
    seal.dataset.mediaFrameBridgeBound = 'true';
    const image = seal.querySelector('[data-media-frame-image]');
    publishFrame(seal, 'exhibitions-medallion', {
      mediaRole: seal.dataset.mediaFrameRole || 'organizer',
      textMode: 'visual_only',
      treatment: 'photo-cover',
      safeCrop: true,
      cropReason: 'institutional_identity_square',
      objectPosition: '50% 50%',
      width: 44,
      height: 44,
      sourceRatio: 1,
      presentationRatio: 1,
    }, { loading: image?.getAttribute('loading') || 'lazy', interactionOwner: 'none' });
    if (image instanceof HTMLImageElement) {
      bindImageResourceLifecycle(seal, image, seal);
      if (image.complete) {
        const loaded = image.naturalWidth > 0 && image.naturalHeight > 0;
        seal.dataset.imageState = loaded ? 'loaded' : 'error';
        if (!loaded) clearFailedImageResource(image);
        publishResourceState(seal, loaded ? 'loaded' : 'error', 'resource_load_error');
      } else {
        seal.dataset.imageState = 'loading';
        publishResourceState(seal, 'loading', 'resource_load_error');
      }
    } else {
      seal.dataset.imageState = 'error';
      publishResourceState(seal, 'error', 'resource_load_error');
    }
  });
};

const galleryState = new WeakMap();

const manifestFromOpener = (opener) => {
  const rich = parseArray(opener?.dataset?.galleryManifest);
  if (rich.length) return rich;
  return parseArray(opener?.dataset?.galleryImages).map((src) => ({
    src: String(src || ''),
    srcset: '',
    width: 1280,
    height: 900,
    sourceRatio: 1280 / 900,
    presentationRatio: 1280 / 900,
    treatment: 'document-natural',
    mediaRole: 'unknown_document',
    textMode: 'unknown',
    safeCrop: false,
    cropReason: 'legacy_gallery_source_contain',
    objectPosition: '50% 50%',
  }));
};

const matchingGalleryAsset = (manifest, image) => {
  if (!(image instanceof HTMLImageElement)) return manifest[0] || null;
  const current = normalizedUrl(image.getAttribute('src') || image.currentSrc);
  return manifest.find((asset) => normalizedUrl(asset?.src) === current) || manifest[0] || null;
};

const applyGalleryResource = (media, image, asset) => {
  if (!(media instanceof HTMLElement) || !(image instanceof HTMLImageElement)) return;
  publishFrame(media, 'exhibitions-gallery', asset || {}, { loading: 'interactive', interactionOwner: 'caller' });
  const resourceState = media.dataset.imageState || 'idle';
  if (resourceState === 'error') {
    clearFailedImageResource(image);
    publishResourceState(media, 'error', 'resource_load_error');
    return;
  }
  const srcset = String(asset?.srcset || '').trim();
  if (srcset) {
    if (image.getAttribute('srcset') !== srcset) image.setAttribute('srcset', srcset);
    image.setAttribute('sizes', GALLERY_SIZES);
  } else {
    image.removeAttribute('srcset');
    image.removeAttribute('sizes');
  }
  if (finitePositive(asset?.width)) image.width = Number(asset.width);
  if (finitePositive(asset?.height)) image.height = Number(asset.height);
  publishResourceState(media, resourceState, 'resource_load_error');
};

const bindGallery = (root) => {
  const dialog = root.querySelector('[data-gallery]');
  const media = root.querySelector('[data-gallery-media]');
  const image = root.querySelector('[data-gallery-image]');
  const skeleton = root.querySelector('[data-gallery-skeleton]');
  const error = root.querySelector('[data-gallery-error]');
  if (!(dialog instanceof HTMLDialogElement) || !(media instanceof HTMLElement) || !(image instanceof HTMLImageElement)) return;
  if (media.dataset.mediaFrameBridgeBound === 'true') return;
  media.dataset.mediaFrameBridgeBound = 'true';
  image.setAttribute('data-media-frame-image', '');
  skeleton?.setAttribute('data-media-frame-placeholder', '');
  error?.setAttribute('data-media-frame-fallback', '');
  galleryState.set(dialog, { manifest: [] });
  bindImageResourceLifecycle(media, image, media);

  const sync = () => {
    const state = galleryState.get(dialog) || { manifest: [] };
    applyGalleryResource(media, image, matchingGalleryAsset(state.manifest, image));
  };
  const prime = (opener) => {
    if (!(opener instanceof HTMLElement)) return;
    const manifest = manifestFromOpener(opener);
    galleryState.set(dialog, { manifest });
    applyGalleryResource(media, image, manifest[0] || null);
  };

  root.addEventListener('click', (event) => {
    const opener = event.target instanceof Element ? event.target.closest('[data-gallery-open]') : null;
    if (opener instanceof HTMLElement) prime(opener);
  }, true);
  root.addEventListener('keydown', (event) => {
    if (!(event instanceof KeyboardEvent) || event.code !== 'KeyG' || event.defaultPrevented) return;
    const row = event.target instanceof Element ? event.target.closest('[data-exhibition-row]') : null;
    const opener = row?.querySelector('[data-gallery-open]');
    if (opener instanceof HTMLElement) prime(opener);
  }, true);

  const mediaObserver = new MutationObserver(sync);
  mediaObserver.observe(media, { attributes: true, attributeFilter: ['data-image-state'] });
  const imageObserver = new MutationObserver(sync);
  imageObserver.observe(image, { attributes: true, attributeFilter: ['src'] });
  sync();
};

export function hydrateExhibitionsMediaFrames(scope = document) {
  scope.querySelectorAll('[data-exhibitions-prototype]').forEach((root) => {
    if (!(root instanceof HTMLElement) || root.dataset.mediaFrameBridgeBound === 'true') return;
    root.dataset.mediaFrameBridgeBound = 'true';
    bindDeckFrames(root);
    bindMedallions(root);
    bindGallery(root);
  });
}
