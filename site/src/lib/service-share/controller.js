export const SERVICE_SHARE_CANONICAL_URL = 'https://kenigevents.ru/';
export const SERVICE_SHARE_DEFAULT_MANIFEST_PATH = '/service-share/current/manifest.json';
export const SERVICE_SHARE_DESKTOP_MODES = Object.freeze(['d0', 'd1', 'd2']);

const DEFAULT_SHARE_TEXT = 'KenigEvents — события Калининграда и области\nНайдите своё событие быстрее';
const ASSET_TIMEOUT_MS = 4500;
const LATENCY_BANDS = Object.freeze([
  [250, 'lt_250ms'],
  [1000, '250_999ms'],
  [3000, '1_3s'],
]);
const manifestCache = new Map();
const assetCache = new Map();

function normalizedMode(value) {
  return SERVICE_SHARE_DESKTOP_MODES.includes(String(value || '').toLowerCase())
    ? String(value).toLowerCase()
    : 'd0';
}

function safeSurface(value) {
  return ['footer', 'menu', 'lab'].includes(String(value || '')) ? String(value) : 'footer';
}

function safeText(value, fallback = '') {
  return typeof value === 'string' ? value.trim() : fallback;
}

function assetMime(asset) {
  return safeText(asset?.mime_type || asset?.mime);
}

function assetSize(asset) {
  const value = Number(asset?.byte_size ?? asset?.bytes);
  return Number.isSafeInteger(value) && value > 0 ? value : null;
}

function assetSha(asset) {
  const value = safeText(asset?.sha256).toLowerCase();
  if (!value) return null;
  if (!/^[a-f0-9]{64}$/u.test(value)) throw new Error('asset_sha_invalid');
  return value;
}

function resolveUrl(value, baseUrl) {
  const url = new URL(value, baseUrl);
  const base = new URL(baseUrl);
  const localHttp = url.protocol === 'http:' && ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname);
  if (url.protocol !== 'https:' && !localHttp) throw new Error('asset_url_insecure');
  const approvedOrigins = new Set([base.origin, 'https://kenigevents.ru', 'https://static.kenigevents.ru']);
  if (!approvedOrigins.has(url.origin)) throw new Error('asset_origin_unapproved');
  url.hash = '';
  return url.href;
}

export function validateServiceShareManifest(raw, manifestUrl = SERVICE_SHARE_DEFAULT_MANIFEST_PATH) {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) throw new Error('manifest_invalid');
  const canonicalUrl = safeText(raw.canonical_url || raw.canonicalUrl);
  if (canonicalUrl !== SERVICE_SHARE_CANONICAL_URL) throw new Error('canonical_mismatch');

  const shareText = safeText(raw.share_text || raw.shareText || raw.copy?.share_text, DEFAULT_SHARE_TEXT);
  if (!shareText || shareText.length > 360 || /https?:\/\//iu.test(shareText)) throw new Error('copy_invalid');

  const assets = raw.assets || {};
  const webp = assets.webp || raw.webp;
  const png = assets.png || raw.png;
  if (!webp || !png) throw new Error('assets_missing');
  if (assetMime(webp) !== 'image/webp' || assetMime(png) !== 'image/png') throw new Error('asset_mime_invalid');

  const baseUrl = new URL(manifestUrl, globalThis.location?.href || SERVICE_SHARE_CANONICAL_URL).href;
  const normalizeAsset = (asset, mimeType) => ({
    url: resolveUrl(safeText(asset.url), baseUrl),
    mime_type: mimeType,
    byte_size: assetSize(asset),
    sha256: assetSha(asset),
  });
  const assetVersion = safeText(raw.asset_version || raw.assetVersion || raw.version);
  const visualPayloadHash = safeText(raw.visual_payload_hash || raw.visualPayloadHash).toLowerCase();
  if (!assetVersion || !visualPayloadHash) throw new Error('manifest_version_missing');
  if (!/^[a-f0-9]{64}$/u.test(visualPayloadHash)) throw new Error('visual_payload_hash_invalid');

  return Object.freeze({
    schema_version: safeText(raw.schema_version || raw.schemaVersion, 'service-share-card-manifest-v1'),
    asset_version: assetVersion,
    visual_payload_hash: visualPayloadHash,
    canonical_url: SERVICE_SHARE_CANONICAL_URL,
    share_text: shareText,
    assets: Object.freeze({
      webp: Object.freeze(normalizeAsset(webp, 'image/webp')),
      png: Object.freeze(normalizeAsset(png, 'image/png')),
    }),
  });
}

export function serviceSharePlainText(manifest) {
  return `${manifest?.share_text || DEFAULT_SHARE_TEXT}\n${SERVICE_SHARE_CANONICAL_URL}`;
}

export function escapeServiceShareHtml(value) {
  return String(value ?? '')
    .replace(/&/gu, '&amp;')
    .replace(/</gu, '&lt;')
    .replace(/>/gu, '&gt;')
    .replace(/"/gu, '&quot;')
    .replace(/'/gu, '&#39;');
}

export function serviceShareHtml(manifest) {
  const copy = escapeServiceShareHtml(manifest?.share_text || DEFAULT_SHARE_TEXT).replace(/\n/gu, '<br>');
  const imageUrl = escapeServiceShareHtml(manifest?.assets?.png?.url || '');
  return `<article><img src="${imageUrl}" alt=""><h2>KenigEvents</h2><p>${copy}</p><p><a href="${SERVICE_SHARE_CANONICAL_URL}">kenigevents.ru</a></p></article>`;
}

export function createImageClipboardItem(pngPromise, ClipboardItemCtor = globalThis.ClipboardItem) {
  if (typeof ClipboardItemCtor !== 'function') throw new Error('clipboard_item_unavailable');
  const png = Promise.resolve(pngPromise).then((blob) => {
    if (!(blob instanceof Blob) || blob.type !== 'image/png') throw new Error('png_invalid');
    return blob;
  });
  // This action intentionally has one representation. Adding text/plain or
  // text/html makes messenger paste behavior ambiguous and mixes user intents.
  return new ClipboardItemCtor({ 'image/png': png });
}

// Compatibility export for consumers of the preview-only D1/D2 helper. The
// legacy mode argument is ignored; the clipboard contract is image-only.
export function createRichClipboardItem(_manifest, _mode, pngPromise, ClipboardItemCtor = globalThis.ClipboardItem) {
  return createImageClipboardItem(pngPromise, ClipboardItemCtor);
}

export function coarseServiceSharePlatform(navigatorLike = globalThis.navigator) {
  const platform = safeText(navigatorLike?.userAgentData?.platform || navigatorLike?.platform).toLowerCase();
  if (/android/u.test(platform)) return 'android';
  if (/iphone|ipad|ipod|ios/u.test(platform)) return 'ios';
  if (/win/u.test(platform)) return 'windows';
  if (/mac/u.test(platform)) return 'macos';
  if (/linux/u.test(platform)) return 'linux';
  return 'other';
}

export function latencyBand(elapsedMs) {
  const elapsed = Math.max(0, Number(elapsedMs) || 0);
  for (const [limit, label] of LATENCY_BANDS) if (elapsed < limit) return label;
  return 'gte_3s';
}

function errorReason(error) {
  const name = safeText(error?.name).toLowerCase();
  const message = safeText(error?.message).toLowerCase();
  if (name === 'aborterror') return 'cancelled';
  if (name === 'notallowederror') return 'denied';
  if (name === 'dataerror') return 'data_error';
  if (/timeout/u.test(message)) return 'timeout';
  if (/cors|fetch|network|asset|png/u.test(message)) return 'asset_error';
  if (/unsupported|unavailable/u.test(message)) return 'unsupported';
  return 'api_error';
}

async function sha256Hex(blob) {
  if (!globalThis.crypto?.subtle) return null;
  const digest = await globalThis.crypto.subtle.digest('SHA-256', await blob.arrayBuffer());
  return [...new Uint8Array(digest)].map((value) => value.toString(16).padStart(2, '0')).join('');
}

async function assertPngSignature(blob) {
  const bytes = new Uint8Array(await blob.slice(0, 8).arrayBuffer());
  const expected = [137, 80, 78, 71, 13, 10, 26, 10];
  if (bytes.length !== expected.length || expected.some((value, index) => bytes[index] !== value)) throw new Error('png_signature_invalid');
}

async function verifiedAsset(asset) {
  const response = await fetch(asset.url, { mode: 'cors', credentials: 'omit', cache: 'force-cache' });
  if (!response.ok || response.type === 'opaque') throw new Error('asset_fetch_failed');
  const blob = await response.blob();
  if (blob.type !== asset.mime_type) throw new Error('asset_mime_mismatch');
  if (asset.byte_size && blob.size !== asset.byte_size) throw new Error('asset_size_mismatch');
  if (asset.mime_type === 'image/png') await assertPngSignature(blob);
  if (asset.sha256) {
    const actualSha = await sha256Hex(blob);
    if (actualSha && actualSha !== asset.sha256) throw new Error('asset_sha_mismatch');
  }
  return blob;
}

function withTimeout(promise, timeoutMs = ASSET_TIMEOUT_MS) {
  let timer = 0;
  const timeout = new Promise((_, reject) => {
    timer = globalThis.setTimeout(() => reject(new Error('asset_timeout')), timeoutMs);
  });
  return Promise.race([promise, timeout]).finally(() => globalThis.clearTimeout(timer));
}

function loadAsset(asset) {
  if (!assetCache.has(asset.url)) assetCache.set(asset.url, withTimeout(verifiedAsset(asset)));
  return assetCache.get(asset.url);
}

export function loadServiceShareManifest(manifestUrl) {
  const url = new URL(manifestUrl, globalThis.location?.href || SERVICE_SHARE_CANONICAL_URL).href;
  if (!manifestCache.has(url)) {
    manifestCache.set(url, withTimeout(
      fetch(url, { mode: 'cors', credentials: 'omit', cache: 'no-cache' })
        .then((response) => {
          if (!response.ok || response.type === 'opaque') throw new Error('manifest_fetch_failed');
          return response.json();
        })
        .then((raw) => validateServiceShareManifest(raw, url)),
    ));
  }
  return manifestCache.get(url);
}

function defaultManifest() {
  return Object.freeze({
    schema_version: 'service-share-card-manifest-fallback-v1',
    asset_version: 'link-only',
    visual_payload_hash: 'link-only',
    canonical_url: SERVICE_SHARE_CANONICAL_URL,
    share_text: DEFAULT_SHARE_TEXT,
    assets: Object.freeze({ webp: null, png: null }),
  });
}

function capabilities() {
  return {
    secure_context: Boolean(globalThis.isSecureContext),
    document_focused: Boolean(document.hasFocus?.()),
    system_share: typeof navigator.share === 'function',
    can_share: typeof navigator.canShare === 'function',
    clipboard_write: typeof navigator.clipboard?.write === 'function',
    clipboard_write_text: typeof navigator.clipboard?.writeText === 'function',
    clipboard_item: typeof globalThis.ClipboardItem === 'function',
    clipboard_item_supports: typeof globalThis.ClipboardItem?.supports === 'function',
  };
}

function telemetry(root, eventKind, detail = {}, startedAt = performance.now()) {
  const payload = Object.freeze({
    event_kind: eventKind,
    surface: safeSurface(root.dataset.serviceShareSurface),
    transport: detail.transport === 'system_share' ? 'system_share' : 'clipboard',
    mode: ['mobile_file', 'mobile_text', 'desktop_image', 'desktop_text', 'd0', 'd1', 'd2'].includes(detail.mode) ? detail.mode : 'desktop_text',
    platform: coarseServiceSharePlatform(),
    capabilities: capabilities(),
    result: safeText(detail.result, 'unknown').slice(0, 40),
    reason: safeText(detail.reason, 'none').slice(0, 40),
    asset_version: safeText(detail.assetVersion, 'unavailable').slice(0, 80),
    latency_band: latencyBand(performance.now() - startedAt),
  });
  globalThis.dispatchEvent(new CustomEvent('service-share-telemetry', { detail: payload }));
  return payload;
}

function status(root, message) {
  const live = root.querySelector('[data-service-share-status]');
  if (live) live.textContent = message;
}

function setActionState(button, state = '') {
  if (!button) return;
  if (!state) {
    delete button.dataset.serviceShareState;
    return;
  }
  button.dataset.serviceShareState = state;
}

function actionStatus(root, button, message, state = '') {
  status(root, message);
  setActionState(button, state);
}

function setButtonBusy(button, busy) {
  if (!button) return;
  button.disabled = busy;
  button.setAttribute('aria-busy', busy ? 'true' : 'false');
}

function showFallback(root) {
  const fallback = root.querySelector('[data-service-share-fallback]');
  if (!fallback) return;
  fallback.hidden = false;
  fallback.focus({ preventScroll: true });
}

async function copyText(root, manifest, startedAt, options = {}) {
  const mode = options.mode || 'desktop_text';
  const attemptedReason = options.attemptedReason || 'direct';
  const button = options.button;
  telemetry(root, 'service_copy_attempted', { transport: 'clipboard', mode, result: 'attempted', reason: attemptedReason, assetVersion: manifest.asset_version }, startedAt);
  try {
    if (typeof navigator.clipboard?.writeText !== 'function') throw new Error('clipboard_unavailable');
    await navigator.clipboard.writeText(serviceSharePlainText(manifest));
    actionStatus(root, button, options.successMessage || 'Текст и ссылка скопированы', 'success');
    telemetry(root, 'service_link_copied', { transport: 'clipboard', mode, result: 'api_resolved', reason: attemptedReason, assetVersion: manifest.asset_version }, startedAt);
    telemetry(root, 'service_copy_result', { transport: 'clipboard', mode, result: 'api_resolved', reason: attemptedReason, assetVersion: manifest.asset_version }, startedAt);
    return true;
  } catch (error) {
    showFallback(root);
    actionStatus(root, button, options.errorMessage || 'Не удалось скопировать ссылку', 'error');
    telemetry(root, 'service_copy_result', { transport: 'clipboard', mode, result: 'fallback_link', reason: errorReason(error), assetVersion: manifest.asset_version }, startedAt);
    return false;
  }
}

async function imageCopy(root, manifest, pngPromise, startedAt, button) {
  const mode = 'desktop_image';
  telemetry(root, 'service_copy_attempted', { transport: 'clipboard', mode, result: 'attempted', reason: 'direct', assetVersion: manifest.asset_version }, startedAt);
  try {
    if (!globalThis.isSecureContext) throw new Error('secure_context_unavailable');
    if (!document.hasFocus?.()) throw new DOMException('Document is not focused', 'NotAllowedError');
    if (typeof navigator.clipboard?.write !== 'function' || typeof globalThis.ClipboardItem !== 'function') throw new Error('clipboard_unavailable');
    const item = createImageClipboardItem(pngPromise, globalThis.ClipboardItem);
    await navigator.clipboard.write([item]);
    actionStatus(root, button, 'Карточка скопирована в буфер', 'success');
    telemetry(root, 'service_copy_result', { transport: 'clipboard', mode, result: 'api_resolved', reason: 'none', assetVersion: manifest.asset_version }, startedAt);
    return true;
  } catch (error) {
    // Do not turn the image intent into text: the adjacent text button remains
    // available and keeps the result of this action deterministic.
    actionStatus(root, button, 'Не удалось скопировать картинку', 'error');
    telemetry(root, 'service_copy_result', { transport: 'clipboard', mode, result: 'api_rejected', reason: errorReason(error), assetVersion: manifest.asset_version }, startedAt);
    return false;
  }
}

async function mobileShare(root, prepared, startedAt, button) {
  const manifest = prepared.manifest;
  if (typeof navigator.share !== 'function') return copyText(root, manifest, startedAt, {
    mode: 'mobile_text',
    attemptedReason: 'share_unavailable',
    successMessage: 'Скопированы текст и ссылка',
    errorMessage: 'Не удалось скопировать. Ссылка доступна ниже',
    button,
  });
  try {
    const webpBlob = prepared.webpBlob;
    if (webpBlob && typeof globalThis.File === 'function' && typeof navigator.canShare === 'function') {
      const file = new File([webpBlob], `kenigevents-service-${manifest.asset_version}.webp`, { type: 'image/webp' });
      const filePayload = { files: [file], text: manifest.share_text, url: SERVICE_SHARE_CANONICAL_URL };
      if (navigator.canShare({ files: [file] })) {
        await navigator.share(filePayload);
        actionStatus(root, button, 'Меню «Поделиться» закрыто', 'success');
        return true;
      }
      telemetry(root, 'share_file_unsupported', { transport: 'system_share', mode: 'mobile_text', result: 'fallback_text', reason: 'can_share_false', assetVersion: manifest.asset_version }, startedAt);
    } else {
      telemetry(root, 'share_file_unsupported', { transport: 'system_share', mode: 'mobile_text', result: 'fallback_text', reason: 'asset_not_ready', assetVersion: manifest.asset_version }, startedAt);
    }
    await navigator.share({ text: manifest.share_text, url: SERVICE_SHARE_CANONICAL_URL });
    actionStatus(root, button, 'Меню «Поделиться» закрыто', 'success');
    return true;
  } catch (error) {
    const reason = errorReason(error);
    if (reason === 'cancelled') {
      status(root, 'Отменено');
      telemetry(root, 'share_cancelled', { transport: 'system_share', mode: prepared.webpBlob ? 'mobile_file' : 'mobile_text', result: 'cancelled', reason, assetVersion: manifest.asset_version }, startedAt);
      return false;
    }
    telemetry(root, 'share_error', { transport: 'system_share', mode: prepared.webpBlob ? 'mobile_file' : 'mobile_text', result: 'fallback_text', reason, assetVersion: manifest.asset_version }, startedAt);
    return copyText(root, manifest, startedAt, {
      mode: 'mobile_text',
      attemptedReason: reason,
      successMessage: 'Скопированы текст и ссылка',
      errorMessage: 'Не удалось скопировать. Ссылка доступна ниже',
      button,
    });
  }
}

function prepare(root) {
  if (root.__serviceSharePrepared) return root.__serviceSharePrepared;
  const manifestUrl = root.dataset.serviceShareManifestUrl || SERVICE_SHARE_DEFAULT_MANIFEST_PATH;
  const prepared = { manifest: defaultManifest(), manifestReady: false, webpBlob: null, pngPromise: null };
  prepared.ready = loadServiceShareManifest(manifestUrl).then((manifest) => {
    prepared.manifest = manifest;
    prepared.manifestReady = true;
    prepared.pngPromise = loadAsset(manifest.assets.png);
    return loadAsset(manifest.assets.webp).then((blob) => {
      prepared.webpBlob = blob;
      root.dataset.serviceShareReady = 'file';
      return prepared;
    }, () => {
      root.dataset.serviceShareReady = 'text';
      return prepared;
    });
  }, () => {
    root.dataset.serviceShareReady = 'link';
    return prepared;
  });
  root.__serviceSharePrepared = prepared;
  return prepared;
}

async function invoke(root, button) {
  const startedAt = performance.now();
  const prepared = prepare(root);
  const isMobile = matchMedia('(max-width: 767px)').matches;
  const intent = isMobile ? 'mobile' : button?.dataset.serviceShareIntent;
  const mode = isMobile ? 'mobile_text' : intent === 'image' ? 'desktop_image' : 'desktop_text';
  status(root, '');
  setActionState(button);
  setButtonBusy(button, true);
  telemetry(root, 'service_share_invoked', {
    transport: isMobile ? 'system_share' : 'clipboard',
    mode,
    result: 'attempted',
    reason: 'none',
    assetVersion: prepared.manifest.asset_version,
  }, startedAt);
  try {
    if (isMobile) {
      // Never wait for a network fetch in the activation path. A verified,
      // prefetched file is used only when it is already available.
      await mobileShare(root, prepared, startedAt, button);
      return;
    }
    if (intent === 'image') {
      if (!prepared.manifestReady || !prepared.pngPromise) {
        actionStatus(root, button, 'Не удалось скопировать картинку', 'error');
        telemetry(root, 'service_copy_result', {
          transport: 'clipboard',
          mode: 'desktop_image',
          result: 'api_rejected',
          reason: prepared.manifestReady ? 'asset_not_ready' : 'manifest_not_ready',
          assetVersion: prepared.manifest.asset_version,
        }, startedAt);
      } else {
        await imageCopy(root, prepared.manifest, prepared.pngPromise, startedAt, button);
      }
    } else {
      await copyText(root, prepared.manifest, startedAt, { mode: 'desktop_text', button });
    }
  } finally {
    setButtonBusy(button, false);
  }
}

export function hydrateServiceShareActions(scope = document) {
  const roots = [...scope.querySelectorAll('[data-service-share-root]')];
  roots.forEach((root) => {
    if (root.dataset.serviceShareHydrated === 'true') return;
    root.dataset.serviceShareHydrated = 'true';
    prepare(root);
    const buttons = [...root.querySelectorAll('[data-service-share-button]')];
    let opened = false;
    const markOpened = () => {
      if (opened) return;
      opened = true;
      telemetry(root, 'service_share_opened', {
        transport: matchMedia('(max-width: 767px)').matches ? 'system_share' : 'clipboard',
        mode: matchMedia('(max-width: 767px)').matches ? 'mobile_text' : 'desktop_text',
        result: 'visible',
        reason: 'none',
        assetVersion: root.__serviceSharePrepared?.manifest?.asset_version,
      });
    };
    buttons.forEach((button) => {
      button.addEventListener('focus', markOpened, { once: true });
      button.addEventListener('pointerenter', markOpened, { once: true });
      button.addEventListener('click', () => void invoke(root, button));
    });
  });
  return roots.length;
}

if (typeof document !== 'undefined') {
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', () => hydrateServiceShareActions(), { once: true });
  else hydrateServiceShareActions();
}
