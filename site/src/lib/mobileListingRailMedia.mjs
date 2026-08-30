const PROTECTED_MEDIA_ROLES = new Set([
  'event_identity_poster',
  'program_or_schedule',
  'attendee_information',
]);

function sourceImages(event) {
  return (event?.image_assets || []).filter((asset) => (
    asset
    && Number(asset.width) > 0
    && Number(asset.height) > 0
    && typeof asset.src === 'string'
    && asset.src.length > 0
  ));
}

function normalizedObjectPosition(value) {
  const raw = String(value || '').trim();
  return /^[a-z0-9.%\s-]+$/iu.test(raw) ? raw : '';
}

function objectPositionForAsset(asset, fallback = '50% 50%') {
  const recommended = normalizedObjectPosition(asset?.recommended_object_position);
  if (recommended) return recommended;
  const x = Number(asset?.focal_point?.x);
  const y = Number(asset?.focal_point?.y);
  if (Number.isFinite(x) && Number.isFinite(y) && x >= 0 && x <= 1 && y >= 0 && y <= 1) {
    return `${Math.round(x * 100)}% ${Math.round(y * 100)}%`;
  }
  return normalizedObjectPosition(fallback) || '50% 50%';
}

/**
 * Resolve only the physical treatment used by the accepted 112px mobile rail.
 *
 * Any explicitly classified and crop-safe visual-only image uses the accepted
 * donor's compact 140×112 landscape window. This is evaluated per asset, so a
 * portrait source or a second gallery image can never make an otherwise safe
 * photo fall back to authored geometry and reintroduce letterboxing.
 * OCR, unknown, contradictory and document-like media remain fail-closed.
 */
export function resolveMobileListingRailMedia(event, selected) {
  const asset = selected?.asset || null;
  const images = sourceImages(event);
  const explicitlyVisual = Boolean(
    asset
    && asset.image_text_mode === 'visual_only'
    && asset.media_semantic_status === 'classified'
    && asset.media_role === 'event_photo'
    && Number(asset.media_role_confidence || 0) >= 0.9
    && asset.safe_crop === true
    && Boolean(asset.focal_point)
    && !PROTECTED_MEDIA_ROLES.has(asset.media_role || '')
  );
  // A contradictory event-level OCR/unknown marker must never be overridden by
  // a permissive asset record. For an event whose aggregate marker and the
  // selected asset both say visual-only, the accepted rail window is always
  // 5:4 — regardless of how many other gallery assets the event has.
  const safeVisual = explicitlyVisual
    && event?.image_text_mode === 'visual_only';

  if (safeVisual) {
    return {
      fit: 'cover',
      ratio: 5 / 4,
      width: 140,
      reason: images.length === 1
        ? 'single_safe_visual_landscape_5x4'
        : 'safe_visual_landscape_5x4',
    };
  }

  // A listing selector may carry a crop-window ratio even when the event-level
  // protection gate later rejects that crop (the source-reviewed Teremok photo
  // is the regression canary). In the contain branch the physical source ratio
  // is authoritative; otherwise the wrapper itself would manufacture side
  // fields around a protected image.
  const sourceRatio = asset && Number(asset.width) > 0 && Number(asset.height) > 0
    ? Number(asset.width) / Number(asset.height)
    : Number(selected?.ratio || 1);
  const ratio = Math.max(62 / 112, Math.min(199 / 112, Number(sourceRatio || selected?.ratio || 1)));
  return {
    fit: 'contain',
    ratio,
    width: Math.max(62, Math.min(199, Math.round(112 * ratio))),
    reason: explicitlyVisual ? 'safe_visual_authored_geometry' : 'protected_natural_geometry',
  };
}

/**
 * Return a bounded, source-ordered set of real gallery cells for the mobile
 * listing rail. The listing-selected asset remains first, while every later
 * item is evaluated independently by the same OCR/document protection gate.
 */
export function resolveMobileListingRailMediaItems(event, selected, maxItems = 4) {
  const limit = Math.max(1, Math.min(6, Number(maxItems) || 4));
  const candidates = [];
  const seen = new Set();
  const add = (asset, fallbackSelected = null) => {
    const src = String(asset?.src || fallbackSelected?.src || '').trim();
    const key = String(asset?.asset_key || src).trim();
    if (!src || !key || seen.has(key)) return;
    seen.add(key);
    const width = Number(asset?.width || fallbackSelected?.asset?.width || 0);
    const height = Number(asset?.height || fallbackSelected?.asset?.height || 0);
    const itemSelected = fallbackSelected || {
      asset,
      src,
      ratio: width > 0 && height > 0 ? width / height : 1,
      mode: 'natural',
      adaptiveCrop: false,
      objectPosition: objectPositionForAsset(asset),
    };
    const media = resolveMobileListingRailMedia(event, itemSelected);
    candidates.push({
      asset: asset || itemSelected.asset || null,
      src,
      width: width || null,
      height: height || null,
      imageTextMode: asset?.image_text_mode || event?.image_text_mode || 'unknown',
      objectPosition: objectPositionForAsset(asset, itemSelected.objectPosition),
      ...media,
    });
  };

  if (selected?.src || selected?.asset?.src) add(selected.asset, selected);
  for (const asset of sourceImages(event)) add(asset);
  return candidates.slice(0, limit);
}
