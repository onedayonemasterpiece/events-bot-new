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
 * Any lone, explicitly classified and crop-safe visual-only image uses the
 * accepted donor's compact 140×112 landscape window. A source-reviewed
 * visual-only portrait selected from a mixed inventory may use a 90×112
 * vertical 4:5 window when the retained area stays at or above 80%.
 * OCR, unknown, unreviewed and document-like media remain fail-closed.
 */
export function resolveMobileListingRailMedia(event, selected) {
  const asset = selected?.asset || null;
  const images = sourceImages(event);
  const sourceRatio = asset && Number(asset.height) > 0
    ? Number(asset.width) / Number(asset.height)
    : Number(selected?.ratio || 1);
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
  // a permissive asset record. The exact 5:4 donor is reserved for one image
  // whose event and asset both say visual-only.
  const loneVisual = explicitlyVisual
    && event?.image_text_mode === 'visual_only'
    && images.length === 1;

  if (loneVisual) {
    return {
      fit: 'cover',
      ratio: 5 / 4,
      width: 140,
      reason: 'single_safe_visual_landscape_5x4',
    };
  }

  const portraitTarget = 4 / 5;
  const portraitRetention = sourceRatio > 0
    ? Math.min(sourceRatio / portraitTarget, portraitTarget / sourceRatio)
    : 0;
  if (
    explicitlyVisual
    && asset.listing_crop_evidence === 'source-reviewed'
    && asset.listing_no_ocr_review === true
    && sourceRatio < 1
    && portraitRetention >= 0.8
  ) {
    return {
      fit: 'cover',
      ratio: portraitTarget,
      width: 90,
      reason: 'reviewed_multi_visual_portrait_4x5',
    };
  }

  const ratio = Math.max(62 / 112, Math.min(199 / 112, Number(selected?.ratio || sourceRatio || 1)));
  return {
    fit: selected?.mode === 'visual-crop' && selected?.adaptiveCrop ? 'cover' : 'contain',
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
