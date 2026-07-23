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

/**
 * Resolve only the physical treatment used by the accepted 112px mobile rail.
 *
 * A lone, explicitly classified visual-only portrait is the one case allowed
 * to become a 140×112 landscape window. OCR, unknown and document-like media
 * remain fail-closed at authored geometry. Multi-image rows retain their
 * selected presentation, so a narrow 4:5 portrait is still allowed.
 */
export function resolveMobileListingRailMedia(event, selected) {
  const asset = selected?.asset || null;
  const images = sourceImages(event);
  const sourceRatio = asset && Number(asset.height) > 0
    ? Number(asset.width) / Number(asset.height)
    : Number(selected?.ratio || 1);
  const explicitlyVisual = Boolean(
    asset
    && images.length === 1
    && event?.image_text_mode === 'visual_only'
    && asset.image_text_mode === 'visual_only'
    && asset.media_semantic_status === 'classified'
    && !PROTECTED_MEDIA_ROLES.has(asset.media_role || '')
  );
  const loneTallVisual = explicitlyVisual && sourceRatio > 0 && sourceRatio < 1;

  if (loneTallVisual) {
    return {
      fit: 'cover',
      ratio: 5 / 4,
      width: 140,
      reason: 'single_tall_visual_landscape',
    };
  }

  const ratio = Math.max(62 / 112, Math.min(199 / 112, Number(selected?.ratio || sourceRatio || 1)));
  return {
    fit: selected?.mode === 'visual-crop' && selected?.adaptiveCrop ? 'cover' : 'contain',
    ratio,
    width: Math.max(62, Math.min(199, Math.round(112 * ratio))),
    reason: explicitlyVisual ? 'authored_or_multi_visual_geometry' : 'protected_natural_geometry',
  };
}
