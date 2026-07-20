import { resolveEventImageCrop } from './imageCrop.mjs';

const DEFAULT_DOCUMENT_RATIO = 4 / 5;
const MIN_VISUAL_RATIO = 1;
const MAX_VISUAL_RATIO = 4 / 3;
const MAX_DOCUMENT_CROP = 0.2;

function finiteRatio(value, fallback) {
  const ratio = Number(value);
  return Number.isFinite(ratio) && ratio > 0 ? ratio : fallback;
}

function displayPayload(item) {
  const candidate = item?.candidate || item || {};
  return candidate?.display || item?.display || candidate;
}

function normalizedObjectPosition(value) {
  const raw = String(value || '').trim();
  return /^[a-z0-9.%\s-]+$/iu.test(raw) ? raw : '';
}

function visualFallbackObjectPosition(item) {
  const candidate = item?.candidate || item || {};
  const display = displayPayload(item);
  const asset = primaryImageAsset(item);
  const explicit = normalizedObjectPosition(
    display?.image_object_position
      || candidate?.image_object_position
      || item?.image_object_position
      || asset?.recommended_object_position
      || asset?.object_position,
  );
  if (explicit) return explicit;
  const focal = display?.focal_point || candidate?.focal_point || item?.focal_point || asset?.focal_point || {};
  const x = Number(focal?.x ?? display?.focal_x ?? candidate?.focal_x ?? item?.focal_x);
  const y = Number(focal?.y ?? display?.focal_y ?? candidate?.focal_y ?? item?.focal_y);
  const safeX = Number.isFinite(x) ? Math.min(1, Math.max(0, x)) : .5;
  const safeY = Number.isFinite(y) ? Math.min(1, Math.max(0, y)) : .5;
  return `${Math.round(safeX * 100)}% ${Math.round(safeY * 100)}%`;
}

function primaryImageAsset(item) {
  const candidate = item?.candidate || item || {};
  const display = displayPayload(item);
  const assets = display?.image_assets || candidate?.image_assets || item?.image_assets || [];
  const primarySrc = display?.image_url || candidate?.image_url || item?.image_url || '';
  const asset = Array.isArray(assets)
    ? assets.find((entry) => entry?.src === primarySrc) || assets[0]
    : null;
  if (asset) return asset;
  return {
    src: primarySrc,
    width: Number(display?.image_width || candidate?.image_width || item?.image_width || 0),
    height: Number(display?.image_height || candidate?.image_height || item?.image_height || 0),
    image_text_mode: display?.image_text_mode || candidate?.image_text_mode || item?.image_text_mode || 'unknown',
    media_role: display?.image_media_role || display?.media_role || candidate?.image_media_role || candidate?.media_role || item?.image_media_role || item?.media_role,
    media_semantic_status: display?.media_semantic_status || candidate?.media_semantic_status || item?.media_semantic_status,
    safe_crop: display?.safe_crop ?? candidate?.safe_crop ?? item?.safe_crop,
    current_pixel_sha256: display?.current_pixel_sha256 || candidate?.current_pixel_sha256 || item?.current_pixel_sha256,
    geometry_pixel_sha256: display?.geometry_pixel_sha256 || candidate?.geometry_pixel_sha256 || item?.geometry_pixel_sha256,
    geometry_status: display?.geometry_status || candidate?.geometry_status || item?.geometry_status,
    geometry_coordinate_space: display?.geometry_coordinate_space || candidate?.geometry_coordinate_space || item?.geometry_coordinate_space,
    face_boxes: display?.face_boxes || candidate?.face_boxes || item?.face_boxes,
    valuable_region: display?.valuable_region || candidate?.valuable_region || item?.valuable_region,
  };
}

export function relatedCardMediaGeometry(item) {
  const asset = primaryImageAsset(item);
  const display = displayPayload(item);
  const imageTextMode = asset?.image_text_mode || display?.image_text_mode || 'unknown';
  const documentMedia = imageTextMode !== 'visual_only';
  const width = Number(asset?.width || display?.image_width || 0);
  const height = Number(asset?.height || display?.image_height || 0);
  return {
    asset,
    documentMedia,
    ratio: finiteRatio(width > 0 && height > 0 ? width / height : 0, documentMedia ? DEFAULT_DOCUMENT_RATIO : 1),
  };
}

/**
 * One authoritative compact recommendation-card media decision. Both the row
 * packer and EventCard call this gate, so a visual photo cannot be advertised
 * as cover by the row and later rendered as contain by the card (or vice
 * versa); OCR/document media stays on the explicit contain branch.
 */
export function resolveRelatedCardMediaTreatment(item, targetAspect, geometry = relatedCardMediaGeometry(item)) {
  const mediaRatio = finiteRatio(geometry?.ratio, geometry?.documentMedia ? DEFAULT_DOCUMENT_RATIO : 1);
  const potentialCoverCrop = Math.max(0, 1 - Math.min(mediaRatio / targetAspect, targetAspect / mediaRatio));
  if (geometry?.documentMedia) {
    return {
      mediaKind:'document',
      mediaTreatment:'document-contain',
      fit:'contain',
      objectPosition:'50% 50%',
      cropReason:'document_media',
      potentialCoverCrop,
      coverCrop:0,
    };
  }
  const crop = resolveEventImageCrop(geometry?.asset || primaryImageAsset(item), targetAspect);
  // Restore the accepted recommendation-card contract from before the bbox
  // rollout: `visual_only` is the product-level permission to fill a compact
  // preview. Exact protected geometry improves its object position when it is
  // available, but missing/stale/over-constrained bbox metadata must not turn
  // a photographic card into a letterboxed document. OCR and unknown media
  // still take the document branch above and remain fully contained.
  const objectPosition = crop.fit === 'cover'
    ? (crop.objectPosition || visualFallbackObjectPosition(item))
    : visualFallbackObjectPosition(item);
  return {
    mediaKind:'visual',
    mediaTreatment:'visual-cover',
    fit:'cover',
    objectPosition,
    cropReason:crop.fit === 'cover'
      ? (crop.reason || 'protected_geometry_cover')
      : `visual_only_focal_fallback:${crop.reason || 'semantic_crop_gate_closed'}`,
    potentialCoverCrop,
    coverCrop:potentialCoverCrop,
  };
}

function geometricMean(ratios, fallback = 1) {
  return ratios.length
    ? Math.exp(ratios.reduce((sum, ratio) => sum + Math.log(ratio), 0) / ratios.length)
    : fallback;
}

function initialRowRatio(row, mediaTreatment) {
  const documents = row.filter(({ documentMedia }) => documentMedia);
  const visuals = row.filter(({ documentMedia }) => !documentMedia);
  const documentRatios = documents.map(({ ratio }) => ratio);
  const visualRatios = visuals.map(({ ratio }) => ratio);
  let targetRatio = 1;
  let rowMode = 'visual-square';

  if (documents.length === 0) {
    targetRatio = Math.min(MAX_VISUAL_RATIO, Math.max(MIN_VISUAL_RATIO, geometricMean(visualRatios)));
    rowMode = 'visual-squareish';
  } else if (visuals.length > 0) {
    if (mediaTreatment === 'cover') {
      targetRatio = 1.12;
      rowMode = 'mixed-compact-cover';
    } else if (mediaTreatment === 'ambient') {
      targetRatio = 1;
      rowMode = 'mixed-square-ambient';
    } else {
      const documentCenter = geometricMean(documentRatios, DEFAULT_DOCUMENT_RATIO);
      targetRatio = [DEFAULT_DOCUMENT_RATIO, 1, MAX_VISUAL_RATIO]
        .sort((left, right) => Math.abs(Math.log(left / documentCenter)) - Math.abs(Math.log(right / documentCenter)))[0];
      rowMode = 'mixed-adaptive-ambient';
    }
  } else if (documentRatios.length === 1) {
    targetRatio = Math.min(0.9, documentRatios[0] * 1.1);
    rowMode = 'document-bounded-vertical-crop';
  } else {
    targetRatio = Math.sqrt(Math.min(...documentRatios) * Math.max(...documentRatios));
    rowMode = 'document-minimax';
  }
  return { targetRatio, rowMode };
}

function rowLayout(row, rowIndex, mediaTreatment) {
  const initial = initialRowRatio(row, mediaTreatment);
  // Keep the reviewed compact row geometry. In particular a single portrait
  // photograph must not expand one three-column row to its natural 2:3 ratio;
  // it remains a square-ish preview and uses the same EventCard crop contract.
  const targetRatio = initial.targetRatio;
  const decisions = row.map((entry) => resolveRelatedCardMediaTreatment(entry.item, targetRatio, entry));
  const rowMode = initial.rowMode;
  const entries = row.map(({ item, ratio:mediaRatio }, index) => {
    const decision = decisions[index];
    return {
      item,
      layout: {
        mediaRatio,
        rowRatio:targetRatio,
        rowIndex,
        rowMode,
        ...decision,
        rowWorstCrop:0,
      },
    };
  });
  const rowWorstCrop = Math.max(0, ...entries
    .filter(({ layout }) => layout.mediaKind === 'document')
    .map(({ layout }) => layout.coverCrop));
  for (const entry of entries) entry.layout.rowWorstCrop = rowWorstCrop;
  return entries;
}

export function packRelatedCardRows(items, options = {}) {
  const limit = Math.max(0, Number(options.limit ?? items.length));
  const rowSize = Math.max(1, Number(options.rowSize ?? 3));
  const mediaTreatment = options.mediaTreatment || 'hybrid';
  const geometry = options.geometry || relatedCardMediaGeometry;
  const selected = items.slice(0, limit).map((item) => ({ item, ...geometry(item) }));
  const packed = [];
  for (let offset = 0, rowIndex = 0; offset < selected.length; offset += rowSize, rowIndex += 1) {
    packed.push(...rowLayout(selected.slice(offset, offset + rowSize), rowIndex, mediaTreatment));
  }
  return packed;
}

export const RELATED_CARD_MAX_DOCUMENT_CROP = MAX_DOCUMENT_CROP;
