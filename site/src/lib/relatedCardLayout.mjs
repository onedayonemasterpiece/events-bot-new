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
 * One authoritative recommendation-card media decision. Both the row packer
 * and EventCard call this gate, so a protected crop can never be advertised as
 * cover by the row and later rendered as contain by the card (or vice versa).
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
  const fit = crop.fit === 'cover' ? 'cover' : 'contain';
  return {
    mediaKind:'visual',
    mediaTreatment:fit === 'cover' ? 'visual-cover' : 'visual-contain',
    fit,
    objectPosition:crop.objectPosition || '50% 50%',
    cropReason:crop.reason || 'semantic_crop_gate_closed',
    potentialCoverCrop,
    coverCrop:fit === 'cover' ? potentialCoverCrop : 0,
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

function subsetMeans(ratios) {
  const values = [];
  const combinations = 1 << ratios.length;
  for (let mask = 1; mask < combinations; mask += 1) {
    const selected = ratios.filter((_ratio, index) => mask & (1 << index));
    values.push(geometricMean(selected));
  }
  return values;
}

function compareScores(left, right) {
  for (let index = 0; index < left.length; index += 1) {
    if (Math.abs(left[index] - right[index]) > 1e-9) return left[index] - right[index];
  }
  return 0;
}

function authoritativeRowDecision(row, initialTarget) {
  const candidates = [...new Set([
    initialTarget,
    ...row.map(({ ratio }) => ratio),
    ...subsetMeans(row.map(({ ratio }) => ratio)),
  ].map((ratio) => finiteRatio(ratio, initialTarget).toFixed(8)))].map(Number);
  let best = null;
  for (const targetRatio of candidates) {
    const decisions = row.map((entry) => resolveRelatedCardMediaTreatment(entry.item, targetRatio, entry));
    const contained = decisions.filter(({ fit }) => fit === 'contain');
    const containedLosses = contained.map(({ potentialCoverCrop }) => potentialCoverCrop);
    const coverLosses = decisions.filter(({ fit }) => fit === 'cover').map(({ coverCrop }) => coverCrop);
    const score = [
      Math.max(0, ...containedLosses),
      containedLosses.length ? containedLosses.reduce((sum, value) => sum + value, 0) / containedLosses.length : 0,
      Math.max(0, ...coverLosses),
      Math.abs(Math.log(targetRatio / initialTarget)),
    ];
    if (!best || compareScores(score, best.score) < 0) best = { targetRatio, decisions, score };
  }
  return best;
}

function rowLayout(row, rowIndex, mediaTreatment) {
  const initial = initialRowRatio(row, mediaTreatment);
  const authoritative = authoritativeRowDecision(row, initial.targetRatio);
  const targetRatio = authoritative?.targetRatio || initial.targetRatio;
  const decisions = authoritative?.decisions || row.map((entry) => resolveRelatedCardMediaTreatment(entry.item, targetRatio, entry));
  const finalContain = decisions.some(({ fit }) => fit === 'contain');
  const targetChanged = Math.abs(Math.log(targetRatio / initial.targetRatio)) > 1e-7;
  const rowMode = `${initial.rowMode}${finalContain || targetChanged ? '-authoritative' : ''}`;
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
  const rowWorstCrop = Math.max(0, ...entries.map(({ layout }) => layout.coverCrop));
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
