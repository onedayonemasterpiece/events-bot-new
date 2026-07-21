import { resolveEventImageCrop } from './imageCrop.mjs';

// CSS aspect ratios are width / height. The compact default is therefore the
// horizontal 5:4 counterpart of the product's "4:5 by height" rule.
const PREFERRED_VISUAL_RATIO = 5 / 4;
const MAX_VISUAL_RATIO = 8 / 5;
const VERY_TALL_DOCUMENT_RATIO = 4 / 5;
const MAX_DOCUMENT_CROP = 0.2;
// Desktop compact cards use fixed 184px body + 58px utility + 56px feedback
// tracks. At the 1536x864 acceptance viewport this is 0.777 normalized card
// widths, so this objective is the actual rendered row height rather than a
// media-only proxy. The browser gate separately proves the fixed chrome
// invariant on generated output.
const ROW_CHROME_COST = 0.777;
const EPSILON = 1e-9;

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
    ratio: finiteRatio(width > 0 && height > 0 ? width / height : 0, documentMedia ? VERY_TALL_DOCUMENT_RATIO : PREFERRED_VISUAL_RATIO),
  };
}

function cropFraction(sourceRatio, targetRatio) {
  return Math.max(0, 1 - Math.min(sourceRatio / targetRatio, targetRatio / sourceRatio));
}

/**
 * Final compact-card treatment. A card image always fills its shell: fields
 * are forbidden. OCR/document assets may use cover only inside the 20% crop
 * budget; packRelatedCardRows guarantees that target-ratio precondition.
 */
export function resolveRelatedCardMediaTreatment(item, targetAspect, geometry = relatedCardMediaGeometry(item)) {
  const mediaRatio = finiteRatio(geometry?.ratio, geometry?.documentMedia ? VERY_TALL_DOCUMENT_RATIO : PREFERRED_VISUAL_RATIO);
  const potentialCoverCrop = cropFraction(mediaRatio, targetAspect);
  if (geometry?.documentMedia) {
    return {
      mediaKind:'document',
      mediaTreatment:'document-safe-cover',
      fit:'cover',
      objectPosition:'50% 50%',
      cropReason:potentialCoverCrop <= EPSILON ? 'document_uncropped' : 'document_bounded_cover',
      potentialCoverCrop,
      coverCrop:potentialCoverCrop,
    };
  }
  const crop = resolveEventImageCrop(geometry?.asset || primaryImageAsset(item), targetAspect);
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

function geometricMean(ratios, fallback = PREFERRED_VISUAL_RATIO) {
  return ratios.length
    ? Math.exp(ratios.reduce((sum, ratio) => sum + Math.log(ratio), 0) / ratios.length)
    : fallback;
}

function documentTargetInterval(entry) {
  // Ordinary OCR posters stay uncropped. Only an unusually tall document may
  // spend up to the explicit 20% area budget to keep its whole row compact.
  if (entry.ratio >= VERY_TALL_DOCUMENT_RATIO - EPSILON) return [entry.ratio, entry.ratio];
  return [entry.ratio, entry.ratio / (1 - MAX_DOCUMENT_CROP)];
}

function rowTarget(row) {
  const documents = row.filter(({ documentMedia }) => documentMedia);
  if (documents.length) {
    const intervals = documents.map(documentTargetInterval);
    const lower = Math.max(...intervals.map(([value]) => value));
    const upper = Math.min(...intervals.map(([, value]) => value));
    if (lower > upper + EPSILON) return null;
    return {
      targetRatio:upper,
      rowMode:documents.some(({ ratio }) => ratio < VERY_TALL_DOCUMENT_RATIO - EPSILON)
        ? 'document-led-bounded-cover'
        : 'document-led-uncropped',
    };
  }
  const visualRatios = row.map(({ ratio }) => ratio);
  const evidenceRatio = geometricMean(visualRatios);
  return {
    targetRatio:Math.min(MAX_VISUAL_RATIO, Math.max(PREFERRED_VISUAL_RATIO, evidenceRatio)),
    rowMode:evidenceRatio > PREFERRED_VISUAL_RATIO + EPSILON ? 'visual-horizontal-adaptive' : 'visual-compact-5x4',
  };
}

function rowPlan(row, mediaTreatment) {
  const target = rowTarget(row, mediaTreatment);
  if (!target) return null;
  const decisions = row.map((entry) => resolveRelatedCardMediaTreatment(entry.item, target.targetRatio, entry));
  if (decisions.some((decision) => decision.mediaKind === 'document' && decision.coverCrop > MAX_DOCUMENT_CROP + EPSILON)) return null;
  const visualCrop = decisions
    .filter(({ mediaKind }) => mediaKind === 'visual')
    .reduce((sum, decision) => sum + decision.coverCrop, 0);
  return {
    ...target,
    decisions,
    // Normalized full-card row height: shared media height plus the invariant
    // title/meta/action chrome. This is the objective minimized page-wide.
    cost:ROW_CHROME_COST + (1 / target.targetRatio),
    visualCrop,
  };
}

function comparePlans(left, right) {
  if (!right) return -1;
  const scoreLeft = [left.cost, left.rows, left.visualCrop, left.displacement];
  const scoreRight = [right.cost, right.rows, right.visualCrop, right.displacement];
  for (let index = 0; index < scoreLeft.length; index += 1) {
    if (Math.abs(scoreLeft[index] - scoreRight[index]) > EPSILON) return scoreLeft[index] - scoreRight[index];
  }
  return left.signature.localeCompare(right.signature);
}

function combinationsIncludingAnchor(mask, anchor, rowSize) {
  const rest = [];
  for (let index = anchor + 1; index < 31; index += 1) if (mask & (1 << index)) rest.push(index);
  const result = [[anchor]];
  const choose = (start, selected) => {
    if (selected.length >= rowSize - 1) return;
    for (let index = start; index < rest.length; index += 1) {
      const next = [...selected, rest[index]];
      result.push([anchor, ...next]);
      choose(index + 1, next);
    }
  };
  choose(0, []);
  return result;
}

function optimizeRows(selected, rowSize, mediaTreatment) {
  if (!selected.length) return [];
  // Current recommendation surfaces cap at 10. Keep a deterministic fallback
  // rather than allowing a bitmask overflow if a future caller forgets limit.
  if (selected.length > 20) {
    const rows = [];
    for (let offset = 0; offset < selected.length; offset += rowSize) rows.push(selected.slice(offset, offset + rowSize));
    return rows;
  }
  const fullMask = (1 << selected.length) - 1;
  const memo = new Map();
  const solve = (mask) => {
    if (!mask) return { cost:0, rows:0, visualCrop:0, displacement:0, signature:'', groups:[] };
    if (memo.has(mask)) return memo.get(mask);
    let anchor = 0;
    while (!(mask & (1 << anchor))) anchor += 1;
    let best = null;
    for (const indexes of combinationsIncludingAnchor(mask, anchor, rowSize)) {
      const group = indexes.map((index) => selected[index]);
      const plan = rowPlan(group, mediaTreatment);
      if (!plan) continue;
      const groupMask = indexes.reduce((value, index) => value | (1 << index), 0);
      const tail = solve(mask & ~groupMask);
      const displacement = indexes.reduce((sum, value, index) => sum + Math.abs(value - (anchor + index)), 0);
      const signature = `${indexes.join('.')};${tail.signature}`;
      const candidate = {
        cost:plan.cost + tail.cost,
        rows:1 + tail.rows,
        visualCrop:plan.visualCrop + tail.visualCrop,
        displacement:displacement + tail.displacement,
        signature,
        groups:[{ entries:group, plan }, ...tail.groups],
      };
      if (comparePlans(candidate, best) < 0) best = candidate;
    }
    memo.set(mask, best);
    return best;
  };
  return solve(fullMask)?.groups || selected.map((entry) => ({ entries:[entry], plan:rowPlan([entry], mediaTreatment) }));
}

function materializeRow(row, rowIndex) {
  const { entries:sourceEntries, plan } = row;
  const entries = sourceEntries.map(({ item, ratio:mediaRatio }, index) => ({
    item,
    layout: {
      mediaRatio,
      rowRatio:plan.targetRatio,
      rowIndex,
      rowColumn:index,
      rowMode:plan.rowMode,
      ...plan.decisions[index],
      rowWorstCrop:0,
      rowCost:plan.cost,
    },
  }));
  const rowWorstCrop = Math.max(0, ...entries
    .filter(({ layout }) => layout.mediaKind === 'document')
    .map(({ layout }) => layout.coverCrop));
  for (const entry of entries) entry.layout.rowWorstCrop = rowWorstCrop;
  return entries;
}

/**
 * Enumerate every feasible row grouping (up to rowSize), then use bitmask
 * dynamic programming to minimize the sum of normalized full-card row heights.
 * Reordering is therefore intentional, deterministic and globally optimal for
 * the declared cost model rather than a greedy per-row guess.
 */
export function packRelatedCardRows(items, options = {}) {
  const limit = Math.max(0, Number(options.limit ?? items.length));
  const rowSize = Math.max(1, Math.min(6, Number(options.rowSize ?? 3)));
  const mediaTreatment = options.mediaTreatment || 'hybrid';
  const geometry = options.geometry || relatedCardMediaGeometry;
  const selected = items.slice(0, limit).map((item, originalIndex) => ({ item, originalIndex, ...geometry(item) }));
  const rows = optimizeRows(selected, rowSize, mediaTreatment);
  return rows.flatMap((row, rowIndex) => materializeRow(row, rowIndex));
}

export const RELATED_CARD_MAX_DOCUMENT_CROP = MAX_DOCUMENT_CROP;
export const RELATED_CARD_PREFERRED_VISUAL_RATIO = PREFERRED_VISUAL_RATIO;
