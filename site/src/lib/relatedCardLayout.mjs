import { resolveEventImageCrop } from './imageCrop.mjs';
import listingMediaOverrides from '../data/listingMediaOverrides.json' with { type:'json' };

// CSS aspect ratios are width / height. The compact default is therefore the
// horizontal 5:4 counterpart of the product's "4:5 by height" rule.
const PREFERRED_VISUAL_RATIO = 5 / 4;
const VERY_TALL_DOCUMENT_RATIO = 4 / 5;
const MAX_DOCUMENT_CROP = 0.2;
// The desktop recommendation column is about 360px wide at the acceptance
// viewport. Chrome is intrinsic per row, so the optimizer models the bounded
// title/meta/place line budget instead of reserving one global fixed height.
const REFERENCE_CARD_WIDTH = 360;
const BASE_BODY_HEIGHT = 109;
const EXTRA_TEXT_LINE_HEIGHT = 23;
const UTILITY_AND_FEEDBACK_HEIGHT = 114;
const EPSILON = 1e-9;
const REVIEWED_MEDIA_OVERRIDES = Array.isArray(listingMediaOverrides?.items)
  ? listingMediaOverrides.items
  : [];

function finiteRatio(value, fallback) {
  const ratio = Number(value);
  return Number.isFinite(ratio) && ratio > 0 ? ratio : fallback;
}

function displayPayload(item) {
  const candidate = item?.candidate || item || {};
  return candidate?.display || item?.display || candidate;
}

function compactLineCount(value, charactersPerLine, maxLines = 2) {
  const length = Array.from(String(value || '').trim()).length;
  if (!length) return 1;
  return Math.max(1, Math.min(maxLines, Math.ceil(length / charactersPerLine)));
}

function estimatedChromeCost(row) {
  const bodyHeight = Math.max(...row.map(({ item }) => {
    const display = displayPayload(item);
    const candidate = item?.candidate || item || {};
    const title = display?.title || candidate?.title || '';
    const place = [display?.city || candidate?.city, display?.venue_name || candidate?.venue_name]
      .filter(Boolean)
      .join(' · ');
    const titleLines = compactLineCount(title, 29);
    const placeLines = compactLineCount(place, 43);
    return BASE_BODY_HEIGHT
      + (titleLines - 1) * EXTRA_TEXT_LINE_HEIGHT
      + (placeLines - 1) * EXTRA_TEXT_LINE_HEIGHT;
  }));
  return (bodyHeight + UTILITY_AND_FEEDBACK_HEIGHT) / REFERENCE_CARD_WIDTH;
}

function normalizedObjectPosition(value) {
  const raw = String(value || '').trim();
  return /^[a-z0-9.%\s-]+$/iu.test(raw) ? raw : '';
}

function applyReviewedMediaOverride(asset) {
  if (!asset?.src) return asset;
  const reviewed = REVIEWED_MEDIA_OVERRIDES.find((entry) => (
    entry?.sourceSrc === asset.src
    && entry?.imageTextMode === 'visual_only'
    && entry?.cropEvidence
  ));
  if (!reviewed) return asset;
  return {
    ...asset,
    original_src:asset.src,
    src:reviewed.replacementSrc || asset.src,
    width:Number(reviewed.width || asset.width || 0),
    height:Number(reviewed.height || asset.height || 0),
    image_text_mode:'visual_only',
    media_semantic_status:'classified',
    media_role:'event_photo',
    media_role_confidence:1,
    image_kind:reviewed.imageKind || 'illustration',
    safe_crop:true,
    focal_point:reviewed.focalPoint || asset.focal_point || { x:.5, y:.5 },
    recommended_object_position:reviewed.objectPosition || asset.recommended_object_position || '50% 50%',
    thumbnail_sources:reviewed.thumbnailSources || asset.thumbnail_sources,
    listing_crop_evidence:reviewed.cropEvidence,
  };
}

export function relatedCardPrimaryImageAsset(item) {
  const candidate = item?.candidate || item || {};
  const display = displayPayload(item);
  const assets = display?.image_assets || candidate?.image_assets || item?.image_assets || [];
  const primarySrc = display?.image_url || candidate?.image_url || item?.image_url || '';
  const asset = Array.isArray(assets)
    ? assets.find((entry) => entry?.src === primarySrc) || assets[0]
    : null;
  if (asset) return applyReviewedMediaOverride(asset);
  return applyReviewedMediaOverride({
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
  });
}

function visualFallbackObjectPosition(item) {
  const candidate = item?.candidate || item || {};
  const display = displayPayload(item);
  const asset = relatedCardPrimaryImageAsset(item);
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
  const asset = relatedCardPrimaryImageAsset(item);
  const display = displayPayload(item);
  const rawImageTextMode = asset?.image_text_mode || display?.image_text_mode || 'unknown';
  const semanticStatus = asset?.media_semantic_status || display?.media_semantic_status || '';
  // A semantic-classifier error invalidates any optimistic visual treatment
  // carried by a stale or partial payload. Pending assets may still use an
  // explicit image_text_mode, but missing evidence always resolves unknown.
  const imageTextMode = semanticStatus === 'error'
    ? 'unknown'
    : ['ocr_text', 'visual_only'].includes(rawImageTextMode) ? rawImageTextMode : 'unknown';
  const documentMedia = imageTextMode !== 'visual_only';
  const width = Number(asset?.width || display?.image_width || 0);
  const height = Number(asset?.height || display?.image_height || 0);
  const dimensionsKnown = Number.isFinite(width) && width > 0 && Number.isFinite(height) && height > 0;
  return {
    asset,
    imageTextMode,
    semanticStatus,
    documentMedia,
    resourcePresent:Boolean(asset?.src),
    dimensionsKnown,
    ratio: finiteRatio(dimensionsKnown ? width / height : 0, documentMedia ? VERY_TALL_DOCUMENT_RATIO : PREFERRED_VISUAL_RATIO),
  };
}

function cropFraction(sourceRatio, targetRatio) {
  return Math.max(0, 1 - Math.min(sourceRatio / targetRatio, targetRatio / sourceRatio));
}

/**
 * Final compact-card treatment. Classified visual media fills its shell.
 * A crop-area limit is not evidence that OCR text survives the crop.
 * Until this API has a verified text-region contract, document media remains
 * whole; an exact-ratio cover is uncropped and is still allowed. The existing
 * 20% maximum remains a ceiling, not permission to trim unlocated text.
 * Direct callers and packRelatedCardRows share this fail-closed boundary.
 */
export function resolveRelatedCardMediaTreatment(item, targetAspect, geometry = relatedCardMediaGeometry(item)) {
  const mediaRatio = finiteRatio(geometry?.ratio, geometry?.documentMedia ? VERY_TALL_DOCUMENT_RATIO : PREFERRED_VISUAL_RATIO);
  const normalizedTargetAspect = finiteRatio(targetAspect, mediaRatio);
  const potentialCoverCrop = cropFraction(mediaRatio, normalizedTargetAspect);
  if (geometry?.documentMedia) {
    // Unknown classification and explicit classifier errors have no positive
    // crop evidence. Even known pixel dimensions cannot turn them into the
    // bounded-cover OCR exception: they remain whole on every EventCard.
    if (geometry?.imageTextMode === 'unknown' || geometry?.semanticStatus === 'error') {
      return {
        mediaKind:'document',
        mediaTreatment:'document-contain',
        fit:'contain',
        objectPosition:'50% 50%',
        cropReason:geometry?.semanticStatus === 'error' ? 'semantic_error_fail_closed' : 'unknown_media_fail_closed',
        potentialCoverCrop:null,
        coverCrop:null,
      };
    }
    // A fallback aspect ratio is useful for reserving layout space but is not
    // evidence that an OCR/document image fits the 20% crop budget. Unknown
    // intrinsic geometry therefore stays contained and exposes no numeric crop
    // claim for analytics or browser gates to mistake for a measured value.
    if (geometry?.dimensionsKnown === false) {
      return {
        mediaKind:'document',
        mediaTreatment:'document-contain',
        fit:'contain',
        objectPosition:'50% 50%',
        cropReason:'document_dimensions_unknown',
        potentialCoverCrop:null,
        coverCrop:null,
      };
    }
    if (potentialCoverCrop > MAX_DOCUMENT_CROP + EPSILON) {
      return {
        mediaKind:'document',
        mediaTreatment:'document-contain',
        fit:'contain',
        objectPosition:'50% 50%',
        cropReason:'document_crop_budget_exceeded',
        potentialCoverCrop,
        coverCrop:0,
      };
    }
    if (potentialCoverCrop > EPSILON) {
      // image_text_mode classifies the asset; it does not locate its text.
      // Centering a numerically legal crop can remove the entire headline.
      // Do not invent OCR boxes or treat photo valuable_region as text proof.
      return {
        mediaKind:'document',
        mediaTreatment:'document-contain',
        fit:'contain',
        objectPosition:'50% 50%',
        cropReason:'document_text_crop_unproven',
        potentialCoverCrop,
        coverCrop:0,
      };
    }
    return {
      mediaKind:'document',
      mediaTreatment:'document-safe-cover',
      fit:'cover',
      objectPosition:'50% 50%',
      cropReason:'document_uncropped',
      potentialCoverCrop,
      coverCrop:0,
    };
  }
  const crop = resolveEventImageCrop(geometry?.asset || relatedCardPrimaryImageAsset(item), normalizedTargetAspect);
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

/**
 * Mobile large cards are not miniature desktop recommendation rows.
 * Visual-only media uses one stable horizontal 5:4 frame; OCR/document media
 * keeps its intrinsic aspect and is never cropped. When document dimensions
 * are absent from the snapshot, the browser replaces the provisional ratio
 * with decoded naturalWidth/naturalHeight on load.
 */
export function resolveMobileEventCardMedia(item, options = {}) {
  const targetAspect = finiteRatio(options.targetAspect, PREFERRED_VISUAL_RATIO);
  const geometry = relatedCardMediaGeometry(item);
  if (geometry.documentMedia) {
    return {
      mediaRatio:geometry.ratio,
      rowRatio:geometry.ratio,
      rowIndex:0,
      rowColumn:0,
      rowMode:'mobile-document-natural',
      presentation:'flow',
      mediaKind:'document',
      mediaTreatment:'document-contain',
      fit:'contain',
      objectPosition:'50% 50%',
      cropReason:geometry.dimensionsKnown ? 'mobile_document_natural' : 'mobile_document_decode_natural',
      potentialCoverCrop:null,
      coverCrop:null,
      rowWorstCrop:null,
      rowCost:0,
      useNaturalAspect:true,
    };
  }
  const decision = resolveRelatedCardMediaTreatment(item, targetAspect, geometry);
  return {
    mediaRatio:geometry.ratio,
    rowRatio:targetAspect,
    rowIndex:0,
    rowColumn:0,
    rowMode:'mobile-visual-fixed-5x4',
    presentation:'flow',
    ...decision,
    rowWorstCrop:0,
    rowCost:0,
    useNaturalAspect:false,
  };
}

function entryResourcePresent(entry) {
  return entry?.resourcePresent !== false && Boolean(entry?.asset?.src ?? true);
}

function entryIdentifier(entry) {
  const item = entry?.item || {};
  const candidate = item?.candidate || item;
  return String(candidate?.id ?? candidate?.event_id ?? item?.id ?? item?.event_id ?? entry?.originalIndex ?? 'unknown');
}

function entryDimensions(entry) {
  const width = Number(entry?.asset?.width || 0);
  const height = Number(entry?.asset?.height || 0);
  return Number.isFinite(width) && width > 0 && Number.isFinite(height) && height > 0
    ? `${width}x${height}`
    : 'unknown';
}

function ratioLabel(entry) {
  return `${entryIdentifier(entry)}@${entryDimensions(entry)}=${finiteRatio(entry?.ratio, 0).toFixed(8)}`;
}

function rowFramingConflict(row) {
  const documents = row.filter((entry) => entry.documentMedia && entryResourcePresent(entry));
  const unknown = documents.find((entry) => entry.dimensionsKnown === false);
  if (unknown) return `document-natural-ratio-unknown:${entryIdentifier(unknown)}@${entryDimensions(unknown)}`;
  for (let left = 0; left < documents.length; left += 1) {
    for (let right = left + 1; right < documents.length; right += 1) {
      if (Math.abs(documents[left].ratio - documents[right].ratio) > EPSILON) {
        return `document-natural-ratio-mismatch:${ratioLabel(documents[left])}|${ratioLabel(documents[right])}`;
      }
    }
  }
  return 'natural-frame-partition-unavailable';
}

function decisionPaintedFields(entry, targetRatio, decision) {
  if (!entryResourcePresent(entry)) return null;
  if (decision?.fit === 'cover') return false;
  if (entry?.dimensionsKnown === false) return null;
  return cropFraction(finiteRatio(entry?.ratio, targetRatio), targetRatio) > EPSILON;
}

function rowTarget(row) {
  const documents = row.filter((entry) => entry.documentMedia && entryResourcePresent(entry));
  if (documents.length) {
    // No current document payload locates protected text. Therefore its only
    // proven no-field frame is its decoded natural ratio. The historic 20%
    // area interval was not crop evidence: selecting its upper bound forced
    // the resolver to contain the source and painted the rejected fields.
    if (documents.some((entry) => entry.dimensionsKnown === false)) return null;
    const targetRatio = documents[0].ratio;
    if (documents.some((entry) => Math.abs(entry.ratio - targetRatio) > EPSILON)) return null;
    return {
      targetRatio,
      rowMode:'document-led-natural',
    };
  }
  return {
    // A photo-only recommendation row has no semantic neighbour that needs to
    // dictate its height. Use the canonical compact 5:4 frame rather than
    // deriving an ever-wider row from source geometry; every image still fills
    // that shared frame through cover and the optimizer remains free to move
    // cards between rows.
    targetRatio:PREFERRED_VISUAL_RATIO,
    rowMode:'visual-compact-5x4',
  };
}

function rowPlan(row, mediaTreatment) {
  const target = rowTarget(row, mediaTreatment);
  if (!target) return null;
  const decisions = row.map((entry) => resolveRelatedCardMediaTreatment(entry.item, target.targetRatio, entry));
  const paintedFields = decisions.map((decision, index) => decisionPaintedFields(row[index], target.targetRatio, decision));
  if (paintedFields.some((value) => value === true)) return null;
  const visualCrop = decisions
    .filter(({ mediaKind }) => mediaKind === 'visual')
    .reduce((sum, decision) => sum + decision.coverCrop, 0);
  return {
    ...target,
    decisions,
    // Normalized full-card row height: shared media height plus row-local
    // intrinsic chrome estimated from the same bounded copy tracks as CSS.
    cost:estimatedChromeCost(row) + (1 / target.targetRatio),
    visualCrop,
    framingStatus:'satisfied',
    framingConflict:null,
    paintedFields,
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

function combinationsIncludingAnchor(mask, anchor, exactSize) {
  const rest = [];
  for (let index = anchor + 1; index < 31; index += 1) if (mask & (1 << index)) rest.push(index);
  if (exactSize === 1) return [[anchor]];
  const result = [];
  const choose = (start, selected) => {
    if (selected.length === exactSize - 1) {
      result.push([anchor, ...selected]);
      return;
    }
    for (let index = start; index < rest.length; index += 1) {
      choose(index + 1, [...selected, rest[index]]);
    }
  };
  choose(0, []);
  return result;
}

function combinations(mask, exactSize) {
  const indexes = [];
  for (let index = 0; index < 31; index += 1) if (mask & (1 << index)) indexes.push(index);
  const result = [];
  const choose = (start, selected) => {
    if (selected.length === exactSize) {
      result.push(selected);
      return;
    }
    for (let index = start; index < indexes.length; index += 1) {
      choose(index + 1, [...selected, indexes[index]]);
    }
  };
  choose(0, []);
  return result;
}

function optimizeRows(selected, rowSize, mediaTreatment, preserveOrder = false) {
  if (!selected.length) return [];
  if (preserveOrder) {
    const rows = [];
    for (let offset = 0; offset < selected.length; offset += rowSize) {
      const entries = selected.slice(offset, offset + rowSize);
      const plan = rowPlan(entries, mediaTreatment);
      if (!plan) return null;
      rows.push({ entries, plan });
    }
    return rows;
  }
  // Current recommendation surfaces cap at 10. Keep a deterministic fallback
  // rather than allowing a bitmask overflow if a future caller forgets limit.
  if (selected.length > 20) {
    const rows = [];
    for (let offset = 0; offset < selected.length; offset += rowSize) {
      const entries = selected.slice(offset, offset + rowSize);
      const plan = rowPlan(entries, mediaTreatment);
      if (!plan) return null;
      rows.push({ entries, plan });
    }
    return rows;
  }
  const fullMask = (1 << selected.length) - 1;
  const memo = new Map();
  const solveFullRows = (mask) => {
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
      const tail = solveFullRows(mask & ~groupMask);
      if (!tail) continue;
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

  const remainderSize = selected.length % rowSize;
  if (!remainderSize) {
    const full = solveFullRows(fullMask);
    return full?.groups || null;
  }

  // The incomplete group is a product constraint, not a scoring preference:
  // enumerate which entries belong to it, optimize only full rows around it,
  // then append that remainder last. This also allows the first source item to
  // move into the final row when it is the only OCR-safe feasible partition.
  let best = null;
  for (const remainderIndexes of combinations(fullMask, remainderSize)) {
    const remainderEntries = remainderIndexes.map((index) => selected[index]);
    const remainderPlan = rowPlan(remainderEntries, mediaTreatment);
    if (!remainderPlan) continue;
    const remainderMask = remainderIndexes.reduce((value, index) => value | (1 << index), 0);
    const full = solveFullRows(fullMask & ~remainderMask);
    if (!full) continue;
    const candidate = {
      cost:full.cost + remainderPlan.cost,
      rows:full.rows + 1,
      visualCrop:full.visualCrop + remainderPlan.visualCrop,
      displacement:full.displacement + remainderIndexes.reduce((sum, value, index) => sum + Math.abs(value - (selected.length - remainderSize + index)), 0),
      signature:`${full.signature}|${remainderIndexes.join('.')}`,
      groups:[...full.groups, { entries:remainderEntries, plan:remainderPlan }],
    };
    if (comparePlans(candidate, best) < 0) best = candidate;
  }
  return best?.groups || null;
}

/**
 * Layout may choose geometry, not remove admitted events. If no complete
 * intrinsic-ratio partition exists, keep the ranked prefix and use a common
 * compact frame only for incompatible rows. Documents stay whole through the
 * same media resolver; compatible rows keep their existing layout unchanged.
 */
function containedFallbackRows(selected, rowSize, mediaTreatment) {
  const rows = [];
  for (let offset = 0; offset < selected.length; offset += rowSize) {
    const entries = selected.slice(offset, offset + rowSize);
    let plan = rowPlan(entries, mediaTreatment);
    if (!plan) {
      // Preserve the first factual document whole rather than inventing a 5:4
      // document frame. Other incompatible sources stay fail-closed, and the
      // row is explicitly UNSATISFIED with its smallest measured conflict.
      const naturalAnchor = entries.find((entry) => (
        entry.documentMedia && entryResourcePresent(entry) && entry.dimensionsKnown !== false
      ));
      const targetRatio = naturalAnchor?.ratio || PREFERRED_VISUAL_RATIO;
      const decisions = entries.map((entry) => resolveRelatedCardMediaTreatment(entry.item, targetRatio, entry));
      plan = {
        targetRatio,
        rowMode:'document-natural-conflict-contained',
        decisions,
        cost:estimatedChromeCost(entries) + (1 / targetRatio),
        visualCrop:decisions.filter(({ mediaKind }) => mediaKind === 'visual')
          .reduce((sum, decision) => sum + decision.coverCrop, 0),
        framingStatus:'unsatisfied',
        framingConflict:rowFramingConflict(entries),
        paintedFields:decisions.map((decision, index) => decisionPaintedFields(entries[index], targetRatio, decision)),
      };
    }
    rows.push({ entries, plan });
  }
  return rows;
}

function materializeRow(row, rowIndex, presentation) {
  const { entries:sourceEntries, plan } = row;
  const entries = sourceEntries.map(({ item, ratio:mediaRatio }, index) => ({
    item,
    layout: {
      mediaRatio,
      rowRatio:plan.targetRatio,
      rowIndex,
      rowColumn:index,
      rowMode:plan.rowMode,
      presentation,
      ...plan.decisions[index],
      framingStatus:plan.framingStatus,
      framingConflict:plan.framingConflict,
      paintedFields:plan.paintedFields[index],
      rowWorstCrop:0,
      rowCost:plan.cost,
    },
  }));
  const documentCrops = entries
    .filter(({ layout }) => layout.mediaKind === 'document')
    .map(({ layout }) => layout.coverCrop);
  const rowWorstCrop = documentCrops.some((value) => !Number.isFinite(value))
    ? null
    : Math.max(0, ...documentCrops);
  for (const entry of entries) entry.layout.rowWorstCrop = rowWorstCrop;
  return entries;
}

/**
 * Enumerate every feasible full-row grouping plus one optional final
 * remainder, then use bitmask dynamic programming to minimize the sum of
 * normalized full-card row heights.
 * Reordering is intentional and deterministic within the feasible intrinsic
 * partitions unless preserveOrder is requested. If none exists, contained
 * rows preserve the admitted prefix and publish an exact UNSATISFIED conflict;
 * that content-preserving fallback is not an accepted no-fields composition.
 */
export function packRelatedCardRows(items, options = {}) {
  const requestedLimit = Number(options.limit ?? items.length);
  const limit = Math.max(0, Math.floor(Number.isFinite(requestedLimit) ? requestedLimit : items.length));
  const requestedRowSize = Number(options.rowSize ?? 3);
  const rowSize = Math.max(1, Math.min(6, Math.floor(Number.isFinite(requestedRowSize) ? requestedRowSize : 3)));
  const mediaTreatment = options.mediaTreatment || 'hybrid';
  const presentation = options.presentation === 'flow' ? 'flow' : 'related-grid';
  const geometry = options.geometry || relatedCardMediaGeometry;
  const targetCount = Math.min(limit, items.length);
  if (!targetCount) return [];
  // The caller owns eligibility and limit. Geometry optimization must preserve
  // the admitted ranked prefix, including a mixed-document final remainder.
  const selected = items.slice(0, targetCount)
    .map((item, originalIndex) => ({ item, originalIndex, ...geometry(item) }));
  const rows = optimizeRows(selected, rowSize, mediaTreatment, options.preserveOrder === true)
    || containedFallbackRows(selected, rowSize, mediaTreatment);
  return rows.flatMap((row, rowIndex) => materializeRow(row, rowIndex, presentation));
}

export const RELATED_CARD_MAX_DOCUMENT_CROP = MAX_DOCUMENT_CROP;
export const RELATED_CARD_PREFERRED_VISUAL_RATIO = PREFERRED_VISUAL_RATIO;
export const MOBILE_EVENT_CARD_VISUAL_RATIO = PREFERRED_VISUAL_RATIO;
