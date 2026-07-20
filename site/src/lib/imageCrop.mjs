const clamp = (value, min, max) => Math.min(max, Math.max(min, value));
const round = (value) => Math.round(value * 1_000_000) / 1_000_000;

function validBox(box) {
  if (!box || typeof box !== 'object') return false;
  const values = [box.x, box.y, box.w, box.h].map(Number);
  return values.every(Number.isFinite)
    && values[0] >= 0
    && values[1] >= 0
    && values[2] > 0
    && values[3] > 0
    && values[0] + values[2] <= 1.000001
    && values[1] + values[3] <= 1.000001;
}

function currentClassifiedGeometry(asset) {
  return asset?.geometry_status === 'classified'
    && asset.geometry_coordinate_space === 'normalized_0_1'
    && typeof asset.current_pixel_sha256 === 'string'
    && asset.current_pixel_sha256.length > 0
    && asset.current_pixel_sha256 === asset.geometry_pixel_sha256;
}

function contain(reason) {
  return {
    fit:'contain',
    objectPosition:'50% 50%',
    cropWindow:null,
    reason,
  };
}

/**
 * Solve one CSS `object-fit: cover` window in normalized source coordinates.
 * Stored boxes are never changed. Faces and the viewer-value region are all
 * protected, with an outer normalized margin. If that union cannot fit in the
 * requested aspect ratio the result deliberately fails closed to `contain`.
 */
export function solveProtectedCrop({ sourceWidth, sourceHeight, targetAspect, boxes, margin = 0.035 }) {
  const width = Number(sourceWidth);
  const height = Number(sourceHeight);
  const aspect = Number(targetAspect);
  if (!(width > 0 && height > 0 && aspect > 0)) return contain('invalid_dimensions');
  const protectedBoxes = Array.isArray(boxes) ? boxes.filter(validBox) : [];
  if (protectedBoxes.length === 0) return contain('missing_protected_regions');

  const safeMargin = clamp(Number(margin) || 0, 0, 0.2);
  const left = clamp(Math.min(...protectedBoxes.map((box) => Number(box.x))) - safeMargin, 0, 1);
  const top = clamp(Math.min(...protectedBoxes.map((box) => Number(box.y))) - safeMargin, 0, 1);
  const right = clamp(Math.max(...protectedBoxes.map((box) => Number(box.x) + Number(box.w))) + safeMargin, 0, 1);
  const bottom = clamp(Math.max(...protectedBoxes.map((box) => Number(box.y) + Number(box.h))) + safeMargin, 0, 1);

  const sourceAspect = width / height;
  const cropWidth = aspect < sourceAspect ? aspect / sourceAspect : 1;
  const cropHeight = aspect >= sourceAspect ? sourceAspect / aspect : 1;
  if (right - left > cropWidth + 1e-9 || bottom - top > cropHeight + 1e-9) {
    return contain('protected_regions_do_not_fit');
  }

  const chooseOrigin = (minimum, maximum, cropSize) => {
    if (cropSize >= 1 - 1e-9) return 0;
    const lowest = Math.max(0, maximum - cropSize);
    const highest = Math.min(minimum, 1 - cropSize);
    const centered = (minimum + maximum - cropSize) / 2;
    return clamp(centered, lowest, highest);
  };
  const x = chooseOrigin(left, right, cropWidth);
  const y = chooseOrigin(top, bottom, cropHeight);
  // CSS object-position percentages distribute the rendered overflow; they are
  // not the normalized center of the crop window.
  const positionX = cropWidth >= 1 - 1e-9 ? 0.5 : x / (1 - cropWidth);
  const positionY = cropHeight >= 1 - 1e-9 ? 0.5 : y / (1 - cropHeight);
  return {
    fit:'cover',
    objectPosition:`${Math.round(clamp(positionX, 0, 1) * 100)}% ${Math.round(clamp(positionY, 0, 1) * 100)}%`,
    cropWindow:{ x:round(x), y:round(y), w:round(cropWidth), h:round(cropHeight) },
    reason:'protected_regions_fit',
  };
}

/** Fail-closed semantic and pixel-provenance gate around the geometry solver. */
export function resolveEventImageCrop(asset, targetAspect, options = {}) {
  if (!asset || asset.image_text_mode !== 'visual_only') return contain('document_or_unknown_media');
  if (asset.media_semantic_status !== 'classified' || asset.media_role !== 'event_photo') {
    return contain('media_role_not_crop_eligible');
  }
  if (asset.safe_crop !== true) return contain('semantic_crop_gate_closed');
  if (!currentClassifiedGeometry(asset)) return contain('missing_or_stale_geometry');
  const valuable = validBox(asset.valuable_region) ? [asset.valuable_region] : [];
  const faces = Array.isArray(asset.face_boxes) ? asset.face_boxes.filter(validBox) : [];
  if (valuable.length === 0) return contain('missing_valuable_region');
  return solveProtectedCrop({
    sourceWidth:asset.width,
    sourceHeight:asset.height,
    targetAspect,
    boxes:[...faces, ...valuable],
    margin:options.margin,
  });
}

