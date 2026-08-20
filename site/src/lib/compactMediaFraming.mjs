const VERY_TALL_DOCUMENT_RATIO = 4 / 5;
const MAX_DOCUMENT_CROP = 0.2;
const EPSILON = 1e-9;

function finiteRatio(value, fallback = VERY_TALL_DOCUMENT_RATIO) {
  const ratio = Number(value);
  return Number.isFinite(ratio) && ratio > 0 ? ratio : fallback;
}

/**
 * Shared compact-card framing for explicitly classified document media.
 * Unknown/error media never receives inferred crop permission.
 */
export function resolveBoundedDocumentFrame({
  ratio,
  imageTextMode = 'unknown',
  semanticStatus = '',
  dimensionsKnown = false,
} = {}) {
  const sourceRatio = finiteRatio(ratio);
  const contained = (cropReason) => ({
    sourceRatio,
    targetRatio: sourceRatio,
    fit: 'contain',
    mediaTreatment: 'document-contain',
    cropReason,
    coverCrop: null,
    verticalRetention: 1,
  });

  if (semanticStatus === 'error') return contained('semantic_error_fail_closed');
  if (imageTextMode !== 'ocr_text') return contained('unknown_media_fail_closed');
  if (!dimensionsKnown) return contained('document_dimensions_unknown');

  const targetRatio = sourceRatio < VERY_TALL_DOCUMENT_RATIO - EPSILON
    ? sourceRatio / (1 - MAX_DOCUMENT_CROP)
    : sourceRatio;
  const coverCrop = Math.max(0, 1 - sourceRatio / targetRatio);
  return {
    sourceRatio,
    targetRatio,
    fit: 'cover',
    mediaTreatment: 'document-safe-cover',
    cropReason: coverCrop > EPSILON ? 'document_bounded_cover' : 'document_uncropped',
    coverCrop,
    verticalRetention: sourceRatio / targetRatio,
  };
}

export const compactDocumentFramingContract = Object.freeze({
  veryTallRatio: VERY_TALL_DOCUMENT_RATIO,
  maxCrop: MAX_DOCUMENT_CROP,
});
