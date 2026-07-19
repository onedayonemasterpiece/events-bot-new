const DEFAULT_DOCUMENT_RATIO = 4 / 5;
const MIN_VISUAL_RATIO = 1;
const MAX_VISUAL_RATIO = 4 / 3;
const MAX_DOCUMENT_CROP = 0.2;

function finiteRatio(value, fallback) {
  const ratio = Number(value);
  return Number.isFinite(ratio) && ratio > 0 ? ratio : fallback;
}

export function relatedCardMediaGeometry(item) {
  const display = item?.candidate?.display || item?.display || item || {};
  const imageTextMode = display.image_text_mode || item?.candidate?.image_text_mode || item?.image_text_mode || 'unknown';
  const documentMedia = imageTextMode !== 'visual_only';
  const width = Number(display.image_width || item?.candidate?.image_width || item?.image_width || 0);
  const height = Number(display.image_height || item?.candidate?.image_height || item?.image_height || 0);
  return {
    documentMedia,
    ratio: finiteRatio(width > 0 && height > 0 ? width / height : 0, documentMedia ? DEFAULT_DOCUMENT_RATIO : 1),
  };
}

function rowLayout(row, rowIndex, mediaTreatment) {
  const documents = row.filter(({ documentMedia }) => documentMedia);
  const visuals = row.filter(({ documentMedia }) => !documentMedia);
  const documentRatios = documents.map(({ ratio }) => ratio);
  const visualRatios = visuals.map(({ ratio }) => ratio);
  let targetRatio = 1;
  let rowMode = 'visual-square';

  if (documents.length === 0) {
    const geometricMean = visualRatios.length
      ? Math.exp(visualRatios.reduce((sum, ratio) => sum + Math.log(ratio), 0) / visualRatios.length)
      : 1;
    targetRatio = Math.min(MAX_VISUAL_RATIO, Math.max(MIN_VISUAL_RATIO, geometricMean));
    rowMode = 'visual-squareish';
  } else if (visuals.length > 0) {
    if (mediaTreatment === 'cover') {
      targetRatio = 1.12;
      rowMode = 'mixed-compact-cover';
    } else if (mediaTreatment === 'ambient') {
      targetRatio = 1;
      rowMode = 'mixed-square-ambient';
    } else {
      const documentCenter = Math.exp(documentRatios.reduce((sum, ratio) => sum + Math.log(ratio), 0) / documentRatios.length);
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

  const safeLower = documentRatios.length
    ? Math.max(...documentRatios.map((ratio) => ratio * (1 - MAX_DOCUMENT_CROP)))
    : 0;
  const safeUpper = documentRatios.length
    ? Math.min(...documentRatios.map((ratio) => ratio / (1 - MAX_DOCUMENT_CROP)))
    : Number.POSITIVE_INFINITY;
  const sharedSafeCover = safeLower <= safeUpper;
  if (sharedSafeCover && documentRatios.length) {
    targetRatio = Math.min(safeUpper, Math.max(safeLower, targetRatio));
  } else if (documentRatios.length > 1) {
    rowMode = `${rowMode}-contained-overflow`;
  }

  const entries = row.map(({ item, documentMedia, ratio: mediaRatio }) => {
    const potentialCoverCrop = Math.max(0, 1 - Math.min(mediaRatio / targetRatio, targetRatio / mediaRatio));
    const mustContain = documentMedia && potentialCoverCrop > MAX_DOCUMENT_CROP + 1e-9;
    return {
      item,
      layout: {
        mediaRatio,
        rowRatio: targetRatio,
        rowIndex,
        rowMode,
        mediaKind: documentMedia ? 'document' : 'visual',
        mediaTreatment: documentMedia
          ? (mustContain ? 'document-contain' : 'document-safe-cover')
          : 'visual-cover',
        coverCrop: mustContain ? 0 : potentialCoverCrop,
        potentialCoverCrop,
        rowWorstCrop: 0,
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
