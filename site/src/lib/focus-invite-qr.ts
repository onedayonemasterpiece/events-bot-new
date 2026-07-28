/**
 * A deliberately small, dependency-free QR encoder for focus-group invite URLs.
 *
 * It emits a fixed Version 10 / Medium error-correction QR symbol. Keeping the
 * symbol version fixed makes the output deterministic and keeps this prototype
 * independent from remote QR services. Version 10-M safely carries up to 213
 * UTF-8 bytes in byte mode.
 */

export const FOCUS_INVITE_QR_VERSION = 10;
export const FOCUS_INVITE_QR_SIZE = 57;
export const FOCUS_INVITE_QR_MAX_BYTES = 213;
export const FOCUS_INVITE_QR_QUIET_ZONE = 4;

const DATA_CODEWORDS = 216;
const ECC_CODEWORDS_PER_BLOCK = 26;
const BLOCK_DATA_LENGTHS = [43, 43, 43, 43, 44] as const;
const ALIGNMENT_PATTERN_POSITIONS = [6, 28, 50] as const;

export interface FocusInviteQr {
  modules: readonly (readonly boolean[])[];
  mask: number;
}

const appendBits = (target: number[], value: number, length: number): void => {
  if (length < 0 || length > 31 || value >>> length !== 0) {
    throw new RangeError('Invalid QR bit sequence.');
  }
  for (let bit = length - 1; bit >= 0; bit -= 1) {
    target.push(((value >>> bit) & 1) !== 0 ? 1 : 0);
  }
};

const multiplyGalois = (left: number, right: number): number => {
  let result = 0;
  for (let bit = 7; bit >= 0; bit -= 1) {
    result = (result << 1) ^ ((result >>> 7) * 0x11d);
    result ^= ((right >>> bit) & 1) * left;
  }
  return result;
};

const makeReedSolomonDivisor = (degree: number): Uint8Array => {
  const result = new Uint8Array(degree);
  result[degree - 1] = 1;
  let root = 1;
  for (let index = 0; index < degree; index += 1) {
    for (let coefficient = 0; coefficient < result.length; coefficient += 1) {
      result[coefficient] = multiplyGalois(result[coefficient], root);
      if (coefficient + 1 < result.length) {
        result[coefficient] ^= result[coefficient + 1];
      }
    }
    root = multiplyGalois(root, 0x02);
  }
  return result;
};

const makeReedSolomonRemainder = (
  data: Uint8Array,
  divisor: Uint8Array,
): Uint8Array => {
  const result = new Uint8Array(divisor.length);
  for (const byte of data) {
    const factor = byte ^ result[0];
    result.copyWithin(0, 1);
    result[result.length - 1] = 0;
    for (let index = 0; index < result.length; index += 1) {
      result[index] ^= multiplyGalois(divisor[index], factor);
    }
  }
  return result;
};

const encodeDataCodewords = (value: string): Uint8Array => {
  const bytes = new TextEncoder().encode(value);
  if (bytes.length > FOCUS_INVITE_QR_MAX_BYTES) {
    throw new RangeError(
      `Focus invite URL is too long for the local QR (${bytes.length}/${FOCUS_INVITE_QR_MAX_BYTES} bytes).`,
    );
  }

  const bits: number[] = [];
  appendBits(bits, 0b0100, 4); // Byte mode.
  appendBits(bits, bytes.length, 16); // Version 10 uses a 16-bit byte count.
  for (const byte of bytes) appendBits(bits, byte, 8);

  const capacityBits = DATA_CODEWORDS * 8;
  appendBits(bits, 0, Math.min(4, capacityBits - bits.length));
  appendBits(bits, 0, (8 - (bits.length % 8)) % 8);

  const result = new Uint8Array(DATA_CODEWORDS);
  let resultLength = 0;
  for (let offset = 0; offset < bits.length; offset += 8) {
    let byte = 0;
    for (let bit = 0; bit < 8; bit += 1) byte = (byte << 1) | bits[offset + bit];
    result[resultLength] = byte;
    resultLength += 1;
  }
  for (let pad = 0; resultLength < result.length; pad += 1) {
    result[resultLength] = pad % 2 === 0 ? 0xec : 0x11;
    resultLength += 1;
  }
  return result;
};

const addErrorCorrectionAndInterleave = (data: Uint8Array): Uint8Array => {
  const divisor = makeReedSolomonDivisor(ECC_CODEWORDS_PER_BLOCK);
  const dataBlocks: Uint8Array[] = [];
  const eccBlocks: Uint8Array[] = [];
  let offset = 0;

  for (const length of BLOCK_DATA_LENGTHS) {
    const block = data.slice(offset, offset + length);
    offset += length;
    dataBlocks.push(block);
    eccBlocks.push(makeReedSolomonRemainder(block, divisor));
  }

  const result = new Uint8Array(DATA_CODEWORDS + ECC_CODEWORDS_PER_BLOCK * BLOCK_DATA_LENGTHS.length);
  let resultIndex = 0;
  for (let index = 0; index < Math.max(...BLOCK_DATA_LENGTHS); index += 1) {
    for (const block of dataBlocks) {
      if (index < block.length) {
        result[resultIndex] = block[index];
        resultIndex += 1;
      }
    }
  }
  for (let index = 0; index < ECC_CODEWORDS_PER_BLOCK; index += 1) {
    for (const block of eccBlocks) {
      result[resultIndex] = block[index];
      resultIndex += 1;
    }
  }
  return result;
};

type MutableMatrix = Array<Array<boolean | null>>;

interface MatrixTemplate {
  modules: MutableMatrix;
  isFunction: boolean[][];
}

const createMatrixTemplate = (): MatrixTemplate => {
  const modules: MutableMatrix = Array.from(
    { length: FOCUS_INVITE_QR_SIZE },
    () => Array<boolean | null>(FOCUS_INVITE_QR_SIZE).fill(null),
  );
  const isFunction = Array.from(
    { length: FOCUS_INVITE_QR_SIZE },
    () => Array<boolean>(FOCUS_INVITE_QR_SIZE).fill(false),
  );

  const setFunction = (x: number, y: number, dark: boolean): void => {
    if (x < 0 || y < 0 || x >= FOCUS_INVITE_QR_SIZE || y >= FOCUS_INVITE_QR_SIZE) return;
    modules[y][x] = dark;
    isFunction[y][x] = true;
  };

  const drawFinder = (centerX: number, centerY: number): void => {
    for (let dy = -4; dy <= 4; dy += 1) {
      for (let dx = -4; dx <= 4; dx += 1) {
        const distance = Math.max(Math.abs(dx), Math.abs(dy));
        setFunction(centerX + dx, centerY + dy, distance !== 2 && distance !== 4);
      }
    }
  };

  const drawAlignment = (centerX: number, centerY: number): void => {
    for (let dy = -2; dy <= 2; dy += 1) {
      for (let dx = -2; dx <= 2; dx += 1) {
        setFunction(
          centerX + dx,
          centerY + dy,
          Math.max(Math.abs(dx), Math.abs(dy)) !== 1,
        );
      }
    }
  };

  for (let index = 0; index < FOCUS_INVITE_QR_SIZE; index += 1) {
    setFunction(6, index, index % 2 === 0);
    setFunction(index, 6, index % 2 === 0);
  }
  drawFinder(3, 3);
  drawFinder(FOCUS_INVITE_QR_SIZE - 4, 3);
  drawFinder(3, FOCUS_INVITE_QR_SIZE - 4);

  for (let row = 0; row < ALIGNMENT_PATTERN_POSITIONS.length; row += 1) {
    for (let column = 0; column < ALIGNMENT_PATTERN_POSITIONS.length; column += 1) {
      const isFinderCorner = (
        (row === 0 && column === 0)
        || (row === 0 && column === ALIGNMENT_PATTERN_POSITIONS.length - 1)
        || (row === ALIGNMENT_PATTERN_POSITIONS.length - 1 && column === 0)
      );
      if (!isFinderCorner) {
        drawAlignment(
          ALIGNMENT_PATTERN_POSITIONS[column],
          ALIGNMENT_PATTERN_POSITIONS[row],
        );
      }
    }
  }

  // Reserve format/version areas by drawing valid placeholder bits.
  drawFormatBits(modules, isFunction, 0);
  drawVersionBits(modules, isFunction);
  return { modules, isFunction };
};

const setReserved = (
  modules: MutableMatrix,
  isFunction: boolean[][],
  x: number,
  y: number,
  dark: boolean,
): void => {
  modules[y][x] = dark;
  isFunction[y][x] = true;
};

const drawFormatBits = (
  modules: MutableMatrix,
  isFunction: boolean[][],
  mask: number,
): void => {
  // Medium ECC has the two-bit format value 00.
  const data = mask;
  let remainder = data;
  for (let bit = 0; bit < 10; bit += 1) {
    remainder = (remainder << 1) ^ (((remainder >>> 9) & 1) * 0x537);
  }
  const formatBits = ((data << 10) | remainder) ^ 0x5412;
  const bit = (index: number) => ((formatBits >>> index) & 1) !== 0;

  for (let index = 0; index <= 5; index += 1) setReserved(modules, isFunction, 8, index, bit(index));
  setReserved(modules, isFunction, 8, 7, bit(6));
  setReserved(modules, isFunction, 8, 8, bit(7));
  setReserved(modules, isFunction, 7, 8, bit(8));
  for (let index = 9; index < 15; index += 1) {
    setReserved(modules, isFunction, 14 - index, 8, bit(index));
  }

  for (let index = 0; index < 8; index += 1) {
    setReserved(modules, isFunction, FOCUS_INVITE_QR_SIZE - 1 - index, 8, bit(index));
  }
  for (let index = 8; index < 15; index += 1) {
    setReserved(
      modules,
      isFunction,
      8,
      FOCUS_INVITE_QR_SIZE - 15 + index,
      bit(index),
    );
  }
  setReserved(modules, isFunction, 8, FOCUS_INVITE_QR_SIZE - 8, true);
};

const drawVersionBits = (
  modules: MutableMatrix,
  isFunction: boolean[][],
): void => {
  let remainder = FOCUS_INVITE_QR_VERSION;
  for (let bit = 0; bit < 12; bit += 1) {
    remainder = (remainder << 1) ^ (((remainder >>> 11) & 1) * 0x1f25);
  }
  const versionBits = (FOCUS_INVITE_QR_VERSION << 12) | remainder;
  for (let index = 0; index < 18; index += 1) {
    const dark = ((versionBits >>> index) & 1) !== 0;
    const primary = FOCUS_INVITE_QR_SIZE - 11 + (index % 3);
    const secondary = Math.floor(index / 3);
    setReserved(modules, isFunction, primary, secondary, dark);
    setReserved(modules, isFunction, secondary, primary, dark);
  }
};

const placeCodewords = (
  modules: MutableMatrix,
  isFunction: boolean[][],
  codewords: Uint8Array,
): void => {
  let bitIndex = 0;
  let upward = true;
  for (let right = FOCUS_INVITE_QR_SIZE - 1; right >= 1; right -= 2) {
    if (right === 6) right -= 1;
    for (let vertical = 0; vertical < FOCUS_INVITE_QR_SIZE; vertical += 1) {
      const y = upward ? FOCUS_INVITE_QR_SIZE - 1 - vertical : vertical;
      for (let column = 0; column < 2; column += 1) {
        const x = right - column;
        if (isFunction[y][x]) continue;
        const dark = bitIndex < codewords.length * 8
          && ((codewords[bitIndex >>> 3] >>> (7 - (bitIndex & 7))) & 1) !== 0;
        modules[y][x] = dark;
        bitIndex += 1;
      }
    }
    upward = !upward;
  }
  if (bitIndex !== codewords.length * 8) {
    throw new Error('QR matrix does not match its encoded payload.');
  }
};

const maskCondition = (mask: number, x: number, y: number): boolean => {
  switch (mask) {
    case 0: return (x + y) % 2 === 0;
    case 1: return y % 2 === 0;
    case 2: return x % 3 === 0;
    case 3: return (x + y) % 3 === 0;
    case 4: return (Math.floor(y / 2) + Math.floor(x / 3)) % 2 === 0;
    case 5: return ((x * y) % 2) + ((x * y) % 3) === 0;
    case 6: return (((x * y) % 2) + ((x * y) % 3)) % 2 === 0;
    case 7: return (((x + y) % 2) + ((x * y) % 3)) % 2 === 0;
    default: throw new RangeError('Invalid QR mask.');
  }
};

const applyMask = (
  modules: MutableMatrix,
  isFunction: boolean[][],
  mask: number,
): void => {
  for (let y = 0; y < FOCUS_INVITE_QR_SIZE; y += 1) {
    for (let x = 0; x < FOCUS_INVITE_QR_SIZE; x += 1) {
      if (!isFunction[y][x] && maskCondition(mask, x, y)) {
        modules[y][x] = !modules[y][x];
      }
    }
  }
};

const penaltyScore = (modules: MutableMatrix): number => {
  let score = 0;
  const scanLine = (line: readonly (boolean | null)[]): void => {
    let runLength = 1;
    for (let index = 1; index < line.length; index += 1) {
      if (line[index] === line[index - 1]) {
        runLength += 1;
        if (runLength === 5) score += 3;
        else if (runLength > 5) score += 1;
      } else {
        runLength = 1;
      }
    }
    const pattern = line.map((module) => (module ? '1' : '0')).join('');
    for (let index = 0; index <= pattern.length - 11; index += 1) {
      const window = pattern.slice(index, index + 11);
      if (window === '00001011101' || window === '10111010000') score += 40;
    }
  };

  for (let index = 0; index < FOCUS_INVITE_QR_SIZE; index += 1) {
    scanLine(modules[index]);
    scanLine(modules.map((row) => row[index]));
  }
  for (let y = 0; y < FOCUS_INVITE_QR_SIZE - 1; y += 1) {
    for (let x = 0; x < FOCUS_INVITE_QR_SIZE - 1; x += 1) {
      const module = modules[y][x];
      if (
        module === modules[y][x + 1]
        && module === modules[y + 1][x]
        && module === modules[y + 1][x + 1]
      ) {
        score += 3;
      }
    }
  }
  const darkModules = modules.flat().filter(Boolean).length;
  score += Math.floor(
    Math.abs(darkModules * 20 - FOCUS_INVITE_QR_SIZE * FOCUS_INVITE_QR_SIZE * 10)
      / (FOCUS_INVITE_QR_SIZE * FOCUS_INVITE_QR_SIZE),
  ) * 10;
  return score;
};

export function encodeFocusInviteQr(value: string): FocusInviteQr {
  const data = encodeDataCodewords(value);
  const codewords = addErrorCorrectionAndInterleave(data);
  const template = createMatrixTemplate();
  placeCodewords(template.modules, template.isFunction, codewords);

  let bestMask = 0;
  let bestModules: MutableMatrix | null = null;
  let bestPenalty = Number.POSITIVE_INFINITY;
  for (let mask = 0; mask < 8; mask += 1) {
    const candidate = template.modules.map((row) => [...row]);
    applyMask(candidate, template.isFunction, mask);
    drawFormatBits(candidate, template.isFunction, mask);
    const penalty = penaltyScore(candidate);
    if (penalty < bestPenalty) {
      bestMask = mask;
      bestModules = candidate;
      bestPenalty = penalty;
    }
  }
  if (!bestModules) throw new Error('Unable to create QR matrix.');
  return {
    mask: bestMask,
    modules: bestModules.map((row) => row.map(Boolean)),
  };
}

export function createFocusInviteQrSvg(value: string): string {
  const { modules } = encodeFocusInviteQr(value);
  const viewBoxSize = FOCUS_INVITE_QR_SIZE + FOCUS_INVITE_QR_QUIET_ZONE * 2;
  const darkPath = modules
    .flatMap((row, y) => row.flatMap((dark, x) => (
      dark ? `M${x + FOCUS_INVITE_QR_QUIET_ZONE},${y + FOCUS_INVITE_QR_QUIET_ZONE}h1v1h-1z` : []
    )))
    .join('');

  return [
    `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${viewBoxSize} ${viewBoxSize}"`,
    ' role="img" aria-labelledby="focus-invite-qr-title" shape-rendering="crispEdges">',
    '<title id="focus-invite-qr-title">QR-код приглашения в фокус-группу</title>',
    `<rect width="${viewBoxSize}" height="${viewBoxSize}" fill="#fff"/>`,
    `<path d="${darkPath}" fill="#111"/>`,
    '</svg>',
  ].join('');
}
