import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import {
  createFocusInviteQrSvg,
  encodeFocusInviteQr,
  FOCUS_INVITE_QR_MAX_BYTES,
  FOCUS_INVITE_QR_QUIET_ZONE,
  FOCUS_INVITE_QR_SIZE,
  FOCUS_INVITE_QR_VERSION,
} from './focus-invite-qr.ts';

const BLOCK_DATA_LENGTHS = [43, 43, 43, 43, 44] as const;
const ALIGNMENT_PATTERN_POSITIONS = [6, 28, 50] as const;

const isMasked = (mask: number, x: number, y: number): boolean => {
  switch (mask) {
    case 0: return (x + y) % 2 === 0;
    case 1: return y % 2 === 0;
    case 2: return x % 3 === 0;
    case 3: return (x + y) % 3 === 0;
    case 4: return (Math.floor(y / 2) + Math.floor(x / 3)) % 2 === 0;
    case 5: return ((x * y) % 2) + ((x * y) % 3) === 0;
    case 6: return (((x * y) % 2) + ((x * y) % 3)) % 2 === 0;
    case 7: return (((x + y) % 2) + ((x * y) % 3)) % 2 === 0;
    default: throw new RangeError('Unexpected mask.');
  }
};

const makeFunctionMap = (): boolean[][] => {
  const map = Array.from({ length: FOCUS_INVITE_QR_SIZE }, () => (
    Array<boolean>(FOCUS_INVITE_QR_SIZE).fill(false)
  ));
  const reserve = (x: number, y: number): void => {
    if (x >= 0 && y >= 0 && x < FOCUS_INVITE_QR_SIZE && y < FOCUS_INVITE_QR_SIZE) {
      map[y][x] = true;
    }
  };
  const reserveSquare = (centerX: number, centerY: number, radius: number): void => {
    for (let dy = -radius; dy <= radius; dy += 1) {
      for (let dx = -radius; dx <= radius; dx += 1) reserve(centerX + dx, centerY + dy);
    }
  };

  for (let index = 0; index < FOCUS_INVITE_QR_SIZE; index += 1) {
    reserve(6, index);
    reserve(index, 6);
  }
  reserveSquare(3, 3, 4);
  reserveSquare(FOCUS_INVITE_QR_SIZE - 4, 3, 4);
  reserveSquare(3, FOCUS_INVITE_QR_SIZE - 4, 4);

  for (let row = 0; row < ALIGNMENT_PATTERN_POSITIONS.length; row += 1) {
    for (let column = 0; column < ALIGNMENT_PATTERN_POSITIONS.length; column += 1) {
      if (
        (row === 0 && column === 0)
        || (row === 0 && column === ALIGNMENT_PATTERN_POSITIONS.length - 1)
        || (row === ALIGNMENT_PATTERN_POSITIONS.length - 1 && column === 0)
      ) continue;
      reserveSquare(
        ALIGNMENT_PATTERN_POSITIONS[column],
        ALIGNMENT_PATTERN_POSITIONS[row],
        2,
      );
    }
  }

  for (let index = 0; index <= 5; index += 1) reserve(8, index);
  reserve(8, 7);
  reserve(8, 8);
  reserve(7, 8);
  for (let index = 9; index < 15; index += 1) reserve(14 - index, 8);
  for (let index = 0; index < 8; index += 1) reserve(FOCUS_INVITE_QR_SIZE - 1 - index, 8);
  for (let index = 8; index < 15; index += 1) reserve(8, FOCUS_INVITE_QR_SIZE - 15 + index);
  reserve(8, FOCUS_INVITE_QR_SIZE - 8);

  for (let index = 0; index < 18; index += 1) {
    const primary = FOCUS_INVITE_QR_SIZE - 11 + (index % 3);
    const secondary = Math.floor(index / 3);
    reserve(primary, secondary);
    reserve(secondary, primary);
  }
  return map;
};

/**
 * Minimal independent byte-mode reader for this encoder's fixed Version 10-M
 * symbol. It intentionally ignores ECC correction: tests read the perfect
 * generated matrix and verify its actual data-module traversal/interleaving.
 */
const decodePerfectMatrix = (
  modules: readonly (readonly boolean[])[],
  mask: number,
): string => {
  const isFunction = makeFunctionMap();
  const bits: number[] = [];
  let upward = true;
  for (let right = FOCUS_INVITE_QR_SIZE - 1; right >= 1; right -= 2) {
    if (right === 6) right -= 1;
    for (let vertical = 0; vertical < FOCUS_INVITE_QR_SIZE; vertical += 1) {
      const y = upward ? FOCUS_INVITE_QR_SIZE - 1 - vertical : vertical;
      for (let column = 0; column < 2; column += 1) {
        const x = right - column;
        if (isFunction[y][x]) continue;
        bits.push((modules[y][x] !== isMasked(mask, x, y)) ? 1 : 0);
      }
    }
    upward = !upward;
  }

  const interleaved = new Uint8Array(Math.floor(bits.length / 8));
  for (let index = 0; index < interleaved.length; index += 1) {
    for (let bit = 0; bit < 8; bit += 1) {
      interleaved[index] = (interleaved[index] << 1) | bits[index * 8 + bit];
    }
  }

  const blocks = BLOCK_DATA_LENGTHS.map(() => [] as number[]);
  let interleavedIndex = 0;
  for (let index = 0; index < Math.max(...BLOCK_DATA_LENGTHS); index += 1) {
    for (let block = 0; block < blocks.length; block += 1) {
      if (index < BLOCK_DATA_LENGTHS[block]) {
        blocks[block].push(interleaved[interleavedIndex]);
        interleavedIndex += 1;
      }
    }
  }
  const data = Uint8Array.from(blocks.flat());
  let bitOffset = 0;
  const readBits = (length: number): number => {
    let value = 0;
    for (let index = 0; index < length; index += 1) {
      value = (value << 1) | ((data[bitOffset >>> 3] >>> (7 - (bitOffset & 7))) & 1);
      bitOffset += 1;
    }
    return value;
  };

  assert.equal(readBits(4), 0b0100, 'payload must use QR byte mode');
  const byteLength = readBits(16);
  return new TextDecoder('utf-8', { fatal: true }).decode(
    Uint8Array.from({ length: byteLength }, () => readBits(8)),
  );
};

test('QR data modules decode to the exact fragment invite URL', () => {
  const exactInviteUrl = [
    'https://example.test/fokus-gruppa/priglashenie/',
    '#invite=abcdefghijklmnopqrstuvwxyz_ABCDEFG123456',
  ].join('');
  const qr = encodeFocusInviteQr(exactInviteUrl);

  assert.equal(FOCUS_INVITE_QR_VERSION, 10);
  assert.equal(qr.modules.length, FOCUS_INVITE_QR_SIZE);
  assert.ok(qr.modules.every((row) => row.length === FOCUS_INVITE_QR_SIZE));
  assert.equal(decodePerfectMatrix(qr.modules, qr.mask), exactInviteUrl);
});

test('QR byte mode preserves UTF-8 URLs and enforces its local capacity', () => {
  const unicodeUrl = 'https://example.test/fokus/#invite=Привет-Калининград';
  const qr = encodeFocusInviteQr(unicodeUrl);
  assert.equal(decodePerfectMatrix(qr.modules, qr.mask), unicodeUrl);

  assert.doesNotThrow(() => encodeFocusInviteQr('x'.repeat(FOCUS_INVITE_QR_MAX_BYTES)));
  assert.throws(
    () => encodeFocusInviteQr('x'.repeat(FOCUS_INVITE_QR_MAX_BYTES + 1)),
    /too long for the local QR/u,
  );
});

test('downloadable SVG is deterministic, self-contained and has a four-module quiet zone', () => {
  const exactInviteUrl = 'https://example.test/fokus/#invite=abcdefghijklmnop';
  const first = createFocusInviteQrSvg(exactInviteUrl);
  const second = createFocusInviteQrSvg(exactInviteUrl);
  const viewBoxSize = FOCUS_INVITE_QR_SIZE + FOCUS_INVITE_QR_QUIET_ZONE * 2;

  assert.equal(first, second);
  assert.match(first, new RegExp(`viewBox="0 0 ${viewBoxSize} ${viewBoxSize}"`, 'u'));
  assert.match(first, /shape-rendering="crispEdges"/u);
  assert.match(first, /<rect[^>]+fill="#fff"/u);
  assert.match(first, /<path[^>]+fill="#111"/u);
  assert.doesNotMatch(first, /(?:href|src)=["']https?:|<script|foreignObject/u);
  assert.doesNotMatch(first, new RegExp(exactInviteUrl, 'u'));
});

test('share surface uses one exact URL for display, open, share and local QR generation', async () => {
  const source = await readFile(
    new URL('../components/FocusGroupInviteShare.astro', import.meta.url),
    'utf8',
  );

  assert.match(source, /const exactInviteUrl = inviteUrl\.toString\(\)/u);
  assert.match(source, /createFocusInviteQrSvg\(exactInviteUrl\)/u);
  assert.match(source, /input\.value = exactInviteUrl/u);
  assert.match(source, /openLink\.href = exactInviteUrl/u);
  assert.match(source, /openLink\.textContent = exactInviteUrl/u);
  assert.match(source, /download="priglashenie-fokus-gruppa\.svg"/u);
  assert.match(source, /createButton\.click\(\)/u);
  assert.match(source, /Подтверждение участия — по желанию/u);
  assert.match(source, /Для участия в розыгрыше билетов нужно войти/u);
});
