import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import { extractSingleOtp, parseOtpMessage } from '../../e2e/focus-email/helpers/otp-message-parser.mjs';

const fixture = (name) => readFile(new URL(`../../e2e/focus-email/fixtures/${name}`, import.meta.url));

test('parses plain, HTML, base64 and quoted-printable OTP mail', async () => {
  const expected = new Map([
    ['otp-plain.eml', '123456'],
    ['otp-html.eml', '654321'],
    ['otp-base64.eml', '012345'],
    ['otp-quoted-printable.eml', '456789'],
  ]);
  for (const [name, otp] of expected) assert.equal((await parseOtpMessage(await fixture(name))).otp, otp);
});

test('allows the same code in text and HTML but rejects none or distinct candidates', () => {
  assert.equal(extractSingleOtp({ text: 'Код 123456', html: '<b>123456</b>' }), '123456');
  assert.throws(() => extractSingleOtp({ text: 'Нет кода' }), /otp_missing/u);
  assert.throws(() => extractSingleOtp({ text: '123456 и 654321' }), /otp_ambiguous/u);
});

test('rejects malformed mail with multiple or no visible codes', async () => {
  await assert.rejects(async () => parseOtpMessage(await fixture('otp-multiple-codes.eml')), /otp_ambiguous/u);
  await assert.rejects(async () => parseOtpMessage(await fixture('otp-no-code.eml')), /otp_missing/u);
});
