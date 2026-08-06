import assert from 'node:assert/strict';
import test from 'node:test';

import { normalizePrelaunchEmail } from './prelaunchEmail.ts';

test('normalizes ordinary addresses and plus-tags', () => {
  assert.deepEqual(
    normalizePrelaunchEmail('  User.Name+Launch@Example.COM  '),
    { ok: true, email: 'user.name+launch@example.com' },
  );
  assert.deepEqual(
    normalizePrelaunchEmail('name_o-neil@example-domain.co.uk'),
    { ok: true, email: 'name_o-neil@example-domain.co.uk' },
  );
  assert.deepEqual(
    normalizePrelaunchEmail('person@xn--e1afmkfd.xn--p1ai'),
    { ok: true, email: 'person@xn--e1afmkfd.xn--p1ai' },
  );
});

test('rejects malformed local and domain syntax', () => {
  for (const value of [
    '',
    'a@b.c',
    '.person@example.com',
    'person.@example.com',
    'per..son@example.com',
    'person@example..com',
    'person@-example.com',
    'person@example-.com',
    'person@@example.com',
    'person@example',
  ]) {
    assert.equal(normalizePrelaunchEmail(value).ok, false, value);
  }
});

test('rejects markup, header injection and code-like payloads before transport', () => {
  for (const value of [
    '<script>@example.com',
    'person\r\nbcc@example.com',
    'person@example.com\n',
    '"quoted"@example.com',
    "x');drop-table@example.com",
    'person\\payload@example.com',
    'тест@example.com',
  ]) {
    assert.equal(normalizePrelaunchEmail(value).ok, false, value);
  }
});

test('enforces RFC length ceilings used by the database contract', () => {
  const longLocal = `${'a'.repeat(65)}@example.com`;
  const longDomain = `a@${'b'.repeat(64)}.com`;
  const totalTooLong = `${'a'.repeat(64)}@${'b'.repeat(60)}.${'c'.repeat(60)}.${'d'.repeat(60)}.com`;
  assert.equal(normalizePrelaunchEmail(longLocal).ok, false);
  assert.equal(normalizePrelaunchEmail(longDomain).ok, false);
  assert.equal(normalizePrelaunchEmail(totalTooLong).ok, false);
});
