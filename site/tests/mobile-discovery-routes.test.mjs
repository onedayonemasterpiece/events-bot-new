import assert from 'node:assert/strict';
import test from 'node:test';

import { mobileDiscoveryHref } from '../src/lib/mobileDiscoveryRoutes.ts';

const bases = {
  calendarBase:'https://kenigevents.ru/preview-20260721-mobile-calendar-v23/',
  searchBase:'https://kenigevents.ru/preview-20260721-mobile-search-runtime-v24/',
};

test('mobile discovery composition keeps accepted calendar v23 and current Search v24', () => {
  assert.equal(mobileDiscoveryHref('/populyarnoe/', bases), 'https://kenigevents.ru/preview-20260721-mobile-calendar-v23/populyarnoe/');
  assert.equal(mobileDiscoveryHref('/segodnya/', bases), 'https://kenigevents.ru/preview-20260721-mobile-calendar-v23/segodnya/');
  assert.equal(mobileDiscoveryHref('/dlya-menya/', bases), 'https://kenigevents.ru/preview-20260721-mobile-calendar-v23/dlya-menya/');
  assert.equal(mobileDiscoveryHref('/poisk/', bases), 'https://kenigevents.ru/preview-20260721-mobile-search-runtime-v24/poisk/');
  assert.equal(
    mobileDiscoveryHref('/podborki/besplatnye-sobytiya/', bases),
    'https://kenigevents.ru/preview-20260721-mobile-search-runtime-v24/podborki/besplatnye-sobytiya/',
  );
});

test('mobile drawer calendar routes share the same resolver while unrelated routes stay local', () => {
  assert.equal(mobileDiscoveryHref('/zavtra/', bases), 'https://kenigevents.ru/preview-20260721-mobile-calendar-v23/zavtra/');
  assert.equal(mobileDiscoveryHref('/vyhodnye/', bases), 'https://kenigevents.ru/preview-20260721-mobile-calendar-v23/vyhodnye/');
  assert.match(mobileDiscoveryHref('/partners/', bases), /\/partners\/$/u);
});
