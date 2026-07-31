import assert from 'node:assert/strict';
import test from 'node:test';

import { focusGroupPageFamily, focusGroupPageLabel } from './focus-group-surface.ts';

test('focus feedback covers the user-facing page families', () => {
  const cases = new Map<string, string>([
    ['/', 'home'],
    ['/segodnya/', 'today'],
    ['/zavtra/', 'tomorrow'],
    ['/date-2026-08-08/', 'calendar_date'],
    ['/vyhodnye/2026-08-08/', 'weekend'],
    ['/populyarnoe/', 'popular'],
    ['/poisk/', 'search'],
    ['/podborki/besplatno/', 'collections'],
    ['/festivali/', 'festivals'],
    ['/kluby-po-interesam/', 'clubs'],
    ['/kluby-po-interesam/game-vibes/', 'club_detail'],
    ['/sobytiya/example-1/', 'event_detail'],
    ['/vystavki/', 'exhibitions'],
    ['/neobychnoe/', 'unusual'],
    ['/izbrannoe/', 'favorites'],
    ['/dlya-menya/', 'for_me'],
  ]);
  for (const [path, family] of cases) {
    assert.equal(focusGroupPageFamily(path), family, path);
  }
});

test('focus feedback accepts immutable candidate prefixes and excludes programme pages', () => {
  assert.equal(
    focusGroupPageFamily('/_review/abc_DEF123/sobytiya/example/'),
    'event_detail',
  );
  assert.equal(focusGroupPageFamily('/preview-r15/kluby-po-interesam/'), 'clubs');
  for (const path of ['/fokus-gruppa/', '/zakrytaya-afisha/', '/lab/medallions/', '/partners/']) {
    assert.equal(focusGroupPageFamily(path), null, path);
  }
});

test('focus feedback page labels stay user-facing', () => {
  assert.equal(focusGroupPageLabel('event_detail'), 'страницу события');
  assert.equal(focusGroupPageLabel('home'), 'главную');
});
