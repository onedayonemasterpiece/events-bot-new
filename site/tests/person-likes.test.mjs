import assert from 'node:assert/strict';
import test from 'node:test';

import {
  getPersonLikeSnapshot,
  publicPersonIds,
  setPersonLike,
} from '../src/lib/personLikes.ts';

test('person ids are bounded, deduplicated and registry-shaped', () => {
  assert.deepEqual(
    publicPersonIds([
      'kgd80:tatyana-udovenko',
      ' kgd80:tatyana-udovenko ',
      'bad person',
      '',
    ]),
    ['kgd80:tatyana-udovenko'],
  );
  assert.equal(
    publicPersonIds(Array.from({ length: 80 }, (_, index) => `person:${index}`)).length,
    64,
  );
});

test('one snapshot RPC returns global counts and the current user state', async () => {
  const calls = [];
  const client = {
    async rpc(name, args) {
      calls.push([name, args]);
      return {
        data: [{
          person_id: 'kgd80:tatyana-udovenko',
          likes_count: 17,
          liked: true,
        }],
        error: null,
      };
    },
  };
  assert.deepEqual(
    await getPersonLikeSnapshot(client, ['kgd80:tatyana-udovenko']),
    [{ personId: 'kgd80:tatyana-udovenko', likesCount: 17, liked: true }],
  );
  assert.deepEqual(calls, [[
    'get_person_like_snapshot_v1',
    { p_person_ids: ['kgd80:tatyana-udovenko'] },
  ]]);
});

test('set RPC is idempotent desired state, not a blind toggle', async () => {
  const client = {
    async rpc(name, args) {
      assert.equal(name, 'set_person_like_v1');
      assert.deepEqual(args, {
        p_person_id: 'kgd80:tatyana-udovenko',
        p_liked: true,
      });
      return {
        data: [{
          person_id: 'kgd80:tatyana-udovenko',
          likes_count: 18,
          liked: true,
        }],
        error: null,
      };
    },
  };
  assert.deepEqual(
    await setPersonLike(client, 'kgd80:tatyana-udovenko', true),
    { personId: 'kgd80:tatyana-udovenko', likesCount: 18, liked: true },
  );
});
