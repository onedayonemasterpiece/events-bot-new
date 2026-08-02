import assert from 'node:assert/strict';
import test from 'node:test';

import { classifySafariInspection, SafariFirstRunUiError, stabilizeSafariSystemUi } from '../../e2e/focus-email/helpers/safari-system-ui.mjs';

function clock() {
  let value = 0;
  return { now: () => value, wait: async (ms) => { value += ms; } };
}

test('inspection accepts only an action already scoped to the exact modal', () => {
  assert.deepEqual(classifySafariInspection({ titleCount: 1, scopedActions: [], topLevelCount: 0 }), {
    known_dialog_count: 1, continue_button_count: 0, blocking_dialog_count: 1,
    unknown_blocking_dialog_count: 0, action_token: null,
  });
  assert.equal(classifySafariInspection({ titleCount: 1, scopedActions: ['same-modal-action'] }).action_token, 'same-modal-action');
});

test('a separate unknown top-level dialog cannot be cancelled by a known title in Other', () => {
  const state = classifySafariInspection({ titleCount: 1, scopedActions: ['same-modal-action'], topLevelCount: 1, knownTopLevelCount: 0 });
  assert.equal(state.unknown_blocking_dialog_count, 1);
  assert.equal(state.blocking_dialog_count, 2);
});

test('Safari startup waits for a delayed exact dialog and verifies stable disappearance', async () => {
  const time = clock();
  let scans = 0;
  let clicked = 0;
  const evidence = await stabilizeSafariSystemUi({
    ...time,
    discoveryTimeoutMs: 1_000,
    intervalMs: 100,
    stableAbsentSamples: 2,
    inspect: async () => {
      scans += 1;
      if (scans < 3) return { blocking_dialog_count: 0, known_dialog_count: 0, continue_button_count: 0 };
      if (!clicked) return { blocking_dialog_count: 1, known_dialog_count: 1, continue_button_count: 1, action_token: 'opaque' };
      return { blocking_dialog_count: 0, known_dialog_count: 0, continue_button_count: 0 };
    },
    dismissKnownDialog: async (token) => { assert.equal(token, 'opaque'); clicked += 1; },
  });
  assert.equal(evidence.seen, true);
  assert.equal(evidence.dismissed, true);
  assert.equal(evidence.obstruction_free, true);
  assert.equal(evidence.attempts, 1);
  assert.deepEqual(evidence.last_inspection, { known_dialog_count: 0, continue_button_count: 0,
    blocking_dialog_count: 0, unknown_blocking_dialog_count: 0 });
});

test('Safari startup accepts a bounded observation window with no modal', async () => {
  const time = clock();
  const evidence = await stabilizeSafariSystemUi({ ...time, discoveryTimeoutMs: 300, intervalMs: 100,
    inspect: async () => ({ blocking_dialog_count: 0, known_dialog_count: 0, continue_button_count: 0 }), dismissKnownDialog: async () => {} });
  assert.equal(evidence.seen, false);
  assert.equal(evidence.obstruction_free, true);
});

test('Safari startup retains only allowlisted numeric and boolean contract probes', async () => {
  const time = clock();
  const contractProbe = {
    exact_visible_static_text_count: 0,
    exact_static_text_count: 0,
    containing_static_text_count: 1,
    exact_any_element_count: 1,
    current_alert_present: true,
    alert_text_length: 35,
    alert_text_line_count: 1,
    exact_title_line_count: 0,
    title_substring_count: 1,
    alert_button_count: 2,
    exact_continue_button_count: 1,
    exact_settings_button_count: 1,
  };
  await assert.rejects(() => stabilizeSafariSystemUi({ ...time,
    inspect: async () => ({ blocking_dialog_count: 1, unknown_blocking_dialog_count: 1,
      known_dialog_count: 0, continue_button_count: 0, contract_probe: contractProbe }),
    dismissKnownDialog: async () => {},
  }), (error) => {
    assert.deepEqual(error.evidence.last_inspection.contract_probe, contractProbe);
    assert.doesNotMatch(JSON.stringify(error.evidence), /Выбор|Продолжить|Настройки/u);
    return true;
  });
});

for (const fixture of [
  { name: 'unknown modal', state: { blocking_dialog_count: 1, unknown_blocking_dialog_count: 1, known_dialog_count: 0, continue_button_count: 0 }, code: /unexpected_blocking_modal/u },
  { name: 'ambiguous dialog', state: { blocking_dialog_count: 2, known_dialog_count: 2, continue_button_count: 2 }, code: /ambiguous/u },
  { name: 'missing action', state: { blocking_dialog_count: 1, known_dialog_count: 1, continue_button_count: 0 }, code: /action_missing/u },
]) {
  test(`Safari startup blocks on ${fixture.name}`, async () => {
    const time = clock();
    await assert.rejects(() => stabilizeSafariSystemUi({ ...time, discoveryTimeoutMs: 100,
      inspect: async () => fixture.state, dismissKnownDialog: async () => {} }), fixture.code);
  });
}

test('Safari startup blocks when the exact dialog remains after one dismissal', async () => {
  const time = clock();
  let clicks = 0;
  await assert.rejects(() => stabilizeSafariSystemUi({ ...time, discoveryTimeoutMs: 100, dismissalTimeoutMs: 300, intervalMs: 100,
    inspect: async () => ({ blocking_dialog_count: 1, known_dialog_count: 1, continue_button_count: 1, action_token: 'opaque' }),
    dismissKnownDialog: async () => { clicks += 1; } }), (error) => {
    assert.ok(error instanceof SafariFirstRunUiError);
    assert.match(error.message, /dialog_stuck/u);
    assert.equal(error.evidence.attempts, 1);
    return true;
  });
  assert.equal(clicks, 1);
});
