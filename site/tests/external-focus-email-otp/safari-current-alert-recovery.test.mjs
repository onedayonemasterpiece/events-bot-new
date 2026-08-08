import assert from 'node:assert/strict';
import test from 'node:test';

import { SafariFirstRunUiError, stabilizeSafariSystemUi } from '../../e2e/focus-email/helpers/safari-system-ui.mjs';

const clearState = () => ({
  known_dialog_count: 0,
  continue_button_count: 0,
  blocking_dialog_count: 0,
  unknown_blocking_dialog_count: 0,
  action_token: null,
});

const boundedSearchChoiceState = ({ titleSignal = 1, buttonCount = 2 } = {}) => ({
  known_dialog_count: 0,
  continue_button_count: 0,
  blocking_dialog_count: 1,
  unknown_blocking_dialog_count: 1,
  action_token: null,
  contract_probe: {
    exact_visible_static_text_count: 0,
    exact_static_text_count: 0,
    containing_static_text_count: titleSignal,
    exact_any_element_count: 0,
    current_alert_present: true,
    alert_text_length: 120,
    alert_text_line_count: 3,
    exact_title_line_count: 0,
    title_substring_count: titleSignal,
    alert_button_count: buttonCount,
    exact_continue_button_count: 1,
    exact_settings_button_count: 1,
  },
});

function deterministicClock() {
  let value = 0;
  return {
    now: () => value,
    wait: async (ms) => { value += ms; },
  };
}

test('stable bounded Safari current alert receives one exact Continue action and must disappear', async () => {
  const clock = deterministicClock();
  const states = [
    boundedSearchChoiceState(),
    boundedSearchChoiceState(),
    boundedSearchChoiceState(),
    clearState(),
    clearState(),
    clearState(),
  ];
  const actions = [];
  const evidence = await stabilizeSafariSystemUi({
    inspect: async () => states.shift() || clearState(),
    dismissKnownDialog: async (label) => { actions.push(label); },
    wait: clock.wait,
    now: clock.now,
    intervalMs: 10,
    discoveryTimeoutMs: 200,
    dismissalTimeoutMs: 200,
  });

  assert.deepEqual(actions, ['Продолжить']);
  assert.equal(evidence.seen, true);
  assert.equal(evidence.dismissed, true);
  assert.equal(evidence.obstruction_free, true);
  assert.equal(evidence.attempts, 1);
  assert.match(evidence.discovery_route, /^current_alert_exact_action_/u);
  assert.equal(evidence.transitions.at(-1), 'dismissal_verified');
});

test('stable exact Safari button pair can recover when WDA exposes no title element', async () => {
  const clock = deterministicClock();
  const states = [
    boundedSearchChoiceState({ titleSignal: 0 }),
    boundedSearchChoiceState({ titleSignal: 0 }),
    clearState(),
    clearState(),
    clearState(),
  ];
  const actions = [];
  const evidence = await stabilizeSafariSystemUi({
    inspect: async () => states.shift() || clearState(),
    dismissKnownDialog: async (label) => { actions.push(label); },
    wait: clock.wait,
    now: clock.now,
    intervalMs: 10,
    discoveryTimeoutMs: 200,
    dismissalTimeoutMs: 200,
  });

  assert.deepEqual(actions, ['Продолжить']);
  assert.equal(evidence.discovery_route, 'current_alert_exact_button_pair');
  assert.equal(evidence.dismissed, true);
});

test('unknown or ambiguous Safari alerts remain fail-closed and receive no action', async () => {
  const clock = deterministicClock();
  const actions = [];
  await assert.rejects(
    stabilizeSafariSystemUi({
      inspect: async () => boundedSearchChoiceState({ buttonCount: 3 }),
      dismissKnownDialog: async (label) => { actions.push(label); },
      wait: clock.wait,
      now: clock.now,
      intervalMs: 10,
      discoveryTimeoutMs: 50,
    }),
    (error) => error instanceof SafariFirstRunUiError
      && error.message === 'safari_first_run_ui:unexpected_blocking_modal',
  );
  assert.deepEqual(actions, []);
});

test('failed exact current-alert action remains BLOCKED', async () => {
  const clock = deterministicClock();
  const state = boundedSearchChoiceState();
  await assert.rejects(
    stabilizeSafariSystemUi({
      inspect: async () => state,
      dismissKnownDialog: async () => { throw new Error('driver rejected button label'); },
      wait: clock.wait,
      now: clock.now,
      intervalMs: 10,
      discoveryTimeoutMs: 50,
    }),
    (error) => error instanceof SafariFirstRunUiError
      && error.message === 'safari_first_run_ui:bounded_current_alert_exact_action_failed'
      && error.evidence.attempts === 1,
  );
});
