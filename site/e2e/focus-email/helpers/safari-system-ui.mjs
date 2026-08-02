export class SafariFirstRunUiError extends Error {
  constructor(code, evidence) {
    super(`safari_first_run_ui:${code}`);
    this.name = 'SafariFirstRunUiError';
    this.evidence = evidence;
  }
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export function classifySafariInspection({ titleCount = 0, scopedActions = [], topLevelCount = 0, knownTopLevelCount = 0 } = {}) {
  const titles = Number(titleCount || 0);
  const actions = Array.isArray(scopedActions) ? scopedActions.filter(Boolean) : [];
  const unknown = Math.max(0, Number(topLevelCount || 0) - Number(knownTopLevelCount || 0));
  return {
    known_dialog_count: titles,
    continue_button_count: actions.length,
    blocking_dialog_count: Math.max(titles, actions.length) + unknown,
    unknown_blocking_dialog_count: unknown,
    action_token: titles === 1 && actions.length === 1 ? actions[0] : null,
  };
}

/**
 * Stabilize Safari native UI without accepting arbitrary alerts.
 * inspect() returns counts only; no native labels or hierarchy enter evidence.
 */
export async function stabilizeSafariSystemUi({
  inspect,
  dismissKnownDialog,
  wait = sleep,
  now = Date.now,
  discoveryTimeoutMs = 12_000,
  dismissalTimeoutMs = 5_000,
  intervalMs = 250,
  stableAbsentSamples = 3,
} = {}) {
  if (typeof inspect !== 'function' || typeof dismissKnownDialog !== 'function') {
    throw new TypeError('safari_system_ui_adapter_missing');
  }
  const started = now();
  const evidence = {
    dialog: 'search_engine_choice',
    seen: false,
    dismissed: false,
    attempts: 0,
    elapsed_ms: 0,
    obstruction_free: false,
    transitions: ['native_scan_started'],
  };
  const finish = (transition) => {
    evidence.elapsed_ms = Math.max(0, now() - started);
    evidence.transitions.push(transition);
    return evidence;
  };
  const reject = (code, transition = code) => {
    finish(transition);
    throw new SafariFirstRunUiError(code, evidence);
  };
  const recordInspection = (state) => {
    evidence.last_inspection = {
      known_dialog_count: Number(state?.known_dialog_count || 0),
      continue_button_count: Number(state?.continue_button_count || 0),
      blocking_dialog_count: Number(state?.blocking_dialog_count || 0),
      unknown_blocking_dialog_count: Number(state?.unknown_blocking_dialog_count || 0),
    };
    if (state?.contract_probe && typeof state.contract_probe === 'object') {
      const probe = state.contract_probe;
      evidence.last_inspection.contract_probe = {
        exact_visible_static_text_count: Number(probe.exact_visible_static_text_count || 0),
        exact_static_text_count: Number(probe.exact_static_text_count || 0),
        containing_static_text_count: Number(probe.containing_static_text_count || 0),
        exact_any_element_count: Number(probe.exact_any_element_count || 0),
        current_alert_present: probe.current_alert_present === true,
        alert_text_length: Number(probe.alert_text_length || 0),
        alert_text_line_count: Number(probe.alert_text_line_count || 0),
        exact_title_line_count: Number(probe.exact_title_line_count || 0),
        title_substring_count: Number(probe.title_substring_count || 0),
        alert_button_count: Number(probe.alert_button_count || 0),
        exact_continue_button_count: Number(probe.exact_continue_button_count || 0),
        exact_settings_button_count: Number(probe.exact_settings_button_count || 0),
      };
      if (Object.hasOwn(probe, 'active_app_owner')) {
        evidence.last_inspection.contract_probe.active_app_owner = ['springboard', 'safari', 'other'].includes(probe.active_app_owner) ? probe.active_app_owner : 'unknown';
      }
      if (Object.hasOwn(probe, 'native_source')) {
        evidence.last_inspection.contract_probe.native_source = probe.native_source?.source_inspected === true ? {
          source_inspected: true,
          application_container_count: Number(probe.native_source.application_container_count || 0),
          alert_container_count: Number(probe.native_source.alert_container_count || 0),
          sheet_container_count: Number(probe.native_source.sheet_container_count || 0),
          title_match_count: Number(probe.native_source.title_match_count || 0),
          continue_match_count: Number(probe.native_source.continue_match_count || 0),
          settings_match_count: Number(probe.native_source.settings_match_count || 0),
          matched_static_text_count: Number(probe.native_source.matched_static_text_count || 0),
          matched_button_count: Number(probe.native_source.matched_button_count || 0),
          matched_other_type_count: Number(probe.native_source.matched_other_type_count || 0),
        } : { source_inspected: false };
      }
    }
  };

  let known = null;
  while (now() - started <= discoveryTimeoutMs) {
    const state = await inspect();
    recordInspection(state);
    if (Number(state?.unknown_blocking_dialog_count || 0) > 0) reject('unexpected_blocking_modal');
    if (Number(state?.known_dialog_count || 0) > 1 || Number(state?.continue_button_count || 0) > 1) {
      reject('ambiguous_search_choice_dialog');
    }
    if (Number(state?.known_dialog_count || 0) === 1) {
      if (Number(state?.continue_button_count || 0) !== 1 || !state?.action_token) {
        reject('search_choice_action_missing');
      }
      known = state;
      break;
    }
    await wait(intervalMs);
  }

  if (!known) {
    evidence.obstruction_free = true;
    return finish('no_blocking_modal_observed');
  }

  evidence.seen = true;
  evidence.attempts = 1;
  evidence.transitions.push('search_choice_observed', 'dismissal_requested');
  await dismissKnownDialog(known.action_token);

  const dismissalStarted = now();
  let absentSamples = 0;
  while (now() - dismissalStarted <= dismissalTimeoutMs) {
    const state = await inspect();
    recordInspection(state);
    if (Number(state?.unknown_blocking_dialog_count || 0) > 0) reject('unexpected_blocking_modal_after_dismissal');
    if (Number(state?.known_dialog_count || 0) > 1 || Number(state?.continue_button_count || 0) > 1) {
      reject('ambiguous_search_choice_dialog_after_dismissal');
    }
    if (Number(state?.known_dialog_count || 0) === 0 && Number(state?.blocking_dialog_count || 0) === 0) {
      absentSamples += 1;
      if (absentSamples >= stableAbsentSamples) {
        evidence.dismissed = true;
        evidence.obstruction_free = true;
        return finish('dismissal_verified');
      }
    } else {
      absentSamples = 0;
    }
    await wait(intervalMs);
  }
  reject('search_choice_dialog_stuck');
}
