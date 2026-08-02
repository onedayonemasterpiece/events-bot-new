export function focusOtpFailureDomain(error) {
  const message = String(error?.message || error);
  if (/safari_first_run_ui/iu.test(message)) return 'BLOCKED_SAFARI_FIRST_RUN_UI';
  if (/ios_simulator_keyboard/iu.test(message)) return 'BLOCKED_IOS_SIMULATOR_KEYBOARD';
  if (/missing_configuration|configuration_invalid|configuration_missing|simulator_(?:runtime|appium|configuration|safari_navigation)|safari_native_|target_url|websocket_connect/iu.test(message)) return 'BLOCKED_INFRASTRUCTURE';
  if (/release_evidence/iu.test(message)) return 'FAIL_RELEASE_EVIDENCE';
  if (/fault_not_active/iu.test(message)) return 'FAIL_FAULT_NOT_ACTIVE';
  if (/fault_route_selection/iu.test(message)) return 'FAIL_ROUTE_SELECTION';
  if (/fault_route_policy/iu.test(message)) return 'FAIL_ROUTE_POLICY';
  if (/mail_/iu.test(message)) return 'FAIL_DELIVERY';
  if (/fail_mobile_keyboard/iu.test(message)) return 'FAIL_MOBILE_KEYBOARD';
  if (/fail_mobile_viewport/iu.test(message)) return 'FAIL_MOBILE_VIEWPORT';
  if (/browser_context/iu.test(message)) return 'FAIL_BROWSER_CONTEXT';
  if (/runtime_diagnostics/iu.test(message)) return 'FAIL_RUNTIME_DIAGNOSTICS';
  return 'FAIL_PRODUCT';
}
