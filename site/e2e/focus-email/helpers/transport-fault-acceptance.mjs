const MANDATORY_OPERATIONS = Object.freeze([
  'auth.otp',
  'auth.verify',
  'rpc.register_focus_group_participant_v1',
]);

export function assertExpectedTransportRoutes(profile, outcomes = []) {
  const expectedRoute = profile === 'client_supabase_direct_unreachable' ? 'relay'
    : profile === 'client_yandex_relay_unreachable' ? 'direct' : null;
  if (!expectedRoute) return;

  for (const operation of MANDATORY_OPERATIONS) {
    const matching = outcomes.filter((event) => event.operation === operation);
    const relayCount = matching.filter((event) => event.finalRoute === 'relay').length;
    const directCount = matching.filter((event) => event.finalRoute === 'direct').length;
    const expectedCount = expectedRoute === 'relay' ? relayCount : directCount;
    const oppositeCount = expectedRoute === 'relay' ? directCount : relayCount;
    if (expectedCount !== 1 || oppositeCount !== 0) {
      throw new Error(`fault_route_selection:${operation}:${directCount}:${relayCount}`);
    }
  }
}
