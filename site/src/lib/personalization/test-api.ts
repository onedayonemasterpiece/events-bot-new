import type {
  PersonalizationRuntimeMode,
  PersonalizationSurfaceIdV1,
  TargetPresenterPlanV1,
} from './contract.ts';
import type { RouteSurfaceResolutionV1 } from './surface-policy.ts';

const MAX_DIAGNOSTICS = 32;
const MAX_PLAN_ITEMS = 64;
const MAX_SURFACES = 32;

export interface PersonalizationNetworkCountersV1 {
  total: number;
  reads: number;
  writes: number;
  harness_supplied: boolean;
}

export interface LegacyParitySnapshotV1 {
  ids: string[];
  scores: number[];
}

export interface PersonalizationTestSnapshotInputV1 {
  mode: PersonalizationRuntimeMode;
  route: RouteSurfaceResolutionV1;
  surfaces?: PersonalizationSurfaceIdV1[];
  diagnosticCodes?: string[];
  legacyProfileByteSize?: number;
  legacyParity?: LegacyParitySnapshotV1;
  targetShadowPlan?: TargetPresenterPlanV1 | null;
  networkCounters?: Partial<PersonalizationNetworkCountersV1> | null;
}

export interface PersonalizationTestSnapshotV1 {
  schema_version: 'p13n-test-api-v1';
  mode: PersonalizationRuntimeMode;
  page_family: string;
  surface_inventory: PersonalizationSurfaceIdV1[];
  target_policy_id: string;
  target_policy_version: string;
  static_only_reason: string | null;
  diagnostic_codes: string[];
  legacy_profile_byte_size: number;
  legacy_parity_plan: LegacyParitySnapshotV1;
  target_shadow_plan: { ids: string[]; applied: false } | null;
  network_request_counters: PersonalizationNetworkCountersV1;
}

function boundedNonNegative(value: unknown): number {
  const numeric = Number(value || 0);
  return Number.isFinite(numeric) ? Math.max(0, Math.min(Number.MAX_SAFE_INTEGER, Math.floor(numeric))) : 0;
}

function boundedStrings(values: unknown, cap: number): string[] {
  return Array.isArray(values)
    ? values.slice(0, cap).map(String).filter((item) => item.length > 0 && item.length <= 160)
    : [];
}

export function buildPersonalizationTestSnapshotV1(input: PersonalizationTestSnapshotInputV1): PersonalizationTestSnapshotV1 {
  const ids = boundedStrings(input.legacyParity?.ids, MAX_PLAN_ITEMS);
  const scores = Array.isArray(input.legacyParity?.scores)
    ? input.legacyParity.scores.slice(0, ids.length).map((value) => Number(Number(value || 0).toFixed(4)))
    : [];
  return {
    schema_version: 'p13n-test-api-v1',
    mode: input.mode,
    page_family: String(input.route.pageFamily || 'unknown').slice(0, 80),
    surface_inventory: (input.surfaces || [input.route.surfaceId]).slice(0, MAX_SURFACES),
    target_policy_id: input.route.policy.id,
    target_policy_version: input.route.policy.registryVersion,
    static_only_reason: input.route.staticOnlyReason ? String(input.route.staticOnlyReason).slice(0, 160) : null,
    diagnostic_codes: boundedStrings([input.route.diagnostic, ...(input.diagnosticCodes || [])], MAX_DIAGNOSTICS),
    legacy_profile_byte_size: boundedNonNegative(input.legacyProfileByteSize),
    legacy_parity_plan: { ids, scores },
    target_shadow_plan: input.targetShadowPlan ? {
      ids: boundedStrings(input.targetShadowPlan.plannedOrder, MAX_PLAN_ITEMS),
      applied: false,
    } : null,
    network_request_counters: {
      total: boundedNonNegative(input.networkCounters?.total),
      reads: boundedNonNegative(input.networkCounters?.reads),
      writes: boundedNonNegative(input.networkCounters?.writes),
      harness_supplied: Boolean(input.networkCounters),
    },
  };
}
