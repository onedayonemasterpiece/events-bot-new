import type {
  PersonalizationPolicyIdV1,
  TargetPresenterPlanV1,
  TargetRankInputV1,
} from './contract.ts';
import { PERSONALIZATION_SURFACE_POLICIES_V1 } from './surface-policy.ts';

const MAX_PLAN_IDS_V1 = 1_000;

function normalizedUniqueIds(values: readonly unknown[]): string[] | null {
  if (values.length > MAX_PLAN_IDS_V1) return null;
  const result: string[] = [];
  const seen = new Set<string>();
  for (const value of values) {
    const id = String(value ?? '').trim();
    if (!id || id.length > 160 || seen.has(id)) return null;
    seen.add(id);
    result.push(id);
  }
  return result;
}

export interface TargetPresenterPlanInputV1 {
  policyId: PersonalizationPolicyIdV1;
  currentOrder: readonly string[];
  targetRanks: readonly TargetRankInputV1[];
  frozenIds: readonly string[];
}

export function buildTargetPresenterPlanV1(input: TargetPresenterPlanInputV1): TargetPresenterPlanV1 {
  const policy = PERSONALIZATION_SURFACE_POLICIES_V1[input.policyId] || PERSONALIZATION_SURFACE_POLICIES_V1['unknown-static'];
  const current = normalizedUniqueIds(input.currentOrder);
  const frozen = normalizedUniqueIds(input.frozenIds);
  const diagnostics: string[] = [];
  if (!current) diagnostics.push('p13n_plan.invalid_current_order');
  if (!frozen) diagnostics.push('p13n_plan.invalid_frozen_ids');
  const safeCurrent = current || [];
  const currentSet = new Set(safeCurrent);
  const safeFrozen = (frozen || []).filter((id) => currentSet.has(id));
  const frozenSet = new Set(safeFrozen);

  const rankMap = new Map<string, number>();
  if (input.targetRanks.length > MAX_PLAN_IDS_V1) diagnostics.push('p13n_plan.target_rank_cap_exceeded');
  for (const item of input.targetRanks.slice(0, MAX_PLAN_IDS_V1)) {
    const id = String(item.eventId ?? '').trim();
    if (!id || !currentSet.has(id) || rankMap.has(id) || !Number.isFinite(item.targetRank)) {
      diagnostics.push('p13n_plan.invalid_target_rank');
      continue;
    }
    rankMap.set(id, item.targetRank);
  }

  if (policy.reorderScope === 'none' || diagnostics.includes('p13n_plan.invalid_current_order')) {
    diagnostics.push('p13n_plan.identity');
    return {
      policyId: policy.id,
      registryVersion: policy.registryVersion,
      currentOrder: safeCurrent,
      plannedOrder: safeCurrent.slice(),
      frozenIds: safeFrozen,
      applied: false,
      diagnosticCodes: Array.from(new Set(diagnostics)),
    };
  }

  const nativeIndex = new Map(safeCurrent.map((id, index) => [id, index]));
  const reorderable = safeCurrent
    .filter((id) => !frozenSet.has(id))
    .sort((left, right) => {
      const leftRank = rankMap.has(left) ? rankMap.get(left) as number : Number.POSITIVE_INFINITY;
      const rightRank = rankMap.has(right) ? rankMap.get(right) as number : Number.POSITIVE_INFINITY;
      return leftRank - rightRank || (nativeIndex.get(left) as number) - (nativeIndex.get(right) as number);
    });
  let next = 0;
  const planned = safeCurrent.map((id) => frozenSet.has(id) ? id : reorderable[next++]);
  diagnostics.push('p13n_plan.shadow_only');
  return {
    policyId: policy.id,
    registryVersion: policy.registryVersion,
    currentOrder: safeCurrent,
    plannedOrder: planned,
    frozenIds: safeFrozen,
    applied: false,
    diagnosticCodes: Array.from(new Set(diagnostics)),
  };
}
