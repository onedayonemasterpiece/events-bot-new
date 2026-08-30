import registryPayload from './design-system-reference-fixtures.json';
import type { PreviewEvent } from '../lib/types';

export interface DesignFixtureScenario {
  route: string;
  viewport: { width: number; height: number; dpr: number };
  reference_date: string;
  event_ids: number[];
  expected_card_count: number;
  container_family: string;
  card_family: string;
  availability: 'local-preview-only';
}

interface DesignFixtureRegistry {
  schema_version: 'design-system-reference-fixtures.v2';
  profile_id: string;
  authority: {
    ui_sot_registry_id: string;
    ui_sot_contract: string;
    ui_sot_contract_sha256: string;
    ui_sot_scenario: string;
    ui_sot_scenario_sha256: string;
  };
  festivals: {
    rows: Array<{ role: string; slugs: string[] }>;
  };
  scenarios: Record<string, DesignFixtureScenario>;
}

const registry = registryPayload as DesignFixtureRegistry;

function assertRegistry(): void {
  if (registry.schema_version !== 'design-system-reference-fixtures.v2') {
    throw new Error(`Unsupported design fixture registry: ${registry.schema_version}`);
  }
  if (registry.authority.ui_sot_registry_id !== 'design-system-reference-v1'
    || !/^[0-9a-f]{64}$/.test(registry.authority.ui_sot_contract_sha256)
    || !/^[0-9a-f]{64}$/.test(registry.authority.ui_sot_scenario_sha256)) {
    throw new Error('Design fixture bridge is missing its exact UI SoT registry/scenario pins');
  }
}

export function getActiveDesignFixtureScenario(
  requestedProfile: string,
  requestedScenario: string,
  siteMode: string,
): { id: string; scenario: DesignFixtureScenario } | null {
  assertRegistry();
  if (!requestedProfile && !requestedScenario) return null;
  if (['production', 'secret_candidate', 'secret-candidate'].includes(siteMode)) {
    throw new Error('UI fixture scenarios are forbidden in production and secret-candidate builds');
  }
  if (requestedProfile !== registry.profile_id) {
    throw new Error(`Unknown design fixture profile: ${requestedProfile || '(empty)'}`);
  }
  const scenario = registry.scenarios[requestedScenario];
  if (!scenario) throw new Error(`Unknown UI fixture scenario: ${requestedScenario || '(empty)'}`);
  return { id: requestedScenario, scenario };
}

export function selectExactScenarioEvents(
  events: PreviewEvent[],
  scenarioId: string,
  scenario: DesignFixtureScenario,
): PreviewEvent[] {
  const ids = scenario.event_ids.map(Number);
  if (ids.length !== scenario.expected_card_count || new Set(ids).size !== ids.length) {
    throw new Error(`Invalid event identity set for UI fixture scenario ${scenarioId}`);
  }
  const byId = new Map(events.map((event) => [Number(event.id), event]));
  const selected = ids.map((id) => byId.get(id));
  if (selected.some((event) => !event)) {
    const missing = ids.filter((id) => !byId.has(id));
    throw new Error(`UI fixture scenario ${scenarioId} is missing factual events: ${missing.join(', ')}`);
  }
  if (selected.some((event) => !event!.ticket.is_free)) {
    throw new Error(`UI fixture scenario ${scenarioId} contains a non-free event`);
  }
  return selected as PreviewEvent[];
}

export const designFixtureProfileId = registry.profile_id;
export const designFixtureRegistry = registry;
