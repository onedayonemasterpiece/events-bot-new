import registryPayload from './design-system-reference-fixtures.json';
import frozenEventPayload from './ui-reference-events-v2.json';
import type { PreviewEvent } from '../lib/types';

export interface DesignFixtureScenario {
  route: string;
  viewport: { width: number; height: number; dpr: number };
  reference_date: string;
  updated_date: string;
  event_ids: number[];
  expected_card_count: number;
  container_family: string;
  card_family: string;
  availability: 'local-preview-only';
}

interface DesignFixtureRegistry {
  schema_version: 'design-system-reference-fixtures.v3';
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

interface FrozenEventFixture {
  fixture_id: string;
  event_id: number;
  preview_event_sha256: string;
  preview_event: PreviewEvent;
}

interface FrozenEventCorpus {
  schema_version: 'astro-ui-reference-events.v2';
  authority: {
    registry_sha256: string;
    scenario_sha256: string;
    corpus_content_sha256: string;
    source_preview_export_sha256: string;
    source_repository_sha: string;
    source_snapshot_sha256: string;
  };
  projection: { fixture_input_order: string[]; explicit_exclusions: unknown[] };
  fixtures: FrozenEventFixture[];
}

const registry = registryPayload as DesignFixtureRegistry;
const frozenCorpus = frozenEventPayload as FrozenEventCorpus;

function assertRegistry(): void {
  if (registry.schema_version !== 'design-system-reference-fixtures.v3') {
    throw new Error(`Unsupported design fixture registry: ${registry.schema_version}`);
  }
  if (registry.authority.ui_sot_registry_id !== 'design-system-reference-v2'
    || !/^[0-9a-f]{64}$/.test(registry.authority.ui_sot_contract_sha256)
    || !/^[0-9a-f]{64}$/.test(registry.authority.ui_sot_scenario_sha256)) {
    throw new Error('Design fixture bridge is missing its exact UI SoT registry/scenario pins');
  }
  if (frozenCorpus.schema_version !== 'astro-ui-reference-events.v2'
    || frozenCorpus.authority.registry_sha256 !== registry.authority.ui_sot_contract_sha256
    || frozenCorpus.authority.scenario_sha256 !== registry.authority.ui_sot_scenario_sha256) {
    throw new Error('Frozen Golden Event Corpus and design fixture bridge authority pins disagree');
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
  scenarioId: string,
  scenario: DesignFixtureScenario,
): PreviewEvent[] {
  const ids = scenario.event_ids.map(Number);
  if (ids.length !== scenario.expected_card_count || new Set(ids).size !== ids.length) {
    throw new Error(`Invalid event identity set for UI fixture scenario ${scenarioId}`);
  }
  const projectionIds = frozenCorpus.projection.fixture_input_order.map((value) => Number(value.replace('event.real.', '')));
  if (projectionIds.length !== ids.length || projectionIds.some((id, index) => id !== ids[index])) {
    throw new Error(`UI fixture scenario ${scenarioId} drifted from its frozen Golden Corpus projection`);
  }
  const byId = new Map(frozenCorpus.fixtures.map((fixture) => [Number(fixture.event_id), fixture.preview_event]));
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
export const frozenDesignFixtureCorpus = frozenCorpus;
