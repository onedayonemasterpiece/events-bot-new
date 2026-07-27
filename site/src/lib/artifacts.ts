import registrySource from '../data/artifactRegistry.json';

export const ARTIFACT_STORY_DOMAINS = [
  'architecture',
  'character',
  'culture',
  'food',
  'history',
  'maritime',
  'nature',
  'place',
  'sport',
  'technology',
  'transport',
] as const;

export type ArtifactStoryDomain = (typeof ARTIFACT_STORY_DOMAINS)[number];
export type ArtifactRegistryStatus = 'candidate' | 'collection_draft' | 'needs_clarification';
export type ArtifactDifficulty = 'onboarding' | 'standard' | 'hard';

export interface ArtifactRegistryItem {
  id: string;
  slug: string;
  public_name: string;
  story_domain: ArtifactStoryDomain;
  registry_status: ArtifactRegistryStatus;
  collection_ids: string[];
  planned_difficulty: ArtifactDifficulty | null;
  review_flags: string[];
  source_refs: string[];
}

export interface ArtifactCollection {
  id: string;
  slug: string;
  public_name: string;
  summary: string;
  status:
    | 'draft'
    | 'scheduled'
    | 'collecting'
    | 'application_grace'
    | 'draw_locked'
    | 'claim'
    | 'closed'
    | 'archived'
    | 'suspended';
  artifact_ids: string[];
  simultaneous_availability: boolean;
  planned_window_days: number;
  application_grace_hours: number;
  unlock: {
    threshold_percent: number;
    required_count: number;
    total_count: number;
    effect: 'drawing_application_available';
    automatic_entry: boolean;
    extra_finds_increase_odds: boolean;
    share_increases_odds: boolean;
  };
}

interface ArtifactRegistry {
  schema_version: 'artifact_registry_v1';
  registry_version: string;
  updated_at: string;
  public_title: string;
  public_explainer: string;
  source_scope: {
    kind: string;
    anchor_message_id: number;
    retrieved_through_message_id: number;
    content_policy: string;
  };
  collections: ArtifactCollection[];
  artifacts: ArtifactRegistryItem[];
}

export const ARTIFACT_DOMAIN_LABELS: Record<ArtifactStoryDomain, string> = {
  architecture: 'Архитектура',
  character: 'Персонажи и легенды',
  culture: 'Культура',
  food: 'Гастрономия',
  history: 'История',
  maritime: 'Море',
  nature: 'Природа',
  place: 'Места',
  sport: 'Спорт',
  technology: 'Технологии',
  transport: 'Транспорт',
};

export const ARTIFACT_STATUS_LABELS: Record<ArtifactRegistryStatus, string> = {
  candidate: 'Кандидат',
  collection_draft: 'В первой коллекции',
  needs_clarification: 'Нужно уточнить',
};

function fail(message: string): never {
  throw new Error(`Invalid artifact registry: ${message}`);
}

function validateRegistry(value: unknown): asserts value is ArtifactRegistry {
  if (!value || typeof value !== 'object') fail('root must be an object');
  const registry = value as Partial<ArtifactRegistry>;
  if (registry.schema_version !== 'artifact_registry_v1') fail('unsupported schema_version');
  if (!Array.isArray(registry.artifacts) || registry.artifacts.length === 0) fail('artifacts must be non-empty');
  if (!Array.isArray(registry.collections) || registry.collections.length === 0) fail('collections must be non-empty');

  const artifactIds = new Set<string>();
  const artifactSlugs = new Set<string>();
  for (const item of registry.artifacts) {
    if (!item.id || !/^[a-z0-9]+(?:-[a-z0-9]+)*$/u.test(item.id)) fail(`unsafe artifact id ${item.id}`);
    if (artifactIds.has(item.id)) fail(`duplicate artifact id ${item.id}`);
    if (artifactSlugs.has(item.slug)) fail(`duplicate artifact slug ${item.slug}`);
    if (!ARTIFACT_STORY_DOMAINS.includes(item.story_domain)) fail(`unsupported domain for ${item.id}`);
    if (!['candidate', 'collection_draft', 'needs_clarification'].includes(item.registry_status)) {
      fail(`unsupported status for ${item.id}`);
    }
    if (!item.public_name.trim()) fail(`missing public_name for ${item.id}`);
    artifactIds.add(item.id);
    artifactSlugs.add(item.slug);
  }

  const collectionIds = new Set<string>();
  const artifactMemberships = new Map<string, Set<string>>();
  for (const collection of registry.collections) {
    if (collectionIds.has(collection.id)) fail(`duplicate collection id ${collection.id}`);
    if (new Set(collection.artifact_ids).size !== collection.artifact_ids.length) {
      fail(`duplicate artifact membership for ${collection.id}`);
    }
    if (
      !Number.isInteger(collection.unlock.threshold_percent)
      || collection.unlock.threshold_percent < 1
      || collection.unlock.threshold_percent > 100
    ) {
      fail(`unsafe threshold_percent for ${collection.id}`);
    }
    if (collection.artifact_ids.length !== collection.unlock.total_count) {
      fail(`total_count mismatch for ${collection.id}`);
    }
    const expectedRequired = Math.ceil(
      collection.artifact_ids.length * collection.unlock.threshold_percent / 100,
    );
    if (collection.unlock.required_count !== expectedRequired) {
      fail(`required_count mismatch for ${collection.id}`);
    }
    if (!collection.simultaneous_availability) fail(`${collection.id} must be simultaneous in the first contract`);
    if (collection.unlock.effect !== 'drawing_application_available') {
      fail(`${collection.id} must unlock an application, not a reward`);
    }
    if (
      collection.unlock.automatic_entry
      || collection.unlock.extra_finds_increase_odds
      || collection.unlock.share_increases_odds
    ) {
      fail(`${collection.id} violates equal-weight application rules`);
    }
    if (collection.planned_window_days <= 0 || collection.application_grace_hours < 0) {
      fail(`invalid collection window for ${collection.id}`);
    }
    for (const artifactId of collection.artifact_ids) {
      if (!artifactIds.has(artifactId)) fail(`${collection.id} references unknown artifact ${artifactId}`);
      const memberships = artifactMemberships.get(artifactId) || new Set<string>();
      memberships.add(collection.id);
      artifactMemberships.set(artifactId, memberships);
    }
    collectionIds.add(collection.id);
  }

  for (const item of registry.artifacts) {
    for (const collectionId of item.collection_ids) {
      if (!collectionIds.has(collectionId)) fail(`${item.id} references unknown collection ${collectionId}`);
    }
    const declaredMemberships = new Set(item.collection_ids);
    const collectionMemberships = artifactMemberships.get(item.id) || new Set<string>();
    if (
      declaredMemberships.size !== collectionMemberships.size
      || [...declaredMemberships].some((collectionId) => !collectionMemberships.has(collectionId))
    ) {
      fail(`non-reciprocal collection membership for ${item.id}`);
    }
    if (declaredMemberships.size > 0 && item.planned_difficulty === null) {
      fail(`collection artifact ${item.id} requires planned_difficulty`);
    }
    if (declaredMemberships.size === 0 && item.planned_difficulty !== null) {
      fail(`uncollected artifact ${item.id} cannot have planned_difficulty`);
    }
  }
}

validateRegistry(registrySource);
const registry: ArtifactRegistry = registrySource;

export function getArtifactRegistry(): ArtifactRegistry {
  return registry;
}

export function getArtifactCollection(collectionId: string): ArtifactCollection | undefined {
  return registry.collections.find((collection) => collection.id === collectionId);
}

export function getArtifactsForCollection(collectionId: string): ArtifactRegistryItem[] {
  const collection = getArtifactCollection(collectionId);
  if (!collection) return [];
  const byId = new Map(registry.artifacts.map((item) => [item.id, item]));
  return collection.artifact_ids.map((id) => byId.get(id)).filter((item): item is ArtifactRegistryItem => Boolean(item));
}

export function getPublicArtifactRegistry() {
  return {
    schema_version: 'artifact_public_registry_v1',
    registry_version: registry.registry_version,
    updated_at: registry.updated_at,
    title: registry.public_title,
    explainer: registry.public_explainer,
    collections: registry.collections.map((collection) => ({
      id: collection.id,
      slug: collection.slug,
      public_name: collection.public_name,
      summary: collection.summary,
      status: collection.status,
      artifact_ids: [...collection.artifact_ids],
      simultaneous_availability: collection.simultaneous_availability,
      planned_window_days: collection.planned_window_days,
      application_grace_hours: collection.application_grace_hours,
      unlock: { ...collection.unlock },
    })),
    artifacts: registry.artifacts.map((item) => ({
      id: item.id,
      slug: item.slug,
      public_name: item.public_name,
      story_domain: item.story_domain,
      registry_status: item.registry_status,
      collection_ids: [...item.collection_ids],
      planned_difficulty: item.planned_difficulty,
    })),
  };
}
