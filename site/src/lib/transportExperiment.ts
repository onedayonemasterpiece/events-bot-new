export const TRANSPORT_EXPERIMENT_KEY = 'transport_timetable_layout';
export const TRANSPORT_EXPERIMENT_VERSION = 1;
// SHA-256 of the canonical key/version/algorithm/range string guarded by
// TR-EXP-02. Release ids never participate in this value or assignment.
export const TRANSPORT_EXPERIMENT_CONFIG_HASH = 'sha256:bf9a8a80e35c8699a26993ae25ac83313d4b6923900f9e51688d2dad7d92cdf2';
export const TRANSPORT_EXPERIMENT_ALGORITHM = 'sha256-u32be-bucket-10000-v1';

export const TRANSPORT_EXPERIMENT_VARIANTS = [
  'departure_board_v1',
  'route_strips_v1',
  'next_departure_queue_v1',
] as const;

export type TransportExperimentVariant = (typeof TRANSPORT_EXPERIMENT_VARIANTS)[number];
export type TransportExperimentMode = 'off' | 'qa' | 'focus_group' | 'live';

export const TRANSPORT_EXPERIMENT_BUCKETS: ReadonlyArray<{
  variant: TransportExperimentVariant;
  from: number;
  to: number;
}> = [
  { variant: 'departure_board_v1', from: 0, to: 3332 },
  { variant: 'route_strips_v1', from: 3333, to: 6665 },
  { variant: 'next_departure_queue_v1', from: 6666, to: 9999 },
];

export const TRANSPORT_QUALIFIED_ACTIONS = new Set([
  'official_transfer_booking_click',
  'bus_origin_map_click',
  'walk_route_click',
  'car_route_click',
  'transport_calendar_add',
]);

export function normalizeTransportExperimentMode(value: unknown): TransportExperimentMode {
  const normalized = String(value || '').trim().toLocaleLowerCase('en-US');
  return normalized === 'qa' || normalized === 'focus_group' || normalized === 'live'
    ? normalized
    : 'off';
}

export function isTransportExperimentVariant(value: unknown): value is TransportExperimentVariant {
  return TRANSPORT_EXPERIMENT_VARIANTS.includes(value as TransportExperimentVariant);
}

export function transportVariantForBucket(bucket: number): TransportExperimentVariant | null {
  if (!Number.isInteger(bucket) || bucket < 0 || bucket > 9999) return null;
  return TRANSPORT_EXPERIMENT_BUCKETS.find((range) => bucket >= range.from && bucket <= range.to)?.variant || null;
}

export function transportExperimentAllocationInput(subjectId: string): string {
  return `${TRANSPORT_EXPERIMENT_KEY}|${TRANSPORT_EXPERIMENT_VERSION}|${subjectId}`;
}

export async function transportExperimentBucket(
  subjectId: string,
  cryptoApi: Pick<Crypto, 'subtle'> | undefined = globalThis.crypto,
): Promise<number | null> {
  if (!cryptoApi?.subtle || !isUuid(subjectId)) return null;
  const bytes = new TextEncoder().encode(transportExperimentAllocationInput(subjectId));
  const digest = await cryptoApi.subtle.digest('SHA-256', bytes);
  const unsigned = new DataView(digest).getUint32(0, false);
  return Math.floor((unsigned / 0x1_0000_0000) * 10_000);
}

export async function assignTransportExperimentVariant(
  subjectId: string,
  cryptoApi?: Pick<Crypto, 'subtle'>,
): Promise<{ bucket: number; variant: TransportExperimentVariant } | null> {
  const bucket = await transportExperimentBucket(subjectId, cryptoApi);
  if (bucket === null) return null;
  const variant = transportVariantForBucket(bucket);
  return variant ? { bucket, variant } : null;
}

export function isUuid(value: unknown): value is string {
  return typeof value === 'string'
    && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu.test(value);
}

export function transportExperimentEligible(
  departureTimestamps: readonly string[],
  nowMs = Date.now(),
  boardingReserveMs = 10 * 60 * 1000,
): boolean {
  if (departureTimestamps.length < 2 || departureTimestamps.length > 20) return false;
  const parsed = departureTimestamps.map((value) => Date.parse(value));
  if (parsed.some((value) => !Number.isFinite(value))) return false;
  return parsed.some((value) => value > nowMs + boardingReserveMs);
}

export function isQualifiedTransportAction(value: unknown): boolean {
  return TRANSPORT_QUALIFIED_ACTIONS.has(String(value || ''));
}

export function transportExperimentManifestRecord(mode: unknown) {
  return {
    experiment_key: TRANSPORT_EXPERIMENT_KEY,
    experiment_version: TRANSPORT_EXPERIMENT_VERSION,
    mode: normalizeTransportExperimentMode(mode),
    assignment_unit: 'browser_subject',
    allocation_algorithm: TRANSPORT_EXPERIMENT_ALGORITHM,
    config_hash: TRANSPORT_EXPERIMENT_CONFIG_HASH,
    variants: TRANSPORT_EXPERIMENT_BUCKETS.map((range) => ({ ...range })),
  } as const;
}

export function evaluateTransportSampleRatio(counts: readonly [number, number, number]) {
  if (counts.some((value) => !Number.isInteger(value) || value < 0)) throw new TypeError('SRM counts must be non-negative integers');
  const total = counts.reduce((sum, value) => sum + value, 0);
  const shares = TRANSPORT_EXPERIMENT_BUCKETS.map((range) => (range.to - range.from + 1) / 10_000);
  const expected = shares.map((share) => total * share);
  const chiSquare = total > 0
    ? counts.reduce((sum, value, index) => sum + ((value - expected[index]) ** 2 / expected[index]), 0)
    : 0;
  // Three cells => df=2, whose chi-square survival function is exp(-x/2).
  const pValue = Math.exp(-chiSquare / 2);
  const maxAbsoluteShareDeviation = total > 0
    ? Math.max(...counts.map((value, index) => Math.abs((value / total) - shares[index])))
    : 0;
  return {
    total,
    expected,
    chiSquare,
    pValue,
    maxAbsoluteShareDeviation,
    diagnosticOnly: total < 300,
    blocker: total >= 300 && pValue < 0.001 && maxAbsoluteShareDeviation > 0.015,
  };
}
