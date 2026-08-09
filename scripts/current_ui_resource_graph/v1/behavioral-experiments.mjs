import { createHash } from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
import { join, resolve } from 'node:path';

const EXACT_SOURCE_SHA = 'ef7aa62e45c60f7a12da6160f490719c0721ec03';
const PRIMARY_PATH = 'src/lib/transportExperiment.ts';
const EXPECTED_SOURCE_HASHES = Object.freeze({
  'src/lib/transportExperiment.ts': '2bc4e0e1860f45423cc31cf740b72d7b82a0d52e01ca9c1ed2e617182ece55b5',
  'src/lib/transportExperimentClient.ts': 'e1e241d0614986076a72c68162ca27ebe97c1c4f45913d97d18b327f983af1cd',
  'src/components/transport/TransportTimetableExperiment.astro': 'a002462763fea6f837c278cdbc5894cd33a1acb197081b56954dedc8132e285a',
  'src/components/KaupTransportSchedule.astro': '6d31a1ddf921b1c42eadafa1a3aa239b8970f58c2bc01d070ec30e32c040e1e3',
  'scripts/build-production.mjs': '76c6628671522576203c40c7a7d24a892cdf5b06bd80ee9f353bb560ede78298',
  'scripts/build-secret-candidate.mjs': 'a3d2276ba0ac9e36c95a0e6596abdbcbd683c3561e87436b77fef06627be847f',
});

const sha = (value) => createHash('sha256').update(value).digest('hex');
const repoPath = (path) => `site/${path}`;

function readPinnedFiles(sourceRoot) {
  const root = resolve(sourceRoot);
  if (!existsSync(join(root, PRIMARY_PATH))) return null;
  const files = {};
  for (const [path, expectedSha256] of Object.entries(EXPECTED_SOURCE_HASHES)) {
    const absolute = join(root, path);
    if (!existsSync(absolute)) throw new Error(`Pinned transport experiment source is missing: ${repoPath(path)}`);
    const content = readFileSync(absolute, 'utf8');
    const actualSha256 = sha(content);
    if (actualSha256 !== expectedSha256) throw new Error(`Pinned transport experiment source hash mismatch: ${repoPath(path)}`);
    files[path] = { path:repoPath(path), content, sha256:actualSha256 };
  }
  return files;
}

function requiredMatch(content, pattern, label) {
  const match = content.match(pattern);
  if (!match) throw new Error(`Cannot decode pinned transport experiment ${label}`);
  return match;
}

function quotedValues(block) {
  return [...block.matchAll(/'([^']+)'/gu)].map((match) => match[1]);
}

export function decodeTransportExperimentSource({ sourceRoot }) {
  const files = readPinnedFiles(sourceRoot);
  if (!files) return {
    status:'missing-pinned-source', exact_source_sha:EXACT_SOURCE_SHA,
    missing_path:repoPath(PRIMARY_PATH), source_files:[],
  };

  const primary = files[PRIMARY_PATH].content;
  const client = files['src/lib/transportExperimentClient.ts'].content;
  const component = files['src/components/transport/TransportTimetableExperiment.astro'].content;
  const productionBuild = files['scripts/build-production.mjs'].content;
  const secretBuild = files['scripts/build-secret-candidate.mjs'].content;
  const key = requiredMatch(primary, /TRANSPORT_EXPERIMENT_KEY\s*=\s*'([^']+)'/u, 'key')[1];
  const version = Number(requiredMatch(primary, /TRANSPORT_EXPERIMENT_VERSION\s*=\s*(\d+)/u, 'version')[1]);
  const configHash = requiredMatch(primary, /TRANSPORT_EXPERIMENT_CONFIG_HASH\s*=\s*'([^']+)'/u, 'config hash')[1];
  const algorithm = requiredMatch(primary, /TRANSPORT_EXPERIMENT_ALGORITHM\s*=\s*'([^']+)'/u, 'algorithm')[1];
  const variantBlock = requiredMatch(primary, /TRANSPORT_EXPERIMENT_VARIANTS\s*=\s*\[([\s\S]*?)\]\s*as const/u, 'variant list')[1];
  const modeBlock = requiredMatch(primary, /TransportExperimentMode\s*=\s*([^;]+);/u, 'mode list')[1];
  const bucketBlock = requiredMatch(primary, /TRANSPORT_EXPERIMENT_BUCKETS[\s\S]*?=\s*\[([\s\S]*?)\];/u, 'bucket ranges')[1];
  const actionBlock = requiredMatch(primary, /TRANSPORT_QUALIFIED_ACTIONS\s*=\s*new Set\(\[([\s\S]*?)\]\)/u, 'qualified actions')[1];
  const variants = quotedValues(variantBlock);
  const modes = quotedValues(modeBlock);
  const buckets = [...bucketBlock.matchAll(/variant:\s*'([^']+)',\s*from:\s*(\d+),\s*to:\s*(\d+)/gu)]
    .map((match) => ({ variant:match[1], from:Number(match[2]), to:Number(match[3]), buckets:Number(match[3])-Number(match[2])+1 }));
  const qualifiedActions = quotedValues(actionBlock);
  if (key !== 'transport_timetable_layout' || version !== 1
    || JSON.stringify(variants) !== JSON.stringify(['departure_board_v1','route_strips_v1','next_departure_queue_v1'])
    || JSON.stringify(modes) !== JSON.stringify(['off','qa','focus_group','live'])
    || buckets.reduce((sum, item) => sum + item.buckets, 0) !== 10_000) {
    throw new Error('Pinned transport experiment identity, modes, variants, or bucket coverage drifted');
  }
  for (const evidence of [
    [primary, /getUint32\(0, false\)/u, 'big-endian word'],
    [primary, /Math\.floor\(\(unsigned \/ 0x1_0000_0000\) \* 10_000\)/u, 'bucket formula'],
    [primary, /departureTimestamps\.length < 1 \|\| departureTimestamps\.length > 20/u, 'eligibility trip bound'],
    [primary, /value > nowMs \+ boardingReserveMs/u, 'future departure eligibility'],
    [primary, /total >= 300 && pValue < 0\.001 && maxAbsoluteShareDeviation > 0\.015/u, 'SRM blocker'],
    [client, /entry\?\.intersectionRatio >= 0\.5[\s\S]*?1000/u, 'valid exposure visibility window'],
    [client, /mode !== 'focus_group' && mode !== 'live'/u, 'telemetry mode gate'],
    [client, /mode !== 'qa' && mode !== 'focus_group'/u, 'QA override mode gate'],
    [component, /mode === 'off'[\s\S]*?baseline=\{true\}/u, 'off-mode baseline'],
    [productionBuild, /PUBLIC_TRANSPORT_TIMETABLE_EXPERIMENT_MODE:\s*'off'/u, 'production forced-off build'],
    [secretBuild, /\['qa', 'focus_group'\]\.includes\(transportExperimentMode\)/u, 'secret candidate mode allowlist'],
  ]) requiredMatch(evidence[0], evidence[1], evidence[2]);
  if (/\bisQualifiedTransportAction\b/u.test(client)) throw new Error('Pinned client now invokes the qualified-action filter; archaeology record must be reviewed');
  if (/\bevaluateTransportSampleRatio\b/u.test([client, component, productionBuild, secretBuild].join('\n'))) throw new Error('Pinned runtime now consumes SRM evaluation; archaeology record must be reviewed');

  return {
    status:'decoded-exact-source', exact_source_sha:EXACT_SOURCE_SHA,
    source_files:Object.values(files).map(({ path, sha256 }) => ({ path, sha256 })),
    experiment_key:key, experiment_version:version, config_hash:configHash,
    modes, variants, buckets, qualified_actions:qualifiedActions,
    assignment:{
      unit:'browser_subject', subject_storage_key:'ke_experiment_subject_v1',
      assignment_storage_key:`ke_experiment_assignment:${key}:${version}`,
      input:'UTF-8(experiment_key|experiment_version|browser_subject_uuid)',
      digest:'SHA-256', word:'first unsigned 32-bit big-endian word',
      bucket_formula:'floor(unsigned_u32be / 2^32 * 10000)', bucket_range:[0,9999],
      saved_assignment_gate:'exact config_hash, known variant and integer bucket 0..9999',
      uuid_gate:'RFC-shaped UUID version 1..5 and variant 8/9/a/b',
      release_id_participates:false, random_per_view:false,
    },
    eligibility:{
      departure_count:{min:1,max:20}, all_departures_date_parse_finite:true,
      required_future_departure:'at least one departure > now + boarding reserve',
      default_boarding_reserve_ms:600000,
      forced_qa_override_bypasses_schedule_time_gate:true,
      automation_without_forced_qa:'baseline/no assignment',
      treatment_unavailable:'baseline with local diagnostic',
    },
    analytics:{
      remote_ingest_modes:['focus_group','live'], qa_override_trusted:false, automation_trusted:false,
      consent_gate:'local profile consent_ok plus UUID anon_id/session_id',
      valid_exposure:{minimum_intersection_ratio:0.5,continuous_ms:1000,document_visibility:'visible'},
      qualified_actions:qualifiedActions,
      qualified_action_definition_present:true,
      qualified_action_filter_called_by_click_ingest:false,
      click_ingest_observation:'client forwards the data-transport-action value after an accepted exposure without invoking isQualifiedTransportAction',
      transport:'resilient direct/relay RPC with idempotent outbox; UI never waits for success',
    },
    srm:{
      expected_bucket_counts:buckets.map(({variant,buckets:count})=>({variant,buckets:count})),
      statistic:'three-cell chi-square goodness-of-fit; df=2 survival exp(-chiSquare/2)',
      diagnostic_only_below_total:300,
      blocker:'total >= 300 AND pValue < 0.001 AND max absolute share deviation > 0.015',
      runtime_consumer:'none in pinned site source; evaluator is referenced by test only',
    },
    mode_reachability:{
      off:'production build hard-codes off and renders the departure-board baseline without experiment wrapper',
      qa:'secret candidate allowlist; forced session-scoped treatment; telemetry untrusted',
      focus_group:'secret candidate allowlist; normal assignment and consent-gated ingest; QA override remains untrusted',
      live:'source/client-supported but no approved pinned production or secret-candidate build path',
    },
    decision_receipt:{
      status:'absent', winner:null,
      evidence_scope:'no winner/acceptance receipt in the pinned source files or supplied decoder experiment evidence',
      consequence:'experiment-unresolved; no treatment is accepted or merged',
    },
  };
}

export function buildTransportExperimentRows({ sourceRoot }) {
  const decoded = decodeTransportExperimentSource({sourceRoot});
  const modes = decoded.modes || ['off','qa','focus_group','live'];
  const variants = decoded.variants || ['departure_board_v1','route_strips_v1','next_departure_queue_v1'];
  const shared = {
    experiment_id:'transport_timetable_layout', exact_source_sha:EXACT_SOURCE_SHA,
    source_path:repoPath(PRIMARY_PATH), source_status:decoded.status,
    source_files:decoded.source_files, experiment_version:decoded.experiment_version ?? 1,
    config_hash:decoded.config_hash || null, modes, variants, buckets:decoded.buckets || [],
    assignment:decoded.assignment || null, eligibility:decoded.eligibility || null,
    analytics:decoded.analytics || null, srm:decoded.srm || null,
    mode_reachability:decoded.mode_reachability || null,
    winner_decision_receipt:'absent', decision_receipt:decoded.decision_receipt || {status:'absent',winner:null},
    lifecycle_status:'experiment-unresolved', decision:'NOT_MERGED', accepted_component:false,
    normalization_allowed:false,
  };
  return [
    {id:'experiment.transport-timetable-layout.off',...shared,treatment:'departure_board_v1',mode:'off',classification:'experiment-off'},
    ...variants.map((treatment)=>({
      id:`experiment.transport-timetable-layout.qa.${treatment.replaceAll('_','-')}`,
      ...shared,treatment,mode:'qa',classification:'controlled-specimen-only',
    })),
    {id:'experiment.transport-timetable-layout.focus-group',...shared,treatment:'deterministic-assignment',mode:'focus_group',classification:'controlled-specimen-only'},
    {id:'experiment.transport-timetable-layout.live',...shared,treatment:'deterministic-assignment',mode:'live',classification:'dead-unreachable'},
  ];
}

const CURATED_HISTORY_SCOPE = 'bounded semantic reconciliation from exact commits, merged PR metadata, named Actions runs and reviewed branch evidence; not an exhaustive claim over every tag, release, artifact or mutable remote ref';
const actionRun = (runId) => runId ? `https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/${runId}` : null;
const pullRequest = (pr) => pr ? `https://github.com/onedayonemasterpiece/events-bot-new/pull/${pr}` : null;

export function buildCuratedBehavioralHistoryRows() {
  const rows = [
    {key:'transport-framework',contract_id:'behavior.transport-experiment',variant_id:'transport_timetable_layout',classification:'historical-unresolved',semantic_status:'experiment-framework-merged-no-winner',ancestry_ref:'1b4f2ccc903f2e068ac5212e68ce9d0a86dde7f2',commit:'1b4f2ccc903f2e068ac5212e68ce9d0a86dde7f2',merge_commit:'ab6fb7d22517c6fc3932ca934fda323d70613c1d',pr:69,run_id:29616990545,replaced_by:null,evidence_scope:CURATED_HISTORY_SCOPE,acceptance_claimed:false},
    {key:'transport-resilient-baseline',contract_id:'behavior.transport-experiment',variant_id:'departure_board_v1',classification:'implemented-current',semantic_status:'accepted-resilient-visual-baseline-assignment-off',ancestry_ref:'d2fa6f2753d417f9c2d91d6833fb764375526f67',commit:'d2fa6f2753d417f9c2d91d6833fb764375526f67',merge_commit:'b9787574870b4524ef66130349fdcb95d39a0ea8',pr:74,run_id:29637010450,replaced_by:null,evidence_scope:CURATED_HISTORY_SCOPE,acceptance_claimed:true},
    {key:'transport-abc-renderability',contract_id:'behavior.transport-experiment',variant_id:'departure_board_v1|route_strips_v1|next_departure_queue_v1',classification:'controlled-specimen-only',semantic_status:'renderability-only-no-product-winner',ancestry_ref:'5fcec2640b3c3a78c7696a51506aa85b7aec6cdc',commit:'5fcec2640b3c3a78c7696a51506aa85b7aec6cdc',merge_commit:'e22557ebbbf08ca386f9865ff30114999cd500a4',pr:79,run_id:29645645509,replaced_by:null,evidence_scope:CURATED_HISTORY_SCOPE,acceptance_claimed:false},
    {key:'transport-advisory-ranking',contract_id:'behavior.transport-experiment',variant_id:'departure_board_v1>next_departure_queue_v1>route_strips_v1',classification:'historical-unresolved',semantic_status:'advisory-ranking-not-winner-receipt',ancestry_ref:'integration/static-event-v11-transport-phone-carousel@3b17e536e4dffa9c9fcebab6e641a7cd4ba99b6a',commit:'51f9c039ca6f9f3d7a940fc82044fce13414a09c',supporting_commit:'3b17e536e4dffa9c9fcebab6e641a7cd4ba99b6a',merge_commit:null,pr:null,run_id:null,replaced_by:null,evidence_scope:'bounded non-ancestor branch/advisory evidence; ranking A > C > B is not an owner winner decision',acceptance_claimed:false},

    {key:'cta-production-root',contract_id:'behavior.desktop-event-action-panel',variant_id:'desktop-event-cta-chain',classification:'historical-variant-evidence',semantic_status:'production-desktop-integration-chain-root',ancestry_ref:'6a26121461321b09953765aed7f49aa83231eddf',commit:'3daba3e234f33cd76b71d74192f25a45bacfb764',merge_commit:'6a26121461321b09953765aed7f49aa83231eddf',pr:40,run_id:29384989743,replaced_by:'later CTA parity and anatomy records',evidence_scope:CURATED_HISTORY_SCOPE,acceptance_claimed:false},
    {key:'cta-template-parity',contract_id:'behavior.desktop-event-action-panel',variant_id:'desktop-event-cta-chain',classification:'historical-variant-evidence',semantic_status:'autogeneration-template-parity',ancestry_ref:'946398589d190de46eb8ede0e550408e43638fcd',commit:'42c64403de3d5ab8baa52d869502f8a8bfeb1f01',merge_commit:'946398589d190de46eb8ede0e550408e43638fcd',pr:70,run_id:29621103101,replaced_by:'later CTA acceptance records',evidence_scope:CURATED_HISTORY_SCOPE,acceptance_claimed:false},
    {key:'cta-preproduction-continuation',contract_id:'behavior.desktop-event-action-panel',variant_id:'desktop-event-cta-chain',classification:'historical-variant-evidence',semantic_status:'preproduction-continuation',ancestry_ref:'f6d51b0c50092e4c0bedbb2462c4b16b550efbdb',commit:'ce27d5936147cc8dc17cf36c00c6ed8ca08f2782',merge_commit:'f6d51b0c50092e4c0bedbb2462c4b16b550efbdb',pr:81,run_id:29652368448,replaced_by:'later CTA acceptance records',evidence_scope:CURATED_HISTORY_SCOPE,acceptance_claimed:false},
    {key:'cta-expiry-proof',contract_id:'behavior.desktop-event-action-panel',variant_id:'desktop-event-cta-chain',classification:'historical-variant-evidence',semantic_status:'live-acceptance-expiry-proof',ancestry_ref:'b82a52bf27a452c50c6bd48237c6ba5d956a5734',commit:'7a63c4348b738f7cda320c68e88a49c1eaf402eb',merge_commit:'b82a52bf27a452c50c6bd48237c6ba5d956a5734',pr:85,run_id:29657326899,replaced_by:'PR88 exact anatomy',evidence_scope:CURATED_HISTORY_SCOPE,acceptance_claimed:false},
    {key:'cta-split-inline-editorial-stacked',contract_id:'behavior.desktop-event-action-panel',variant_id:'split-inline|editorial-stacked',classification:'implemented-current',semantic_status:'exact-current-anatomy',ancestry_ref:'5805a5a851c0c292848846365362540bfe906e4d',commit:'5805a5a851c0c292848846365362540bfe906e4d',merge_commit:'d27e032a1e9c5db02f05a98edee45292defbbfd2',pr:88,run_id:29664131223,replaced_by:null,evidence_scope:CURATED_HISTORY_SCOPE,acceptance_claimed:true},
    {key:'cta-continuation-motion',contract_id:'behavior.desktop-event-action-panel',variant_id:'desktop-event-cta-motion',classification:'implemented-current',semantic_status:'motion-regression-chain',ancestry_ref:'c587a0cf86e144a88c0457035866c8325ea59dc5',commit:'56bb18c15ca31bdf2424390a138847336f2feb7c',merge_commit:'c587a0cf86e144a88c0457035866c8325ea59dc5',pr:97,run_id:29680418058,replaced_by:null,evidence_scope:CURATED_HISTORY_SCOPE,acceptance_claimed:true},
    {key:'cta-tactile-reverted',contract_id:'behavior.desktop-event-action-panel',variant_id:'tactile-split-desktop',classification:'historical-replaced',semantic_status:'reverted-unaccepted-branch-variant',ancestry_ref:'e21a36547a69776dd35bfd47837d5be4a10d18ef',commit:'e21a36547a69776dd35bfd47837d5be4a10d18ef',merge_commit:null,pr:null,run_id:null,replaced_by:'e1800d6ce182ef86f0660d4eae006dba4de37178',evidence_scope:'bounded branch history; exact revert proves this tactile variant is not current',acceptance_claimed:false},

    {key:'listing-personal-filter-v1-v2',contract_id:'behavior.listing-personal-filter',variant_id:'ListingPersonalFilter@1|ListingPersonalFilter@2',classification:'historical-replaced',semantic_status:'historical-prop-behaviors-retained-inside-later-shell',ancestry_ref:EXACT_SOURCE_SHA,commit:null,merge_commit:null,pr:null,run_id:null,replaced_by:'ListingPersonalFilter data-ds-version=3 shell',evidence_scope:'bounded exact-source and design-system-document reconciliation; no exhaustive PR/run mapping claimed',acceptance_claimed:false},
    {key:'listing-personal-filter-v3',contract_id:'behavior.listing-personal-filter',variant_id:'ListingPersonalFilter@3',classification:'historical-unresolved',semantic_status:'current-candidate-shell-with-v1-v2-behavior-props-and-document-conflict',ancestry_ref:EXACT_SOURCE_SHA,commit:null,merge_commit:null,pr:null,run_id:null,replaced_by:null,evidence_scope:'bounded exact-source and design-system-document reconciliation; candidate is not an accepted normalized component',acceptance_claimed:false},
    {key:'listing-discovery-rail-v1-v4',contract_id:'behavior.listing-discovery-rail',variant_id:'ListingDiscoveryRail@1..4',classification:'historical-replaced',semantic_status:'replaced-rail-iterations',ancestry_ref:EXACT_SOURCE_SHA,commit:null,merge_commit:null,pr:null,run_id:null,replaced_by:'ListingDiscoveryRail@5 candidate',evidence_scope:'bounded pinned ancestry/source reconciliation; individual PR/run mapping not claimed',acceptance_claimed:false},
    {key:'listing-discovery-rail-v5',contract_id:'behavior.listing-discovery-rail',variant_id:'ListingDiscoveryRail@5',classification:'historical-unresolved',semantic_status:'current-source-candidate-not-normalized',ancestry_ref:EXACT_SOURCE_SHA,commit:null,merge_commit:null,pr:null,run_id:null,replaced_by:null,evidence_scope:'bounded exact-source record; current candidate does not imply design-system acceptance',acceptance_claimed:false},
    {key:'physical-rails-menu-secret-candidate',contract_id:'behavior.reference4-mobile-menu',variant_id:'accepted-mobile-physical-rails',classification:'controlled-specimen-only',semantic_status:'secret-candidate-scope-not-production-root-proof',ancestry_ref:'61870ba1122e2d4ad19ea59795f0b6c242d38268',commit:'20307320d8a7b591f2d7a403aeb5eeab65204455',merge_commit:'61870ba1122e2d4ad19ea59795f0b6c242d38268',pr:125,run_id:30254204820,replaced_by:null,evidence_scope:CURATED_HISTORY_SCOPE,acceptance_claimed:false},
    {key:'mobile-media-geometry',contract_id:'behavior.event-hero',variant_id:'mobile-media-regression-chain',classification:'implemented-current',semantic_status:'accepted-current-regression-chain',ancestry_ref:'66b4f129719c02c90420e6c56801f7fa65509bf5',commit:'1d7853f28ffc67189d76c9ffb27e2ec75d3c53bf',merge_commit:'66b4f129719c02c90420e6c56801f7fa65509bf5',pr:100,run_id:29735139804,replaced_by:null,evidence_scope:CURATED_HISTORY_SCOPE,acceptance_claimed:true},
    {key:'mobile-media-layout',contract_id:'behavior.event-hero',variant_id:'mobile-media-regression-chain',classification:'implemented-current',semantic_status:'accepted-current-regression-chain',ancestry_ref:'58440062e7bab708676c378de345c65f19ce91b1',commit:'bf01f849bc5f38294831cce0c25f3e60ecdf6437',merge_commit:'58440062e7bab708676c378de345c65f19ce91b1',pr:117,run_id:29820482885,replaced_by:null,evidence_scope:CURATED_HISTORY_SCOPE,acceptance_claimed:true},
    {key:'mobile-media-crop-gate',contract_id:'behavior.event-hero',variant_id:'mobile-media-regression-chain',classification:'implemented-current',semantic_status:'accepted-current-regression-chain',ancestry_ref:'f368eeaebc9ab5c818346abd6a44152c789341f4',commit:'5b987a5a1054a1e1a71017c4fedebab78ec32997',merge_commit:'f368eeaebc9ab5c818346abd6a44152c789341f4',pr:169,run_id:30675302697,replaced_by:null,evidence_scope:CURATED_HISTORY_SCOPE,acceptance_claimed:true},
  ];
  return rows.map((row)=>({
    id:`history.curated.${row.key}`, ...row,
    pr_url:pullRequest(row.pr), actions_run_url:actionRun(row.run_id),
    evidence_kind:'curated-semantic-history', canonical_history:false,
    decision:'NOT_MERGED', normalization_allowed:false,
  }));
}
