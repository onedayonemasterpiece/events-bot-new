import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

REQUIRED_ARCHAEOLOGY_PATHS = {
    'docs/features/static-site-pages/page-archetype-source-map.md',
    'docs/features/static-site-pages/schedule-user-requirements.md',
    'docs/features/static-site-pages/rail-multimodal-directory.md',
    'docs/features/static-site-pages/listing-surfaces-v14-product.md',
    'docs/features/static-site-pages/listing-surfaces-v16-product.md',
    'docs/features/static-site-pages/listing-surfaces-v17-product.md',
    'docs/features/static-site-pages/listing-surfaces-v18-product.md',
    'docs/features/static-site-pages/listing-surfaces-v19-product.md',
    'docs/features/static-site-pages/listing-surfaces-v20-mobile-popular.md',
    'docs/features/static-site-pages/listing-surfaces-v21-mobile-popular.md',
    'docs/features/static-site-pages/listing-surfaces-v22-popular-breakpoint-restore.md',
    'docs/features/static-site-pages/personalizaion/requirements.md',
    'docs/features/static-site-pages/personalizaion/personalization-to-be.md',
    'docs/features/static-site-pages/personalizaion/personalization-implementation-contract.md',
    'docs/features/unsigned-personalization/requirements.md',
    'docs/features/unsigned-personalization/personal-feed-architecture.md',
    'docs/features/static-site-pages/mobile-shell.md',
    'docs/features/static-site-pages/smart-vector-search/README.md',
    'docs/features/static-site-pages/smart-vector-search/smart-vector-search-requirements.md',
    'docs/features/unsigned-personalization/authorized-event-search.md',
    'docs/features/static-site-pages/event-page-merged-skeleton.md',
    'docs/features/static-site-pages/event-mobile-ui-lab-2026-07-15.md',
}


def node_json(script):
    result = subprocess.run(
        ['node', '--input-type=module', '-e', script],
        cwd=ROOT, check=True, text=True, capture_output=True,
    )
    return json.loads(result.stdout)


def test_requirements_archaeology_corpus_is_explicit_bounded_and_status_aware():
    payload = node_json(r"""
      import {REQUIREMENT_SOURCES,buildRequirementsProvenance} from './scripts/current_ui_resource_graph/v1/behavioral-requirements.mjs';
      const rows=buildRequirementsProvenance({sourceRoot:'site'});
      const curated=Object.fromEntries(rows.filter(row=>row.id.startsWith('requirement.curated.listing.')).map(row=>[row.id,row.status]));
      console.log(JSON.stringify({paths:REQUIREMENT_SOURCES.map(row=>row[0]),missing:rows.filter(row=>row.evidence_kind==='missing-requirement-document').map(row=>row.source_path),counts:Object.fromEntries(REQUIREMENT_SOURCES.map(([path])=>[path,rows.filter(row=>row.source_path===path&&row.evidence_kind==='pinned-requirement-document').length])),unsafeRules:rows.filter(row=>/(?:GOOGLE_API_KEY2|PERSONALIZATION_SUPABASE_ACCESS_TOKEN|sb_(?:secret|publishable)|https?:\/\/)/iu.test(row.rule)).map(row=>row.id),curated}));
    """)
    assert REQUIRED_ARCHAEOLOGY_PATHS <= set(payload['paths'])
    assert payload['missing'] == []
    assert payload['unsafeRules'] == []
    assert all(payload['counts'][path] <= 256 for path in REQUIRED_ARCHAEOLOGY_PATHS)
    assert payload['curated'] == {
        'requirement.curated.listing.current-chain': 'accepted-current',
        'requirement.curated.listing.v14-v15-replaced': 'historical-replaced',
        'requirement.curated.listing.v16-replaced': 'historical-replaced',
        'requirement.curated.listing.v17-replaced': 'historical-replaced',
        'requirement.curated.listing.v18-mixed': 'conflict',
        'requirement.curated.listing.v19-mixed': 'conflict',
        'requirement.curated.listing.v20-replaced': 'historical-replaced',
        'requirement.curated.listing.v21-rejected': 'historical-replaced',
        'requirement.curated.listing.v22-mixed': 'conflict',
    }


def test_canonical_history_is_pinned_to_ef7_ancestry_not_mutable_all_refs():
    source = (ROOT / 'scripts/current_ui_resource_graph/v1/behavioral.mjs').read_text()
    assert "'log',PINNED_BEHAVIOR_SOURCE_SHA,'--follow'" in source
    assert "'log','--all','--follow'" not in source
    assert "classification:'pinned-ancestry-commit-unclassified'" in source
    assert 'Source/history extraction is complete' not in source
    assert 'do not claim exhaustive coverage' in source


def test_dynamic_region_paths_exist_and_popular_row_uses_pinned_listing_path():
    payload = node_json(r"""
      import {DYNAMIC_REGIONS,buildDynamicRegionMatrix,buildRequirementsProvenance} from './scripts/current_ui_resource_graph/v1/behavioral-requirements.mjs';
      const provenance=buildRequirementsProvenance({sourceRoot:'site'});
      const rows=buildDynamicRegionMatrix({sourceRoot:'site',provenance});
      console.log(JSON.stringify({popular:DYNAMIC_REGIONS.find(row=>row.id==='popular-personalized-row'),missing:rows.filter(row=>row.reachability==='missing-pinned-source').map(row=>row.id)}));
    """)
    assert payload['popular']['path'] == 'site/src/components/listings/PopularPersonalizedRow.astro'
    assert payload['missing'] == []


def test_requirement_evidence_redacts_non_ui_credentials_names_values_and_urls():
    payload = node_json(r"""
      import {sanitizeRequirementEvidence} from './scripts/current_ui_resource_graph/v1/behavioral-requirements.mjs';
      const sensitive='GOOGLE_API_KEY2=decoder-fixture-public-value PERSONALIZATION_SUPABASE_ACCESS_TOKEN=sb_publishable_fake https://example.supabase.co/path';
      console.log(JSON.stringify({sensitive:sanitizeRequirementEvidence(sensitive),ordinary:sanitizeRequirementEvidence('Search skeleton resolves into canonical EventCard results.')}));
    """)
    assert payload['sensitive'].startswith('<redacted-non-ui-credential-line:')
    assert 'GOOGLE_API_KEY2' not in payload['sensitive']
    assert 'PERSONALIZATION_SUPABASE_ACCESS_TOKEN' not in payload['sensitive']
    assert 'decoder-fixture-public-value' not in payload['sensitive']
    assert 'sb_publishable' not in payload['sensitive']
    assert 'example.supabase.co' not in payload['sensitive']
    assert payload['ordinary'] == 'Search skeleton resolves into canonical EventCard results.'


def test_transport_experiment_is_exact_source_decoded_and_remains_unresolved():
    payload = node_json(r"""
      import {decodeTransportExperimentSource,buildTransportExperimentRows} from './scripts/current_ui_resource_graph/v1/behavioral-experiments.mjs';
      const decoded=decodeTransportExperimentSource({sourceRoot:'site'});
      const rows=buildTransportExperimentRows({sourceRoot:'site'});
      console.log(JSON.stringify({decoded,rows}));
    """)
    decoded = payload['decoded']
    rows = payload['rows']
    assert decoded['status'] == 'decoded-exact-source'
    assert decoded['exact_source_sha'] == 'ef7aa62e45c60f7a12da6160f490719c0721ec03'
    assert decoded['modes'] == ['off', 'qa', 'focus_group', 'live']
    assert decoded['variants'] == ['departure_board_v1', 'route_strips_v1', 'next_departure_queue_v1']
    assert [(row['from'], row['to'], row['buckets']) for row in decoded['buckets']] == [
        (0, 3332, 3333), (3333, 6665, 3333), (6666, 9999, 3334),
    ]
    assert decoded['assignment']['unit'] == 'browser_subject'
    assert decoded['assignment']['digest'] == 'SHA-256'
    assert decoded['assignment']['word'] == 'first unsigned 32-bit big-endian word'
    assert decoded['assignment']['release_id_participates'] is False
    assert decoded['eligibility']['departure_count'] == {'min': 1, 'max': 20}
    assert decoded['eligibility']['default_boarding_reserve_ms'] == 600000
    assert decoded['analytics']['qualified_actions'] == [
        'official_transfer_booking_click', 'bus_origin_map_click', 'walk_route_click',
        'car_route_click', 'transport_calendar_add',
    ]
    assert decoded['analytics']['qualified_action_filter_called_by_click_ingest'] is False
    assert decoded['srm']['diagnostic_only_below_total'] == 300
    assert decoded['srm']['runtime_consumer'].startswith('none in pinned site source')
    assert decoded['decision_receipt'] == {
        'status': 'absent', 'winner': None,
        'evidence_scope': 'no winner/acceptance receipt in the pinned source files or supplied decoder experiment evidence',
        'consequence': 'experiment-unresolved; no treatment is accepted or merged',
    }
    assert {row['mode'] for row in rows} == {'off', 'qa', 'focus_group', 'live'}
    assert {row['treatment'] for row in rows if row['mode'] == 'qa'} == set(decoded['variants'])
    assert next(row for row in rows if row['mode'] == 'off')['classification'] == 'experiment-off'
    assert next(row for row in rows if row['mode'] == 'live')['classification'] == 'dead-unreachable'
    assert all(row['winner_decision_receipt'] == 'absent' for row in rows)
    assert all(row['lifecycle_status'] == 'experiment-unresolved' for row in rows)
    assert all(row['decision'] == 'NOT_MERGED' and row['accepted_component'] is False for row in rows)


def test_curated_history_preserves_semantics_refs_runs_and_replacements():
    rows = node_json(r"""
      import {buildCuratedBehavioralHistoryRows} from './scripts/current_ui_resource_graph/v1/behavioral-experiments.mjs';
      console.log(JSON.stringify(buildCuratedBehavioralHistoryRows()));
    """)
    by_id = {row['id']: row for row in rows}
    baseline = by_id['history.curated.transport-resilient-baseline']
    assert baseline['variant_id'] == 'departure_board_v1'
    assert baseline['semantic_status'] == 'accepted-resilient-visual-baseline-assignment-off'
    assert baseline['pr'] == 74 and baseline['run_id'] == 29637010450
    assert baseline['commit'] == 'd2fa6f2753d417f9c2d91d6833fb764375526f67'
    assert by_id['history.curated.transport-abc-renderability']['acceptance_claimed'] is False
    assert by_id['history.curated.transport-advisory-ranking']['semantic_status'] == 'advisory-ranking-not-winner-receipt'

    current_cta = by_id['history.curated.cta-split-inline-editorial-stacked']
    assert current_cta['variant_id'] == 'split-inline|editorial-stacked'
    assert current_cta['pr'] == 88 and current_cta['run_id'] == 29664131223
    assert current_cta['commit'] == '5805a5a851c0c292848846365362540bfe906e4d'
    tactile = by_id['history.curated.cta-tactile-reverted']
    assert tactile['classification'] == 'historical-replaced'
    assert tactile['replaced_by'] == 'e1800d6ce182ef86f0660d4eae006dba4de37178'

    assert by_id['history.curated.listing-personal-filter-v1-v2']['classification'] == 'historical-replaced'
    assert by_id['history.curated.listing-personal-filter-v3']['classification'] == 'historical-unresolved'
    assert by_id['history.curated.listing-discovery-rail-v1-v4']['classification'] == 'historical-replaced'
    assert by_id['history.curated.listing-discovery-rail-v5']['classification'] == 'historical-unresolved'
    physical = by_id['history.curated.physical-rails-menu-secret-candidate']
    assert physical['classification'] == 'controlled-specimen-only'
    assert physical['pr'] == 125 and physical['run_id'] == 30254204820
    assert all('not an exhaustive' in row['evidence_scope'] or 'bounded' in row['evidence_scope'] for row in rows)
    assert all(row['decision'] == 'NOT_MERGED' and row['normalization_allowed'] is False for row in rows)
