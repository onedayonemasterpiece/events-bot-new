import { readFileSync } from 'node:fs';

const inputs = Object.fromEntries(process.argv.slice(2).map((argument) => {
  const separator = argument.indexOf('=');
  if (separator <= 0) throw new Error(`expected label=path, received ${argument}`);
  return [argument.slice(0, separator), argument.slice(separator + 1)];
}));

const compact = (path) => {
  const report = JSON.parse(readFileSync(path, 'utf8'));
  const evidence = report.checklist_evidence;
  const meaningfulFontFamilies = evidence['12'].raw_font_family_consumers
    .map((item) => ({
      path: item.path,
      values: item.values.filter((value) => value !== 'inherit'),
    }))
    .filter((item) => item.values.length > 0);
  const siteFontAssets = evidence['11'].font_assets
    .filter((asset) => asset.startsWith('site/'));
  const topFiles = (items, limit = 24) => items
    .slice(0, limit)
    .map(({ path: itemPath, count, values }) => ({
      path: itemPath,
      count,
      values: values?.slice(0, 24),
    }));

  return {
    resolved_ref: report.resolved_ref,
    source_file_count: report.source_file_count,
    consumer_file_count: report.consumer_file_count,
    central_file_count: report.central_file_count,
    checklist: {
      '11': {
        font_declaration: evidence['11'].font_declaration,
        site_font_assets: siteFontAssets,
        weight_role_count: evidence['11'].weight_role_count,
        classification: siteFontAssets.length === 0
          ? 'PRODUCT_DECISION_REQUIRED_NO_APPROVED_SITE_FONT_ASSET'
          : 'REVIEW_ASSET_AUTHORITY',
      },
      '12': {
        central_role_count: evidence['12'].type_roles.length,
        central_roles: evidence['12'].type_roles,
        direct_font_family_consumers: meaningfulFontFamilies,
        classification: meaningfulFontFamilies.length
          ? 'A0_BINDINGS_READY'
          : 'CENTRAL_ROLES_PRESENT',
      },
      '13': {
        raw_length_file_count: evidence['13'].consumer_files_with_raw_length_values,
        highest_density_consumers: topFiles(evidence['13'].top_files),
        classification: 'REQUIRES_SEMANTIC_CLUSTER_REVIEW_NOT_BLIND_LITERAL_REMOVAL',
      },
      '14': {
        central_containers: evidence['14'].central_containers,
        central_breakpoints: evidence['14'].central_breakpoints,
        classification: 'CENTRAL_REGISTRY_PRESENT_ROUTE_USAGE_REVIEW_REQUIRED',
      },
      '15': {
        canonical_component_owner_present: evidence['15'].canonical_component_owner_present,
        obsolete_legacy_owner_matches: evidence['15'].obsolete_legacy_owner_matches,
        classification: evidence['15'].obsolete_legacy_owner_matches.length
          ? 'F0_FIX_READY'
          : 'PHYSICAL_OWNER_CLOSED',
      },
      '16': {
        raw_color_file_count: evidence['16'].consumer_files_with_raw_colors,
        raw_color_occurrence_count: evidence['16'].occurrence_count,
        top_values: evidence['16'].top_values.slice(0, 40),
        highest_density_consumers: topFiles(evidence['16'].top_files),
        classification: 'REQUIRES_ROLE_CLUSTER_CLASSIFICATION_IDENTITY_AND_MEDIA_EXCEPTIONS_ALLOWED',
      },
      '17': {
        canonical_action_count: evidence['17'].canonical_action_count,
        canonical_actions: evidence['17'].canonical_actions,
        inline_svg_consumers: evidence['17'].consumer_inline_svg_files,
        classification: evidence['17'].consumer_inline_svg_files.length
          ? 'EXACT_GLYPH_IDENTITY_REVIEW_REQUIRED'
          : 'CANONICAL_REGISTRY_ONLY',
      },
      '18': {
        icon_roles: evidence['18'].icon_roles,
        raw_icon_size_consumers: evidence['18'].consumer_files_with_raw_icon_sizes,
        classification: Object.keys(evidence['18'].icon_roles).length === 4
          ? 'FOUR_ROLES_PRESENT_CONSUMER_BINDING_REVIEW_REQUIRED'
          : 'F0_ROLE_REGISTRY_DEFECT',
      },
      '19': {
        brand_root_files: evidence['19'].brand_root_files,
        social_root_files: evidence['19'].social_root_files,
        medallion_root_files: evidence['19'].medallion_root_files,
        classification: 'ROOT_CENSUS_REVIEW_REQUIRED',
      },
    },
  };
};

const refs = Object.fromEntries(Object.entries(inputs).map(([label, path]) => [label, compact(path)]));
const branch = refs.branch;
const candidate = refs.candidate;
const delta = branch && candidate ? {
  candidate_ref: candidate.resolved_ref,
  branch_ref: branch.resolved_ref,
  candidate_obsolete_legacy_owner_count: candidate.checklist['15'].obsolete_legacy_owner_matches.length,
  branch_obsolete_legacy_owner_count: branch.checklist['15'].obsolete_legacy_owner_matches.length,
  candidate_raw_icon_consumer_count: candidate.checklist['18'].raw_icon_size_consumers.length,
  branch_raw_icon_consumer_count: branch.checklist['18'].raw_icon_size_consumers.length,
  candidate_raw_color_file_count: candidate.checklist['16'].raw_color_file_count,
  branch_raw_color_file_count: branch.checklist['16'].raw_color_file_count,
  candidate_direct_font_consumer_count: candidate.checklist['12'].direct_font_family_consumers.length,
  branch_direct_font_consumer_count: branch.checklist['12'].direct_font_family_consumers.length,
} : undefined;

process.stdout.write(`${JSON.stringify({
  schema: 'kenigevents.f0-checklist-summary.v1',
  refs,
  candidate_vs_branch: delta,
}, null, 2)}\n`);
